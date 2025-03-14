/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::*;
use crate::graphs::union_graph::UnionGraph;
use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::{codes::dispatch_factory::CodesReaderFactoryHelper, prelude::*};
use mmap_rs::MmapFlags;
use std::path::PathBuf;
use tempfile::Builder;

pub const COMMAND_NAME: &str = "simplify";

#[derive(Args, Debug)]
#[command(about = "Makes a BvGraph simple (undirected and loopless) by adding missing arcs and removing loops, optionally applying a permutation.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
    /// The basename of the simplified graph.
    pub dst: PathBuf,

    #[arg(long)]
    /// The basename of a pre-computed transposed version of the source graph, which
    /// will be use to speed up the simplification.
    pub transposed: Option<PathBuf>,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub batch_size: BatchSizeArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[arg(long)]
    /// The path to an optional permutation in binary big-endian format to apply to the graph.
    pub permutation: Option<PathBuf>,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => simplify::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => simplify::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn no_ef_warn(basepath: impl AsRef<std::path::Path>) {
    log::warn!("The .ef file was not found so the simplification will proceed sequentially. This may be slow. To speed it up, you can use `webgraph build ef {}` which would allow us create batches in parallel", basepath.as_ref().display());
}

pub fn simplify<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    // TODO!: speed it up by using random access graph if possible
    let thread_pool = crate::cli::get_thread_pool(args.num_threads.num_threads);

    let target_endianness = args.ca.endianness.clone().unwrap_or_else(|| E::NAME.into());

    let dir = Builder::new().prefix("transform_simplify_").tempdir()?;

    match (args.permutation, args.transposed) {
        // load the transposed graph and use it to directly compress the graph
        // without doing any sorting
        (None, Some(t_path)) => {
            log::info!("Transposed graph provided, using it to simplify the graph");

            let has_ef_graph =
                std::fs::metadata(args.src.with_extension("ef")).is_ok_and(|x| x.is_file());
            let has_ef_t_graph =
                std::fs::metadata(t_path.with_extension("ef")).is_ok_and(|x| x.is_file());

            match (has_ef_graph, has_ef_t_graph) {
                (true, true) => {
                    log::info!("Both .ef files found, using simplify split");

                    let graph =
                        crate::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                            .endianness::<E>()
                            .load()?;
                    let num_nodes = graph.num_nodes();
                    let graph_t =
                        crate::graphs::bvgraph::random_access::BvGraph::with_basename(&t_path)
                            .endianness::<E>()
                            .load()?;

                    if graph_t.num_nodes() != num_nodes {
                        anyhow::bail!("The number of nodes in the graph and its transpose do not match! {} != {}", num_nodes, graph_t.num_nodes());
                    }

                    let sorted = NoSelfLoopsGraph(UnionGraph(graph, graph_t));

                    BvComp::parallel_endianness(
                        &args.dst,
                        &sorted,
                        num_nodes,
                        args.ca.into(),
                        &thread_pool,
                        dir,
                        &target_endianness,
                    )?;

                    return Ok(());
                }
                (true, false) => {
                    no_ef_warn(&args.src);
                }
                (false, true) => {
                    no_ef_warn(&t_path);
                }
                (false, false) => {
                    no_ef_warn(&args.src);
                    no_ef_warn(&t_path);
                }
            }

            let seq_graph =
                crate::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?;
            let num_nodes = seq_graph.num_nodes();
            let seq_graph_t =
                crate::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&t_path)
                    .endianness::<E>()
                    .load()?;

            if seq_graph_t.num_nodes() != num_nodes {
                anyhow::bail!(
                    "The number of nodes in the graph and its transpose do not match! {} != {}",
                    num_nodes,
                    seq_graph_t.num_nodes()
                );
            }

            let sorted = NoSelfLoopsGraph(UnionGraph(seq_graph, seq_graph_t));

            BvComp::parallel_endianness(
                &args.dst,
                &sorted,
                num_nodes,
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness,
            )?;
        }
        // apply the permutation, don't care if the transposed graph is already computed
        // as we cannot really exploit it
        (Some(perm_path), None | Some(_)) => {
            log::info!("Permutation provided, applying it to the graph");

            let perm = JavaPermutation::mmap(perm_path, MmapFlags::RANDOM_ACCESS)?;

            // if the .ef file exists, we can use the simplify split
            if std::fs::metadata(args.src.with_extension("ef")).is_ok_and(|x| x.is_file()) {
                log::info!(".ef file found, using simplify split");
                let graph =
                    crate::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                        .endianness::<E>()
                        .load()?;

                let perm_graph = PermutedGraph {
                    graph: &graph,
                    perm: &perm,
                };

                let sorted = crate::transform::simplify_split(
                    &perm_graph,
                    args.batch_size.batch_size,
                    &thread_pool,
                )?;

                BvComp::parallel_endianness(
                    &args.dst,
                    &sorted,
                    graph.num_nodes(),
                    args.ca.into(),
                    &thread_pool,
                    dir,
                    &target_endianness,
                )?;

                return Ok(());
            }

            no_ef_warn(&args.src);

            let seq_graph =
                crate::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?;

            let perm_graph = PermutedGraph {
                graph: &seq_graph,
                perm: &perm,
            };

            // simplify the graph
            let sorted =
                crate::transform::simplify(&perm_graph, args.batch_size.batch_size).unwrap();

            BvComp::parallel_endianness(
                &args.dst,
                &sorted,
                sorted.num_nodes(),
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness,
            )?;
        }
        // just compute the transpose on the fly
        (None, None) => {
            log::info!(
                "No permutation or transposed graph provided, computing the transpose on the fly"
            );
            // if the .ef file exists, we can use the simplify split
            if std::fs::metadata(args.src.with_extension("ef")).is_ok_and(|x| x.is_file()) {
                log::info!(".ef file found, using simplify split");

                let graph =
                    crate::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                        .endianness::<E>()
                        .load()?;

                let sorted = crate::transform::simplify_split(
                    &graph,
                    args.batch_size.batch_size,
                    &thread_pool,
                )?;

                BvComp::parallel_endianness(
                    &args.dst,
                    &sorted,
                    graph.num_nodes(),
                    args.ca.into(),
                    &thread_pool,
                    dir,
                    &target_endianness,
                )?;

                return Ok(());
            }

            no_ef_warn(&args.src);

            let seq_graph =
                crate::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?;

            let num_nodes = seq_graph.num_nodes();
            // transpose the graph
            let sorted =
                crate::transform::simplify_sorted(seq_graph, args.batch_size.batch_size).unwrap();

            BvComp::parallel_endianness(
                &args.dst,
                &sorted,
                num_nodes,
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness,
            )?;
        }
    }

    Ok(())
}
