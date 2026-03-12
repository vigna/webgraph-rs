/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use std::path::PathBuf;
use tempfile::Builder;
use webgraph::graphs::union_graph::UnionGraph;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "simplify", about = "Makes a BvGraph simple (undirected and loopless) by adding missing arcs and removing loops, optionally applying a permutation.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
    /// The basename of the simplified graph.
    pub dst: PathBuf,

    #[arg(long)]
    /// The basename of a pre-computed transposed version of the source graph,
    /// which will be used to speed up the simplification.
    pub transposed: Option<PathBuf>,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[arg(long)]
    /// The path to an optional permutation to apply to the graph.
    pub permutation: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.
    pub fmt: IntSliceFormat,

    #[arg(long)]
    /// Use the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.
    pub dcf: bool,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => simplify::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => simplify::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn no_ef_warn(basepath: impl AsRef<std::path::Path>) {
    log::warn!(SEQ_PROC_WARN![], basepath.as_ref().display());
}

pub fn simplify<E: Endianness>(_global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    // TODO!: speed it up by using random access graph if possible
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let use_dcf = args.dcf;
    let src = args.src.clone();

    let target_endianness = args.ca.endianness.clone().unwrap_or_else(|| E::NAME.into());

    let dir = Builder::new().prefix("transform_simplify_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

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
                        webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                            .endianness::<E>()
                            .load()?;
                    let num_nodes = graph.num_nodes();
                    let graph_t =
                        webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&t_path)
                            .endianness::<E>()
                            .load()?;

                    if graph_t.num_nodes() != num_nodes {
                        anyhow::bail!(
                            "The number of nodes in the graph and its transpose do not match! {} != {}",
                            num_nodes,
                            graph_t.num_nodes()
                        );
                    }

                    let sorted = NoSelfLoopsGraph(UnionGraph(graph, graph_t));

                    thread_pool.install(|| {
                        let cp = crate::cutpoints(
                            &src,
                            sorted.num_nodes(),
                            sorted.num_arcs_hint(),
                            use_dcf,
                        )?;
                        builder.par_comp_lenders_endianness_at(&sorted, &target_endianness, cp)
                    })?;

                    return Ok(());
                }
                (true, false) => {
                    no_ef_warn(&t_path);
                }
                (false, true) => {
                    no_ef_warn(&args.src);
                }
                (false, false) => {
                    no_ef_warn(&args.src);
                    no_ef_warn(&t_path);
                }
            }

            let seq_graph =
                webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?;
            let num_nodes = seq_graph.num_nodes();
            let seq_graph_t =
                webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&t_path)
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

            thread_pool.install(|| {
                let cp =
                    crate::cutpoints(&src, sorted.num_nodes(), sorted.num_arcs_hint(), use_dcf)?;
                builder.par_comp_lenders_endianness_at(&sorted, &target_endianness, cp)
            })?;
        }
        // apply the permutation, don't care if the transposed graph is already computed
        // as we cannot really exploit it
        (Some(perm_path), None | Some(_)) => {
            log::info!("Permutation provided, applying it to the graph");

            let loaded = args.fmt.load(perm_path)?;
            let memory_usage = args.memory_usage.memory_usage;
            let src_basename = args.src;

            dispatch_int_slice!(loaded, |perm| {
                if std::fs::metadata(src_basename.with_extension("ef")).is_ok_and(|x| x.is_file()) {
                    log::info!(".ef file found, using parallel simplify + permute");

                    // We split the BvGraph directly and apply the permutation
                    // inline rather than wrapping it in a PermutedGraph and
                    // calling simplify_split. PermutedGraph's SplitLabeling
                    // uses split::seq::Iter, which advances sequentially to
                    // each cutpoint; it cannot use split::ra::Iter because
                    // PermutedGraph does not implement RandomAccessLabeling
                    // (that would require the inverse permutation).
                    let graph = webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(
                        &src_basename,
                    )
                    .endianness::<E>()
                    .load()?;
                    let num_nodes = graph.num_nodes();

                    thread_pool.install(|| {
                        let par_sort_iters = webgraph::utils::ParSortIters::new_dedup(num_nodes)?
                            .memory_usage(memory_usage);
                        let parts = rayon::current_num_threads();

                        let pairs: Vec<_> = graph
                            .split_iter(parts)
                            .map(|iter| {
                                iter.into_pairs().flat_map(|(src, dst)| {
                                    // The two-element iterator is fully inlined by LLVM,
                                    // generating the same code as a hand-written loop.
                                    let ps = perm.index_value(src);
                                    let pd = perm.index_value(dst);
                                    if ps != pd {
                                        Some((ps, pd)).into_iter().chain(Some((pd, ps)))
                                    } else {
                                        None.into_iter().chain(None)
                                    }
                                })
                            })
                            .collect();

                        let sorted = par_sort_iters.sort(pairs)?;
                        let pairs: Vec<_> = sorted.into();
                        match target_endianness.as_str() {
                            #[cfg(any(
                                feature = "be_bins",
                                not(any(feature = "be_bins", feature = "le_bins"))
                            ))]
                            BE::NAME => {
                                builder.par_comp_lenders::<BE, _>(pairs.into_iter(), num_nodes)
                            }
                            #[cfg(any(
                                feature = "le_bins",
                                not(any(feature = "be_bins", feature = "le_bins"))
                            ))]
                            LE::NAME => {
                                builder.par_comp_lenders::<LE, _>(pairs.into_iter(), num_nodes)
                            }
                            e => anyhow::bail!("Unknown endianness: {}", e),
                        }
                    })
                } else {
                    no_ef_warn(&src_basename);
                    let seq_graph =
                        webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(
                            &src_basename,
                        )
                        .endianness::<E>()
                        .load()?;
                    let perm_graph = PermutedGraph {
                        graph: &seq_graph,
                        perm,
                    };
                    let sorted = webgraph::transform::simplify(&perm_graph, memory_usage)?;
                    thread_pool.install(|| {
                        let cp = crate::cutpoints(
                            &src,
                            sorted.num_nodes(),
                            sorted.num_arcs_hint(),
                            use_dcf,
                        )?;
                        builder.par_comp_lenders_endianness_at(&sorted, &target_endianness, cp)
                    })
                }
            })?;
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
                    webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                        .endianness::<E>()
                        .load()?;
                let num_nodes = graph.num_nodes();

                thread_pool.install(|| {
                    let sorted = webgraph::transform::simplify_split(
                        &graph,
                        args.memory_usage.memory_usage,
                    )?;

                    let pairs: Vec<_> = sorted.into();
                    match target_endianness.as_str() {
                        #[cfg(any(
                            feature = "be_bins",
                            not(any(feature = "be_bins", feature = "le_bins"))
                        ))]
                        BE::NAME => builder.par_comp_lenders::<BE, _>(pairs.into_iter(), num_nodes),
                        #[cfg(any(
                            feature = "le_bins",
                            not(any(feature = "be_bins", feature = "le_bins"))
                        ))]
                        LE::NAME => builder.par_comp_lenders::<LE, _>(pairs.into_iter(), num_nodes),
                        e => anyhow::bail!("Unknown endianness: {}", e),
                    }
                })?;

                return Ok(());
            }

            no_ef_warn(&args.src);

            let seq_graph =
                webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?;

            // transpose the graph
            let sorted =
                webgraph::transform::simplify_sorted(seq_graph, args.memory_usage.memory_usage)?;

            thread_pool.install(|| {
                let cp =
                    crate::cutpoints(&src, sorted.num_nodes(), sorted.num_arcs_hint(), use_dcf)?;
                builder.par_comp_lenders_endianness_at(&sorted, &target_endianness, cp)
            })?;
        }
    }

    Ok(())
}
