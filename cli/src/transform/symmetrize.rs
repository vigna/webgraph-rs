/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use std::path::PathBuf;
use tempfile::Builder;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "symmetrize", about = "Symmetrizes a graph in the BV format by adding missing reverse arcs, optionally removing self-loops and applying a permutation.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub src: PathBuf,
    /// The basename of the symmetrized graph.​
    pub dst: PathBuf,

    #[arg(long)]
    /// Remove self-loops from the result.​
    pub no_loops: bool,

    #[arg(long)]
    /// The basename of a pre-computed transposed version of the source graph,
    /// which will be used to speed up the symmetrization.​
    pub transposed: Option<PathBuf>,

    #[arg(short, long)]
    /// Use the sequential algorithm (does not need offsets).​
    pub sequential: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[arg(long)]
    /// The path to an optional permutation to apply to the graph.​
    pub permutation: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.​
    pub fmt: IntSliceFormat,

    #[arg(long, conflicts_with = "sequential")]
    /// Use the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => {
            if args.sequential {
                seq_symmetrize::<BE>(args)
            } else {
                par_symmetrize::<BE>(args)
            }
        }
        #[cfg(feature = "le_bins")]
        LE::NAME => {
            if args.sequential {
                seq_symmetrize::<LE>(args)
            } else {
                par_symmetrize::<LE>(args)
            }
        }
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn par_symmetrize<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let use_dcf = args.dcf;
    let no_loops = args.no_loops;
    let src = args.src.clone();

    let target_endianness = args.ca.endianness.clone().unwrap_or_else(|| E::NAME.into());

    let dir = Builder::new().prefix("transform_symmetrize_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    match (args.permutation, args.transposed) {
        // Load the transposed graph and use it to directly compress the graph
        // without doing any sorting
        (None, Some(t_path)) => {
            log::info!("Transposed graph provided, using it to symmetrize the graph");

            let graph = webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                .endianness::<E>()
                .load()?;
            let num_nodes = graph.num_nodes();
            let graph_t = webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&t_path)
                .endianness::<E>()
                .load()?;

            if graph_t.num_nodes() != num_nodes {
                anyhow::bail!(
                    "The number of nodes in the graph and its transpose do not match! {} != {}",
                    num_nodes,
                    graph_t.num_nodes()
                );
            }

            // Splits both graphs independently at the same cutpoints
            // so that each partition uses split::ra::Iter (direct
            // seeking), then merges successor lists at the lender level.
            let cp = crate::cutpoints(&src, num_nodes, graph.num_arcs_hint(), use_dcf)?;
            if no_loops {
                thread_pool.install(|| {
                    let lenders: Vec<_> = graph
                        .split_iter_at(cp.clone())
                        .zip(graph_t.split_iter_at(cp))
                        .map(|(g, gt)| {
                            webgraph::graphs::no_selfloops_graph::NodeLabels::new(
                                webgraph::graphs::union_graph::NodeLabels::new(g, gt),
                            )
                        })
                        .collect();

                    par_comp_lenders!(builder, lenders.into_iter(), num_nodes, target_endianness)
                })?;
            } else {
                thread_pool.install(|| {
                    let lenders: Vec<_> = graph
                        .split_iter_at(cp.clone())
                        .zip(graph_t.split_iter_at(cp))
                        .map(|(g, gt)| webgraph::graphs::union_graph::NodeLabels::new(g, gt))
                        .collect();

                    par_comp_lenders!(builder, lenders.into_iter(), num_nodes, target_endianness)
                })?;
            }
        }
        // apply the permutation, don't care if the transposed graph is already computed
        // as we cannot really exploit it
        (Some(perm_path), None | Some(_)) => {
            log::info!("Permutation provided, applying it to the graph");

            let loaded = args.fmt.load(perm_path)?;
            let memory_usage = args.memory_usage.memory_usage;

            dispatch_int_slice!(loaded, |perm| {
                // We split the BvGraph directly and apply the permutation
                // inline rather than wrapping it in a PermutedGraph and
                // calling symmetrize_split. PermutedGraph's SplitLabeling
                // uses split::seq::Iter, which advances sequentially to
                // each cutpoint; it cannot use split::ra::Iter because
                // PermutedGraph does not implement RandomAccessLabeling
                // (that would require the inverse permutation).
                let graph =
                    webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                        .endianness::<E>()
                        .load()?;
                let num_nodes = graph.num_nodes();

                let cp = crate::cutpoints(&src, num_nodes, graph.num_arcs_hint(), use_dcf)?;

                thread_pool.install(|| {
                    let par_sort_iters = webgraph::utils::ParSortIters::new_dedup(num_nodes)?
                        .memory_usage(memory_usage)
                        .expected_num_pairs(2 * graph.num_arcs() as usize);

                    let pairs: Vec<_> = graph
                        .split_iter_at(cp)
                        .map(|iter| {
                            iter.into_pairs().flat_map(move |(src, dst)| {
                                let ps = perm.index_value(src);
                                let pd = perm.index_value(dst);
                                if ps != pd {
                                    Some((ps, pd)).into_iter().chain(Some((pd, ps)))
                                } else if !no_loops {
                                    Some((ps, pd)).into_iter().chain(None)
                                } else {
                                    None.into_iter().chain(None)
                                }
                            })
                        })
                        .collect();

                    let sorted = par_sort_iters.sort(pairs)?;
                    let pairs: Vec<_> = sorted.into();
                    par_comp_lenders!(builder, pairs.into_iter(), num_nodes, target_endianness)
                })
            })?;
        }
        // Compute the transpose on the fly
        (None, None) => {
            log::info!(
                "No permutation or transposed graph provided, computing the transpose on the fly"
            );

            let graph = webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                .endianness::<E>()
                .load()?;
            let num_nodes = graph.num_nodes();
            let cp = crate::cutpoints(&src, num_nodes, graph.num_arcs_hint(), use_dcf)?;

            macro_rules! symmetrize_and_compress {
                ($no_loops:expr) => {
                    thread_pool.install(|| {
                        let sorted = webgraph::transform::symmetrize_sorted_split::<$no_loops, _>(
                            &graph,
                            args.memory_usage.memory_usage,
                            Some(cp),
                        )?;
                        let pairs: Vec<_> = sorted.into();
                        par_comp_lenders!(builder, pairs.into_iter(), num_nodes, target_endianness)
                    })?
                };
            }
            if no_loops {
                symmetrize_and_compress!(true);
            } else {
                symmetrize_and_compress!(false);
            }
        }
    }

    Ok(())
}

pub fn seq_symmetrize<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let no_loops = args.no_loops;

    let target_endianness = args.ca.endianness.clone().unwrap_or_else(|| E::NAME.into());

    let dir = Builder::new().prefix("transform_symmetrize_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    match (args.permutation, args.transposed) {
        (None, Some(t_path)) => {
            log::info!("Transposed graph provided, using it to symmetrize the graph");

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

            if no_loops {
                let lenders: Vec<_> = seq_graph
                    .split_iter(rayon::current_num_threads())
                    .zip(seq_graph_t.split_iter(rayon::current_num_threads()))
                    .map(|(g, gt)| {
                        webgraph::graphs::no_selfloops_graph::NodeLabels::new(
                            webgraph::graphs::union_graph::NodeLabels::new(g, gt),
                        )
                    })
                    .collect();

                thread_pool.install(|| {
                    par_comp_lenders!(builder, lenders.into_iter(), num_nodes, target_endianness)
                })?;
            } else {
                let lenders: Vec<_> = seq_graph
                    .split_iter(rayon::current_num_threads())
                    .zip(seq_graph_t.split_iter(rayon::current_num_threads()))
                    .map(|(g, gt)| webgraph::graphs::union_graph::NodeLabels::new(g, gt))
                    .collect();

                thread_pool.install(|| {
                    par_comp_lenders!(builder, lenders.into_iter(), num_nodes, target_endianness)
                })?;
            }
        }
        (Some(perm_path), None | Some(_)) => {
            log::info!("Permutation provided, applying it to the graph");

            let loaded = args.fmt.load(perm_path)?;
            let memory_usage = args.memory_usage.memory_usage;

            dispatch_int_slice!(loaded, |perm| {
                let seq_graph =
                    webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
                        .endianness::<E>()
                        .load()?;
                let perm_graph = PermutedGraph {
                    graph: &seq_graph,
                    perm,
                };
                macro_rules! symmetrize_and_compress {
                    ($no_loops:expr) => {{
                        let sorted = webgraph::transform::symmetrize::<$no_loops>(
                            &perm_graph,
                            memory_usage,
                        )?;
                        let num_nodes = sorted.num_nodes();
                        thread_pool.install(|| {
                            par_comp_lenders!(
                                builder,
                                sorted.split_iter(rayon::current_num_threads()),
                                num_nodes,
                                target_endianness
                            )
                        })
                    }};
                }
                if no_loops {
                    symmetrize_and_compress!(true)
                } else {
                    symmetrize_and_compress!(false)
                }
            })?;
        }
        (None, None) => {
            log::info!(
                "No permutation or transposed graph provided, computing the transpose on the fly"
            );

            let seq_graph =
                webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?;

            macro_rules! symmetrize_and_compress {
                ($no_loops:expr) => {{
                    let symmetrized = webgraph::transform::symmetrize_sorted::<$no_loops, _>(
                        &seq_graph,
                        args.memory_usage.memory_usage,
                    )?;
                    let num_nodes = symmetrized.num_nodes();
                    thread_pool.install(|| {
                        par_comp_lenders!(
                            builder,
                            symmetrized.split_iter(rayon::current_num_threads()),
                            num_nodes,
                            target_endianness
                        )
                    })?
                }};
            }
            if no_loops {
                symmetrize_and_compress!(true);
            } else {
                symmetrize_and_compress!(false);
            }
        }
    }

    Ok(())
}
