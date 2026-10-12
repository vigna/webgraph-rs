/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
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
    pub transpose: Option<PathBuf>,

    #[arg(short, long)]
    /// Uses the sequential algorithm (does not need offsets).​
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

    #[arg(long, conflicts_with = "sequential", requires = "permutation")]
    /// Uses the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

/// Compresses the union of a graph and its transpose, possibly removing
/// self-loops.
macro_rules! compress_union {
    ($graph:expr, $graph_t:expr, $no_loops:expr, $builder:expr, $thread_pool:expr, $target_endianness:expr, $log_interval:expr) => {{
        let (graph, graph_t) = ($graph, $graph_t);
        anyhow::ensure!(
            graph_t.num_nodes() == graph.num_nodes(),
            "The number of nodes in the graph and its transpose do not match! {} != {}",
            graph.num_nodes(),
            graph_t.num_nodes()
        );
        let mut pl = progress_logger![display_memory = true, log_interval = $log_interval];
        let mut builder = $builder.progress_logger(&mut pl);
        if $no_loops {
            let union = NoSelfLoopsGraph(UnionGraph(graph, graph_t));
            $thread_pool.install(|| par_comp!(builder, &union, $target_endianness))?;
        } else {
            let union = UnionGraph(graph, graph_t);
            $thread_pool.install(|| par_comp!(builder, &union, $target_endianness))?;
        }
    }};
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    dispatch_endianness!(&args.src, |E| {
        if args.sequential {
            symmetrize_seq::<E>(args)
        } else {
            symmetrize_par::<E>(args)
        }
    })
}

/// Parallel version of [`symmetrize_seq`].
pub fn symmetrize_par<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let no_loops = args.no_loops;
    let log_interval = args.log_interval.log_interval;
    let target_endianness = args.ca.target_endianness::<E>();
    let builder = args.ca.bv_comp_conf(&args.dst);

    match (args.permutation, args.transpose) {
        // Load the transposed graph and use it to directly compress the graph
        // without doing any sorting
        (None, Some(t_path)) => {
            log::info!("Transposed graph provided, using it to symmetrize the graph");
            compress_union!(
                BvGraph::with_basename(&args.src).endianness::<E>().load()?,
                BvGraph::with_basename(&t_path).endianness::<E>().load()?,
                no_loops,
                builder,
                thread_pool,
                target_endianness,
                log_interval
            );
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
                // calling symmetrize_par. PermutedGraph's SplitLabeling
                // uses split::seq::Iter, which advances sequentially to
                // each cutpoint; it cannot use split::ra::Iter because
                // PermutedGraph does not implement RandomAccessLabeling
                // (that would require the inverse permutation).
                let graph =
                    webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
                        .endianness::<E>()
                        .load()?;
                let num_nodes = graph.num_nodes();

                let cp = crate::cutpoints(
                    &args.src,
                    num_nodes,
                    graph.get_num_arcs(),
                    args.dcf,
                    thread_pool.current_num_threads(),
                )?;

                thread_pool.install(|| {
                    let mut pl =
                        progress_logger![display_memory = true, log_interval = log_interval];
                    let conf = ParSortedGraph::config()
                        .dedup()
                        .memory_usage(memory_usage)
                        .progress_logger(&mut pl);

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

                    let sorted = conf.par_sort_pair_iters(num_nodes, pairs)?;
                    let mut builder = builder.progress_logger(&mut pl);
                    par_comp!(builder, sorted, target_endianness)
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

            macro_rules! symmetrize_and_compress {
                ($no_loops:expr) => {
                    thread_pool.install(|| {
                        let mut pl =
                            progress_logger![display_memory = true, log_interval = log_interval];
                        let sorted = webgraph::transform::symmetrize_sorted_par::<$no_loops, _>(
                            &graph,
                            args.memory_usage.memory_usage,
                            &mut pl,
                        )?;
                        let mut builder = builder.progress_logger(&mut pl);
                        par_comp!(builder, sorted, target_endianness)
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

/// Sequential version of [`symmetrize_par`].
pub fn symmetrize_seq<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let no_loops = args.no_loops;
    let log_interval = args.log_interval.log_interval;
    let memory_usage = args.memory_usage.memory_usage;
    let target_endianness = args.ca.target_endianness::<E>();
    let builder = args.ca.bv_comp_conf(&args.dst);

    // Symmetrizes sequentially a graph and compresses the result.
    macro_rules! symmetrize_and_compress {
        ($graph:expr) => {{
            let mut pl = progress_logger![display_memory = true, log_interval = log_interval];
            if no_loops {
                let sorted =
                    webgraph::transform::symmetrize_seq::<true>($graph, memory_usage, &mut pl)?;
                let mut builder = builder.progress_logger(&mut pl);
                thread_pool.install(|| par_comp!(builder, sorted, target_endianness))
            } else {
                let sorted =
                    webgraph::transform::symmetrize_seq::<false>($graph, memory_usage, &mut pl)?;
                let mut builder = builder.progress_logger(&mut pl);
                thread_pool.install(|| par_comp!(builder, sorted, target_endianness))
            }
        }};
    }

    match (args.permutation, args.transpose) {
        (None, Some(t_path)) => {
            log::info!("Transposed graph provided, using it to symmetrize the graph");
            compress_union!(
                BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?,
                BvGraphSeq::with_basename(&t_path)
                    .endianness::<E>()
                    .load()?,
                no_loops,
                builder,
                thread_pool,
                target_endianness,
                log_interval
            );
        }
        (Some(perm_path), None | Some(_)) => {
            log::info!("Permutation provided, applying it to the graph");

            let loaded = args.fmt.load(perm_path)?;

            dispatch_int_slice!(loaded, |perm| {
                let seq_graph = BvGraphSeq::with_basename(&args.src)
                    .endianness::<E>()
                    .load()?;
                symmetrize_and_compress!(&PermutedGraph::new(&seq_graph, perm))
            })?;
        }
        (None, None) => {
            log::info!(
                "No permutation or transposed graph provided, computing the transpose on the fly"
            );

            let seq_graph = BvGraphSeq::with_basename(&args.src)
                .endianness::<E>()
                .load()?;
            symmetrize_and_compress!(&seq_graph)?;
        }
    }

    Ok(())
}
