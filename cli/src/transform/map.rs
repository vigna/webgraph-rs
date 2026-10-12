/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
use value_traits::slices::SliceByValue;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "map", about = "Maps a graph in the BV format through an arbitrary function on nodes, deduplicating arcs.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the source graph.​
    pub src: PathBuf,
    /// The basename of the mapped graph.​
    pub dst: PathBuf,

    /// The path to the map to apply to the graph.​
    pub map: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the map file.​
    pub fmt: IntSliceFormat,

    #[arg(long)]
    /// The number of nodes of the resulting graph; if not specified, it is
    /// computed as one plus the maximum value in the map; if specified, it must
    /// be strictly greater than the maximum value in the map.​
    pub num_nodes: Option<usize>,

    #[arg(short, long)]
    /// Uses the sequential algorithm (does not need offsets).​
    pub sequential: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[arg(long, conflicts_with = "sequential")]
    /// Uses the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    dispatch_endianness!(&args.src, |E| {
        if args.sequential {
            map_seq::<E>(args)
        } else {
            map_par::<E>(args)
        }
    })
}

/// Computes the number of nodes of the mapped graph.
fn mapped_num_nodes<P: SliceByValue<Value = usize>>(
    node_map: &P,
    cli_num_nodes: Option<usize>,
) -> Result<usize> {
    let max_mapped = (0..node_map.len())
        .map(|i| node_map.index_value(i))
        .max()
        .unwrap_or(0);
    match cli_num_nodes {
        Some(n) => {
            anyhow::ensure!(
                n > max_mapped,
                "The specified number of nodes ({}) is not strictly greater than the maximum mapped node ({})",
                n,
                max_mapped,
            );
            Ok(n)
        }
        None => Ok(max_mapped + 1),
    }
}

/// Parallel version of [`map_seq`].
pub fn map_par<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let log_interval = args.log_interval.log_interval;
    let target_endianness = args.ca.target_endianness::<E>();
    let builder = args.ca.bv_comp_conf(&args.dst);

    let loaded = args.fmt.load(&args.map)?;
    let cli_num_nodes = args.num_nodes;
    let memory_usage = args.memory_usage.memory_usage;

    dispatch_int_slice!(loaded, |node_map| {
        let num_nodes = mapped_num_nodes(&node_map, cli_num_nodes)?;

        let graph = webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.src)
            .endianness::<E>()
            .load()?;

        let par_graph = crate::par_graph(
            graph,
            &args.src,
            args.dcf,
            thread_pool.current_num_threads(),
        )?;

        thread_pool.install(|| {
            log::info!("Mapping graph with memory usage {}", memory_usage);
            let mut pl = progress_logger![display_memory = true, log_interval = log_interval];
            let start = std::time::Instant::now();
            let sorted = webgraph::transform::map_par(
                &par_graph,
                &node_map,
                num_nodes,
                memory_usage,
                &mut pl,
            )?;
            log::info!(
                "Mapped the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );

            let mut builder = builder.progress_logger(&mut pl);
            par_comp!(builder, sorted, target_endianness)
        })?;

        Ok(())
    })
}

/// Sequential version of [`map_par`].
pub fn map_seq<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let log_interval = args.log_interval.log_interval;
    let target_endianness = args.ca.target_endianness::<E>();
    let builder = args.ca.bv_comp_conf(&args.dst);

    let loaded = args.fmt.load(&args.map)?;
    let cli_num_nodes = args.num_nodes;
    let memory_usage = args.memory_usage.memory_usage;

    dispatch_int_slice!(loaded, |node_map| {
        let num_nodes = mapped_num_nodes(&node_map, cli_num_nodes)?;

        let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
            .endianness::<E>()
            .load()?;

        log::info!("Mapping graph with memory usage {}", memory_usage);
        let mut pl = progress_logger![display_memory = true, log_interval = log_interval];
        let start = std::time::Instant::now();
        let sorted =
            webgraph::transform::map_seq(&seq_graph, &node_map, num_nodes, memory_usage, &mut pl)?;
        log::info!(
            "Mapped the graph. It took {:.3} seconds",
            start.elapsed().as_secs_f64()
        );

        let mut builder = builder.progress_logger(&mut pl);
        thread_pool.install(|| par_comp!(builder, sorted, target_endianness))?;

        Ok(())
    })
}
