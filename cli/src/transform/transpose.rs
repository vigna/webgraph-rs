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
#[command(name = "transpose", about = "Transposes a graph in the BV format.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub src: PathBuf,
    /// The basename of the transposed graph.​
    pub dst: PathBuf,

    #[arg(short, long)]
    /// Uses the sequential algorithm (does not need offsets).​
    pub sequential: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[arg(long, conflicts_with = "sequential")]
    /// Uses the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    dispatch_endianness!(&args.src, |E| {
        if args.sequential {
            transpose_seq::<E>(args)
        } else {
            transpose_par::<E>(args)
        }
    })
}

/// Sequential version of [`transpose_par`].
pub fn transpose_seq<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    // TODO!: speed it up by using random access graph if possible
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    // transpose the graph
    let mut pl = progress_logger![
        display_memory = true,
        log_interval = args.log_interval.log_interval
    ];
    let sorted =
        webgraph::transform::transpose_seq(&seq_graph, args.memory_usage.memory_usage, &mut pl)?;

    // Use uniform cutpoints for compression of the transposed graph
    // (the source DCF does not match the transpose's degree distribution)
    let target_endianness = args.ca.target_endianness::<E>();
    let mut builder = args.ca.bv_comp_conf(&args.dst).progress_logger(&mut pl);
    thread_pool.install(|| par_comp!(builder, sorted, target_endianness))?;

    Ok(())
}

/// Parallel version of [`transpose_seq`].
pub fn transpose_par<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    let graph = webgraph::graphs::bvgraph::BvGraph::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    let par_graph = crate::par_graph(
        graph,
        &args.src,
        args.dcf,
        thread_pool.current_num_threads(),
    )?;

    // transpose the graph
    let mut pl = progress_logger![
        display_memory = true,
        log_interval = args.log_interval.log_interval
    ];
    let sorted =
        webgraph::transform::transpose_par(&par_graph, args.memory_usage.memory_usage, &mut pl)?;

    let target_endianness = args.ca.target_endianness::<E>();
    let mut builder = args.ca.bv_comp_conf(&args.dst).progress_logger(&mut pl);
    thread_pool.install(|| par_comp!(builder, sorted, target_endianness))?;
    Ok(())
}
