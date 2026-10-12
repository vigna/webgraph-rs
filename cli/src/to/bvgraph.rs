/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::create_parent_dir;
use crate::*;
use anyhow::Result;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use value_traits::slices::SliceByValue;

use std::path::PathBuf;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "bvgraph", about = "Writes a graph in the BV format, possibly applying a permutation to its node identifiers.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the source graph.​
    pub src: PathBuf,
    /// The basename of the destination graph.​
    pub dst: PathBuf,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[arg(long)]
    /// The path to an optional permutation to be applied to the graph.​
    pub permutation: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.​
    pub fmt: IntSliceFormat,

    #[arg(short, long)]
    /// Uses the sequential algorithm (does not need offsets).​
    pub sequential: bool,

    #[arg(long, conflicts_with = "sequential")]
    /// Uses the degree cumulative function to balance work by arcs rather than
    /// by nodes. The DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;
    dispatch_endianness!(&args.src, |E| compress::<E>(args))
}

pub fn compress<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let builder = args.ca.bv_comp_conf(&args.dst);

    if let Some(path) = args.permutation.as_ref() {
        let loaded = args.fmt.load(path)?;
        if args.sequential {
            dispatch_int_slice!(loaded, |perm| {
                compress_seq_with_perm::<E, _>(&args, thread_pool, builder, perm)
            })
        } else {
            dispatch_int_slice!(loaded, |perm| {
                compress_par_with_perm::<E, _>(&args, thread_pool, builder, perm)
            })
        }
    } else if args.sequential {
        compress_seq_no_perm::<E>(&args, thread_pool, builder)
    } else {
        compress_par_no_perm::<E>(&args, thread_pool, builder)
    }
}

/// Parallel version of [`compress_seq_with_perm`].
fn compress_par_with_perm<E: Endianness, P: SliceByValue<Value = usize> + Send + Sync + Clone>(
    args: &CliArgs,
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
    perm: &P,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let target_endianness = args.ca.target_endianness::<E>();
    let memory_usage = args.memory_usage.memory_usage;

    let graph = BvGraph::with_basename(&args.src).endianness::<E>().load()?;
    thread_pool.install(|| {
        log::info!("Permuting graph with memory usage {}", memory_usage);
        let mut pl = progress_logger![
            display_memory = true,
            log_interval = args.log_interval.log_interval
        ];
        let start = std::time::Instant::now();
        let sorted = if args.dcf {
            let par_graph = crate::par_graph(graph, &args.src, true, rayon::current_num_threads())?;
            webgraph::transform::permute_par(&par_graph, perm, memory_usage, &mut pl)?
        } else {
            webgraph::transform::permute_par(&graph, perm, memory_usage, &mut pl)?
        };
        log::info!(
            "Permuted the graph. It took {:.3} seconds",
            start.elapsed().as_secs_f64()
        );
        let mut builder = builder.progress_logger(&mut pl);
        par_comp!(builder, sorted, target_endianness)
    })?;
    Ok(())
}

/// Sequential version of [`compress_par_with_perm`].
fn compress_seq_with_perm<E: Endianness, P: SliceByValue<Value = usize>>(
    args: &CliArgs,
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
    perm: &P,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let target_endianness = args.ca.target_endianness::<E>();
    let memory_usage = args.memory_usage.memory_usage;

    let seq_graph = BvGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    log::info!("Permuting graph with memory usage {}", memory_usage);
    let mut pl = progress_logger![
        display_memory = true,
        log_interval = args.log_interval.log_interval
    ];
    let start = std::time::Instant::now();
    let permuted = webgraph::transform::permute_seq(&seq_graph, perm, memory_usage, &mut pl)?;
    log::info!(
        "Permuted the graph. It took {:.3} seconds",
        start.elapsed().as_secs_f64()
    );

    let mut builder = builder.progress_logger(&mut pl);
    thread_pool.install(|| par_comp!(builder, permuted, target_endianness))?;
    Ok(())
}

/// Parallel version of [`compress_seq_no_perm`].
fn compress_par_no_perm<E: Endianness>(
    args: &CliArgs,
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let target_endianness = args.ca.target_endianness::<E>();
    let mut pl = progress_logger![
        display_memory = true,
        log_interval = args.log_interval.log_interval
    ];
    let mut builder = builder.progress_logger(&mut pl);

    let graph = BvGraph::with_basename(&args.src).endianness::<E>().load()?;
    if args.dcf {
        let par_graph =
            crate::par_graph(graph, &args.src, true, thread_pool.current_num_threads())?;
        thread_pool.install(|| par_comp!(builder, &par_graph, target_endianness))?;
    } else {
        thread_pool.install(|| par_comp!(builder, &graph, target_endianness))?;
    }
    Ok(())
}

/// Sequential version of [`compress_par_no_perm`].
fn compress_seq_no_perm<E: Endianness>(
    args: &CliArgs,
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: Clone + Send + Sync,
{
    let target_endianness = args.ca.target_endianness::<E>();
    let mut pl = progress_logger![
        display_memory = true,
        log_interval = args.log_interval.log_interval
    ];
    let mut builder = builder.progress_logger(&mut pl);

    let seq_graph = BvGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()?;
    thread_pool.install(|| par_comp!(builder, &seq_graph, target_endianness))?;
    Ok(())
}
