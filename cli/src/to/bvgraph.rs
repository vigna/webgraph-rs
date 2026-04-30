/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::create_parent_dir;
use crate::*;
use anyhow::Result;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use std::time::Duration;
use value_traits::slices::SliceByValue;

use std::path::PathBuf;
use tempfile::Builder;
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

    let target_endianness = args.ca.endianness.clone();
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => compress::<BE>(args, target_endianness),
        #[cfg(feature = "le_bins")]
        LE::NAME => compress::<LE>(args, target_endianness),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn compress<E: Endianness>(args: CliArgs, target_endianness: Option<String>) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let dir = Builder::new().prefix("to_bvgraph_").tempdir()?;

    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let sequential = args.sequential;
    let use_dcf = args.dcf;
    let src = args.src.clone();
    let memory_usage = args.memory_usage.memory_usage;
    let log_interval = args.log_interval.log_interval;
    let mut builder = BvCompConf::new(&args.dst)
        .comp_flags(args.ca.into())
        .tmp_dir(&dir);

    if bvgraphz {
        builder = builder.chunk_size(chunk_size);
    }

    if let Some(path) = args.permutation.as_ref() {
        let loaded = args.fmt.load(path)?;
        if sequential {
            dispatch_int_slice!(loaded, |perm| {
                compress_seq_with_perm::<E, _>(
                    thread_pool,
                    builder,
                    &src,
                    target_endianness,
                    memory_usage,
                    log_interval,
                    perm,
                )
            })
        } else {
            dispatch_int_slice!(loaded, |perm| {
                compress_par_with_perm::<E, _>(
                    thread_pool,
                    builder,
                    &src,
                    target_endianness,
                    memory_usage,
                    use_dcf,
                    log_interval,
                    perm,
                )
            })
        }
    } else if sequential {
        compress_seq_no_perm::<E>(thread_pool, builder, &src, target_endianness, log_interval)
    } else {
        compress_par_no_perm::<E>(
            thread_pool,
            builder,
            &src,
            target_endianness,
            use_dcf,
            log_interval,
        )
    }
}

/// Parallel version of [`compress_seq_with_perm`].
#[allow(clippy::too_many_arguments)]
pub fn compress_par_with_perm<E: Endianness, P: SliceByValue<Value = usize> + Send + Sync + Clone>(
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
    src: &std::path::Path,
    target_endianness: Option<String>,
    memory_usage: webgraph::utils::MemoryUsage,
    _use_dcf: bool,
    log_interval: Duration,
    perm: &P,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let te = target_endianness.unwrap_or_else(|| E::NAME.into());

    let graph = BvGraph::with_basename(src).endianness::<E>().load()?;
    thread_pool.install(|| {
        log::info!("Permuting graph with memory usage {}", memory_usage);
        let mut pl = progress_logger![display_memory = true, log_interval = log_interval];
        let start = std::time::Instant::now();
        let sorted = webgraph::transform::permute_par(&graph, perm, memory_usage, &mut pl)?;
        log::info!(
            "Permuted the graph. It took {:.3} seconds",
            start.elapsed().as_secs_f64()
        );
        let mut builder = builder.progress_logger(&mut pl);
        par_comp!(builder, sorted, te)
    })?;
    Ok(())
}

/// Sequential version of [`compress_par_with_perm`].
pub fn compress_seq_with_perm<E: Endianness, P: SliceByValue<Value = usize>>(
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
    src: &std::path::Path,
    target_endianness: Option<String>,
    memory_usage: webgraph::utils::MemoryUsage,
    log_interval: Duration,
    perm: &P,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let te = target_endianness.unwrap_or_else(|| E::NAME.into());

    let seq_graph = BvGraphSeq::with_basename(src).endianness::<E>().load()?;

    log::info!("Permuting graph with memory usage {}", memory_usage);
    let mut pl = progress_logger![display_memory = true, log_interval = log_interval];
    let start = std::time::Instant::now();
    let permuted = webgraph::transform::permute_seq(&seq_graph, perm, memory_usage, &mut pl)?;
    log::info!(
        "Permuted the graph. It took {:.3} seconds",
        start.elapsed().as_secs_f64()
    );

    let mut builder = builder.progress_logger(&mut pl);
    thread_pool.install(|| par_comp!(builder, permuted, te))?;
    Ok(())
}

/// Parallel version of [`compress_seq_no_perm`].
fn compress_par_no_perm<E: Endianness>(
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
    src: &std::path::Path,
    target_endianness: Option<String>,
    use_dcf: bool,
    log_interval: Duration,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let target_endianness = target_endianness.unwrap_or_else(|| E::NAME.into());

    let mut pl = progress_logger![display_memory = true, log_interval = log_interval];
    let mut builder = builder.progress_logger(&mut pl);

    let graph = BvGraph::with_basename(src).endianness::<E>().load()?;
    if use_dcf {
        use epserde::prelude::*;
        let dcf_path = src.with_extension(DEG_CUMUL_EXTENSION);
        let dcf = unsafe { DCF::mmap(&dcf_path, Flags::RANDOM_ACCESS) }?;
        let num_arcs = graph.num_arcs();
        let dcf_graph =
            ParGraph::with_dcf(graph, num_arcs, dcf.uncase(), rayon::current_num_threads());
        thread_pool.install(|| par_comp!(builder, &dcf_graph, target_endianness))?;
    } else {
        thread_pool.install(|| par_comp!(builder, &graph, target_endianness))?;
    }
    Ok(())
}

/// Sequential version of [`compress_par_no_perm`].
fn compress_seq_no_perm<E: Endianness>(
    thread_pool: rayon::ThreadPool,
    builder: BvCompConf,
    src: &std::path::Path,
    target_endianness: Option<String>,
    log_interval: Duration,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: Clone + Send + Sync,
{
    let target_endianness = target_endianness.unwrap_or_else(|| E::NAME.into());

    let mut pl = progress_logger![display_memory = true, log_interval = log_interval];
    let mut builder = builder.progress_logger(&mut pl);

    let seq_graph = BvGraphSeq::with_basename(src).endianness::<E>().load()?;
    thread_pool.install(|| par_comp!(builder, &seq_graph, target_endianness))?;
    Ok(())
}
