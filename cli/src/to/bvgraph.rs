/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::create_parent_dir;
use crate::*;
use anyhow::Result;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use value_traits::slices::SliceByValue;

use std::path::PathBuf;
use tempfile::Builder;
use webgraph::prelude::*;
use webgraph::traits::SequentialLabeling;

#[derive(Parser, Debug)]
#[command(name = "bvgraph", about = "Recompresses a BvGraph, possibly applying a permutation to its node identifiers.", long_about = None)]
pub struct CliArgs {
    /// The basename of the source graph.
    pub src: PathBuf,
    /// The basename of the destination graph.
    pub dst: PathBuf,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[arg(long)]
    /// The path to an optional permutation to be applied to the graph.
    pub permutation: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.
    pub fmt: IntSliceFormat,

    #[arg(long)]
    /// Use the degree cumulative function to balance work by arcs rather than
    /// by nodes. The DCF must have been pre-built with `webgraph build dcf`.
    pub dcf: bool,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,
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
    let use_dcf = args.dcf;
    let src = args.src.clone();
    let memory_usage = args.memory_usage.memory_usage;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    if let Some(path) = args.permutation.as_ref() {
        let loaded = args.fmt.load(path)?;
        dispatch_int_slice!(loaded, |perm| {
            compress_with_perm::<E, _>(
                thread_pool,
                builder,
                &src,
                target_endianness,
                memory_usage,
                use_dcf,
                perm,
            )
        })
    } else {
        compress_no_perm::<E>(thread_pool, builder, &src, target_endianness, use_dcf)
    }
}

pub fn compress_with_perm<E: Endianness, P: SliceByValue<Value = usize> + Send + Sync>(
    thread_pool: rayon::ThreadPool,
    mut builder: BvCompConfig,
    src: &std::path::Path,
    target_endianness: Option<String>,
    memory_usage: webgraph::utils::MemoryUsage,
    use_dcf: bool,
    perm: &P,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let te = target_endianness.unwrap_or_else(|| E::NAME.into());

    if std::fs::metadata(src.with_extension(EF_EXTENSION)).is_ok_and(|x| x.is_file()) {
        let graph = BvGraph::with_basename(src).endianness::<E>().load()?;
        let num_nodes = graph.num_nodes();
        thread_pool.install(|| {
            log::info!("Permuting graph with memory usage {}", memory_usage);
            let start = std::time::Instant::now();
            let sorted = webgraph::transform::permute_split(&graph, perm, memory_usage)?;
            log::info!(
                "Permuted the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );
            let pairs: Vec<_> = sorted.into();
            match te.as_str() {
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
    } else {
        log::warn!(SEQ_PROC_WARN![], src.display());
        let seq_graph = BvGraphSeq::with_basename(src).endianness::<E>().load()?;

        log::info!("Permuting graph with memory usage {}", memory_usage);
        let start = std::time::Instant::now();
        let permuted = webgraph::transform::permute(&seq_graph, perm, memory_usage)?;
        log::info!(
            "Permuted the graph. It took {:.3} seconds",
            start.elapsed().as_secs_f64()
        );

        thread_pool.install(|| {
            let cp = cutpoints(src, permuted.num_nodes(), permuted.num_arcs_hint(), use_dcf)?;
            builder.par_comp_lenders_endianness_at(&permuted, &te, cp)
        })?;
    }
    Ok(())
}

fn compress_no_perm<E: Endianness>(
    thread_pool: rayon::ThreadPool,
    mut builder: BvCompConfig,
    src: &std::path::Path,
    target_endianness: Option<String>,
    use_dcf: bool,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Send + Sync + Clone,
{
    let te = target_endianness.unwrap_or_else(|| E::NAME.into());

    if std::fs::metadata(src.with_extension(EF_EXTENSION)).is_ok_and(|x| x.is_file()) {
        let graph = BvGraph::with_basename(src).endianness::<E>().load()?;
        thread_pool.install(|| {
            let cp = crate::cutpoints(src, graph.num_nodes(), graph.num_arcs_hint(), use_dcf)?;
            builder.par_comp_lenders_endianness_at(&graph, &te, cp)
        })?;
    } else {
        log::warn!(SEQ_PROC_WARN![], src.display());
        let seq_graph = BvGraphSeq::with_basename(src).endianness::<E>().load()?;
        thread_pool.install(|| {
            let cp = cutpoints(
                src,
                seq_graph.num_nodes(),
                seq_graph.num_arcs_hint(),
                use_dcf,
            )?;
            builder.par_comp_lenders_endianness_at(&seq_graph, &te, cp)
        })?;
    }
    Ok(())
}
