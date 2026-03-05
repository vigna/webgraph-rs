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

use epserde::prelude::*;
use mmap_rs::MmapFlags;
use std::path::PathBuf;
use sux::traits::IndexedSeq;
use sux::utils::FairChunks;
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
    /// The path to an optional permutation in binary big-endian format to be applied to the graph.
    pub permutation: Option<PathBuf>,

    #[arg(long)]
    /// Use the degree cumulative function to balance work by arcs rather than
    /// by nodes. The DCF must have been pre-built with `webgraph build dcf`.
    pub dcf: bool,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    let permutation = if let Some(path) = args.permutation.as_ref() {
        Some(JavaPermutation::mmap(path, MmapFlags::RANDOM_ACCESS)?)
    } else {
        None
    };

    let target_endianness = args.ca.endianness.clone();
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => compress::<BE>(global_args, args, target_endianness, permutation),
        #[cfg(feature = "le_bins")]
        LE::NAME => compress::<LE>(global_args, args, target_endianness, permutation),
        e => panic!("Unknown endianness: {}", e),
    }
}

/// Computes cutpoints for splitting a graph into chunks.
///
/// If `use_dcf` is true and a `.dcf` file exists, uses `FairChunks` to balance
/// by arc count. Otherwise, falls back to uniform cutpoints by node count.
fn cutpoints(
    basename: &std::path::Path,
    num_nodes: usize,
    num_arcs: Option<u64>,
    use_dcf: bool,
) -> Result<Vec<usize>> {
    if use_dcf {
        let dcf_path = basename.with_extension(DEG_CUMUL_EXTENSION);
        anyhow::ensure!(
            dcf_path.exists(),
            "DCF file {} does not exist; build it with `webgraph build dcf`",
            dcf_path.display()
        );
        let dcf = unsafe { DCF::mmap(&dcf_path, Flags::RANDOM_ACCESS) }?;
        let dcf = dcf.uncase();
        anyhow::ensure!(
            dcf.len() == num_nodes + 1,
            "DCF has {} entries, expected {} (num_nodes + 1)",
            dcf.len(),
            num_nodes + 1
        );
        anyhow::ensure!(dcf.get(0) == 0, "DCF does not start with 0");
        let num_arcs = num_arcs.expect("num_arcs_hint required for --dcf") as usize;
        anyhow::ensure!(
            dcf.get(num_nodes) == num_arcs,
            "DCF ends with {}, expected {} (num_arcs)",
            dcf.get(num_nodes),
            num_arcs
        );
        let num_threads = rayon::current_num_threads();
        let target_weight = num_arcs.div_ceil(num_threads);
        let cutpoints: Vec<usize> = std::iter::once(0)
            .chain(FairChunks::new(target_weight, &dcf).map(|r| r.end))
            .collect();
        log::info!(
            "Using DCF-based splitting into {} parts",
            cutpoints.len() - 1
        );
        Ok(cutpoints)
    } else {
        let n = rayon::current_num_threads();
        let step = num_nodes.div_ceil(n);
        Ok((0..n + 1).map(move |i| (i * step).min(num_nodes)).collect())
    }
}

pub fn compress<E: Endianness>(
    _global_args: GlobalArgs,
    args: CliArgs,
    target_endianness: Option<String>,
    permutation: Option<JavaPermutation>,
) -> Result<()>
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
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    if src.with_extension(EF_EXTENSION).exists() {
        let graph = BvGraph::with_basename(&src).endianness::<E>().load()?;

        if let Some(permutation) = permutation {
            let memory_usage = args.memory_usage.memory_usage;
            thread_pool.install(|| {
                log::info!("Permuting graph with memory usage {}", memory_usage);
                let start = std::time::Instant::now();
                let sorted =
                    webgraph::transform::permute_split(&graph, &permutation, memory_usage)?;
                log::info!(
                    "Permuted the graph. It took {:.3} seconds",
                    start.elapsed().as_secs_f64()
                );
                let cp = cutpoints(&src, sorted.num_nodes(), sorted.num_arcs_hint(), use_dcf)?;
                builder.par_comp_lenders_endianness_at(
                    &sorted,
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                    cp,
                )
            })?;
        } else {
            thread_pool.install(|| {
                let cp = cutpoints(&src, graph.num_nodes(), graph.num_arcs_hint(), use_dcf)?;
                builder.par_comp_lenders_endianness_at(
                    &graph,
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                    cp,
                )
            })?;
        }
    } else {
        log::warn!(
            "The .ef file does not exist. The graph will be read sequentially which will result in slower compression. If you can, run `webgraph build ef` before recompressing."
        );
        let seq_graph = BvGraphSeq::with_basename(&src).endianness::<E>().load()?;

        if let Some(permutation) = permutation {
            let memory_usage = args.memory_usage.memory_usage;

            log::info!("Permuting graph with memory usage {}", memory_usage);
            let start = std::time::Instant::now();
            let permuted = webgraph::transform::permute(&seq_graph, &permutation, memory_usage)?;
            log::info!(
                "Permuted the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );

            thread_pool.install(|| {
                let cp = cutpoints(
                    &src,
                    permuted.num_nodes(),
                    permuted.num_arcs_hint(),
                    use_dcf,
                )?;
                builder.par_comp_lenders_endianness_at(
                    &permuted,
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                    cp,
                )
            })?;
        } else {
            thread_pool.install(|| {
                let cp = cutpoints(
                    &src,
                    seq_graph.num_nodes(),
                    seq_graph.num_arcs_hint(),
                    use_dcf,
                )?;
                builder.par_comp_lenders_endianness_at(
                    &seq_graph,
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                    cp,
                )
            })?;
        }
    }
    Ok(())
}
