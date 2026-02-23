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

use mmap_rs::MmapFlags;
use std::path::PathBuf;
use tempfile::Builder;
use webgraph::prelude::*;

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
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    if args.src.with_extension(EF_EXTENSION).exists() {
        let graph = BvGraph::with_basename(&args.src).endianness::<E>().load()?;

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
                builder.par_comp_lenders_endianness(
                    &sorted,
                    sorted.num_nodes(),
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                )
            })?;
        } else {
            thread_pool.install(|| {
                builder.par_comp_lenders_endianness(
                    &graph,
                    graph.num_nodes(),
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                )
            })?;
        }
    } else {
        log::warn!(
            "The .ef file does not exist. The graph will be read sequentially which will result in slower compression. If you can, run `webgraph build ef` before recompressing."
        );
        let seq_graph = BvGraphSeq::with_basename(&args.src)
            .endianness::<E>()
            .load()?;

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
                builder.par_comp_lenders_endianness(
                    &permuted,
                    permuted.num_nodes(),
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                )
            })?;
        } else {
            thread_pool.install(|| {
                builder.par_comp_lenders_endianness(
                    &seq_graph,
                    seq_graph.num_nodes(),
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                )
            })?;
        }
    }
    Ok(())
}
