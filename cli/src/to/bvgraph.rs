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
use epserde::deser::DeserializeInner;
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

    #[clap(long)]
    /// The path to an optional permutation in binary big-endian format to be applied to the graph.
    pub permutation: Option<PathBuf>,

    #[clap(flatten)]
    pub batch_size: BatchSizeArg,

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
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => compress::<BE>(global_args, args, target_endianness, permutation)?,
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => compress::<LE>(global_args, args, target_endianness, permutation)?,
        e => panic!("Unknown endianness: {}", e),
    };
    Ok(())
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

    if args.src.with_extension(EF_EXTENSION).exists() {
        let graph = BvGraph::with_basename(&args.src).endianness::<E>().load()?;

        if let Some(permutation) = permutation {
            let batch_size = args.batch_size.batch_size;

            log::info!("Permuting graph with batch size {}", batch_size);
            let start = std::time::Instant::now();
            // TODO!: this type annotation is not needed in the nightly version
            let sorted = webgraph::transform::permute_split::<
                BvGraph<
                    DynCodesDecoderFactory<
                        E,
                        MmapHelper<u32>,
                        <EF as DeserializeInner>::DeserType<'_>,
                    >,
                >,
                JavaPermutation,
            >(&graph, &permutation, batch_size, &thread_pool)?;
            log::info!(
                "Permuted the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );
            BvComp::parallel_endianness(
                args.dst,
                &sorted,
                sorted.num_nodes(),
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            BvComp::parallel_endianness(
                args.dst,
                &graph,
                graph.num_nodes(),
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        }
    } else {
        log::warn!(
            "The .ef file does not exist. The graph will be sequentially which will result in slower compression. If you can, run `build_ef` before recompressing."
        );
        let seq_graph = BvGraphSeq::with_basename(&args.src)
            .endianness::<E>()
            .load()?;

        if let Some(permutation) = permutation {
            let batch_size = args.batch_size.batch_size;

            log::info!("Permuting graph with batch size {}", batch_size);
            let start = std::time::Instant::now();
            let permuted = webgraph::transform::permute(&seq_graph, &permutation, batch_size)?;
            log::info!(
                "Permuted the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );

            BvComp::parallel_endianness(
                args.dst,
                &permuted,
                permuted.num_nodes(),
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            BvComp::parallel_endianness(
                args.dst,
                &seq_graph,
                seq_graph.num_nodes(),
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        }
    }
    Ok(())
}
