/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::create_parent_dir;
use crate::cli::*;
use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use epserde::deser::DeserializeInner;
use mmap_rs::MmapFlags;
use std::path::PathBuf;
use tempfile::Builder;

pub const COMMAND_NAME: &str = "bvgraph";

#[derive(Args, Debug)]
#[command(about = "Recompresses a BVGraph, possibly applying a permutation to its node identifiers.", long_about = None)]
pub struct CliArgs {
    /// The basename of the source graph.
    pub src: PathBuf,
    /// The basename of the destination graph.
    pub dst: PathBuf,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(long)]
    /// The path to an optional permutation to be applied to the graph.
    pub permutation: Option<PathBuf>,

    #[clap(flatten)]
    pub batch_size: BatchSizeArg,

    #[clap(flatten)]
    pub ca: CompressArgs,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let start = std::time::Instant::now();
    let args = CliArgs::from_arg_matches(submatches)?;

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
        BE::NAME => compress::<BE>(args, target_endianness, permutation)?,
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => compress::<LE>(args, target_endianness, permutation)?,
        e => panic!("Unknown endianness: {}", e),
    };

    log::info!(
        "The re-compression took {:.3} seconds",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

pub fn compress<E: Endianness + Clone + Send + Sync>(
    args: CliArgs,
    target_endianness: Option<String>,
    permutation: Option<JavaPermutation>,
) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let dir = Builder::new().prefix("Recompress").tempdir()?;

    let thread_pool = crate::cli::get_thread_pool(args.num_threads.num_threads);

    if args.src.with_extension(EF_EXTENSION).exists() {
        let graph = BVGraph::with_basename(&args.src).endianness::<E>().load()?;

        if let Some(permutation) = permutation {
            let batch_size = args.batch_size.batch_size;

            log::info!("Permuting graph with batch size {}", batch_size);
            let start = std::time::Instant::now();
            // TODO!: this type annotation is not needed in the nightly version
            let sorted = crate::transform::permute_split::<
                BVGraph<
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
            BVComp::parallel_endianness(
                args.dst,
                &sorted,
                sorted.num_nodes(),
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            BVComp::parallel_endianness(
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
        log::warn!("The .ef file does not exist. The graph will be sequentially which will result in slower compression. If you can, run `build_ef` before recompressing.");
        let seq_graph = BVGraphSeq::with_basename(&args.src)
            .endianness::<E>()
            .load()?;

        if let Some(permutation) = permutation {
            let batch_size = args.batch_size.batch_size;

            log::info!("Permuting graph with batch size {}", batch_size);
            let start = std::time::Instant::now();
            let permuted = crate::transform::permute(&seq_graph, &permutation, batch_size)?;
            log::info!(
                "Permuted the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );

            BVComp::parallel_endianness(
                args.dst,
                &permuted,
                permuted.num_nodes(),
                args.ca.into(),
                &thread_pool,
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            BVComp::parallel_endianness(
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
