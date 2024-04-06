/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::utils::*;
use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use mmap_rs::MmapFlags;
use std::path::PathBuf;
use tempfile::Builder;

pub const COMMAND_NAME: &str = "recompress";

#[derive(Args, Debug)]
#[command(about = "Recompress a BVGraph", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
    /// The basename for the newly compressed graph.
    new_basename: PathBuf,

    #[clap(flatten)]
    num_cpus: NumCpusArg,

    #[clap(flatten)]
    pa: PermutationArgs,

    #[clap(flatten)]
    ca: CompressArgs,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let start = std::time::Instant::now();
    let args = CliArgs::from_arg_matches(submatches)?;

    let permutation = if let Some(path) = args.pa.permutation.as_ref() {
        Some(JavaPermutation::mmap(path, MmapFlags::RANDOM_ACCESS)?)
    } else {
        None
    };

    let target_endianness = args.ca.endianess.clone();
    match get_endianness(&args.basename)?.as_str() {
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

fn compress<E: Endianness + Clone + Send + Sync>(
    args: CliArgs,
    target_endianness: Option<String>,
    permutation: Option<JavaPermutation>,
) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let dir = Builder::new().prefix("Recompress").tempdir()?;

    if args.basename.with_extension(EF_EXTENSION).exists() {
        let graph = BVGraph::with_basename(&args.basename)
            .endianness::<E>()
            .load()?;

        if let Some(permutation) = permutation {
            let batch_size = args.pa.batch_size;

            log::info!("Permuting graph with batch size {}", batch_size);
            let start = std::time::Instant::now();
            // TODO!: this type annotation is not needed in the nightly version
            let sorted = crate::transform::permute_split::<
                BVGraph<
                    DynCodesDecoderFactory<
                        E,
                        MmapHelper<u32>,
                        sux::prelude::EliasFano<
                            sux::prelude::SelectFixed2<
                                sux::prelude::CountBitVec<&[usize]>,
                                &[u64],
                                8,
                            >,
                            sux::prelude::BitFieldVec<usize, &[usize]>,
                        >,
                    >,
                >,
                JavaPermutation,
            >(
                &graph,
                &permutation,
                batch_size,
                Threads::Num(args.num_cpus.num_cpus),
            )?;
            log::info!(
                "Permuted the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );
            BVComp::parallel_endianness(
                args.new_basename,
                &sorted,
                sorted.num_nodes(),
                args.ca.into(),
                Threads::Num(args.num_cpus.num_cpus),
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            BVComp::parallel_endianness(
                args.new_basename,
                &graph,
                graph.num_nodes(),
                args.ca.into(),
                Threads::Num(args.num_cpus.num_cpus),
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        }
    } else {
        log::warn!("The .ef file does not exist. The graph will be sequentially which will result in slower compression. If you can, run `build_ef` before recompressing.");
        let seq_graph = BVGraphSeq::with_basename(&args.basename)
            .endianness::<E>()
            .load()?;

        if let Some(permutation) = permutation {
            let batch_size = args.pa.batch_size;

            log::info!("Permuting graph with batch size {}", batch_size);
            let start = std::time::Instant::now();
            let permuted = crate::transform::permute(&seq_graph, &permutation, batch_size)?;
            log::info!(
                "Permuted the graph. It took {:.3} seconds",
                start.elapsed().as_secs_f64()
            );

            BVComp::parallel_endianness(
                args.new_basename,
                &permuted,
                permuted.num_nodes(),
                args.ca.into(),
                Threads::Num(args.num_cpus.num_cpus),
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            BVComp::parallel_endianness(
                args.new_basename,
                &seq_graph,
                seq_graph.num_nodes(),
                args.ca.into(),
                Threads::Num(args.num_cpus.num_cpus),
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        }
    }
    Ok(())
}
