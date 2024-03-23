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

pub const COMMAND_NAME: &str = "simplify";

#[derive(Args, Debug)]
#[command(about = "Simplify a BVGraph, i.e. make it undirected and remove duplicates and selfloops", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
    /// The basename of the transposed graph.
    simplified: PathBuf,

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
        BE::NAME => simplify::<BE>(args, target_endianness, permutation),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => simplify::<LE>(args, target_endianness, permutation),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn simplify<E: Endianness + 'static + Clone + Send + Sync>(
    args: CliArgs,
    target_endianness: Option<String>,
    permutation: Option<JavaPermutation>,
) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let dir = Builder::new().prefix("Simplify").tempdir()?;

    if args.basename.with_extension(EF_EXTENSION).exists() {
        let graph = BVGraph::with_basename(&args.basename)
            .endianness::<E>()
            .load()?;

        if let Some(permutation) = permutation {
            let permuted = PermutedGraph {
                graph: &graph,
                perm: &permutation,
            };
            let simplified = crate::transform::simplify_split(
                &permuted,
                args.pa.batch_size,
                Threads::Num(args.num_cpus.num_cpus),
            )?;
            log::debug!("Created simplified graph iter");
            BVComp::parallel_endianness(
                args.simplified,
                &simplified,
                simplified.num_nodes(),
                args.ca.into(),
                Threads::Num(args.num_cpus.num_cpus),
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            let simplified = crate::transform::simplify(&graph, args.pa.batch_size)?;
            log::debug!("Created simplified graph iter");
            BVComp::parallel_endianness(
                args.simplified,
                &simplified,
                simplified.num_nodes(),
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
            let permuted = PermutedGraph {
                graph: &seq_graph,
                perm: &permutation,
            };
            let simplified = crate::transform::simplify(&permuted, args.pa.batch_size)?;
            log::debug!("Created simplified graph iter");
            BVComp::parallel_endianness(
                args.simplified,
                &simplified,
                simplified.num_nodes(),
                args.ca.into(),
                Threads::Num(args.num_cpus.num_cpus),
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        } else {
            let simplified = crate::transform::simplify(&seq_graph, args.pa.batch_size)?;
            log::debug!("Created simplified graph iter");
            BVComp::parallel_endianness(
                args.simplified,
                &simplified,
                simplified.num_nodes(),
                args.ca.into(),
                Threads::Num(args.num_cpus.num_cpus),
                dir,
                &target_endianness.unwrap_or_else(|| E::NAME.into()),
            )?;
        }
    }
    Ok(())
}
