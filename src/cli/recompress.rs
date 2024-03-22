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

    let dir = Builder::new().prefix("Recompress").tempdir()?;
    let target_endianness = args.ca.endianess.clone();
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => {
            if args.basename.with_extension(EF_EXTENSION).exists() {
                let seq_graph = BVGraph::with_basename(&args.basename)
                    .endianness::<BE>()
                    .load()?;

                BVComp::parallel_endianness(
                    args.new_basename,
                    &seq_graph,
                    seq_graph.num_nodes(),
                    args.ca.into(),
                    Threads::Num(args.num_cpus.num_cpus),
                    dir,
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                )?;
            } else {
                log::warn!("The .ef file does not exist. The graph will be sequentially which will result in slower compression. If you can, run `build_ef` before recompressing.");
                let seq_graph = BVGraphSeq::with_basename(&args.basename)
                    .endianness::<BE>()
                    .load()?;

                BVComp::parallel_endianness(
                    args.new_basename,
                    &seq_graph,
                    seq_graph.num_nodes(),
                    args.ca.into(),
                    Threads::Num(args.num_cpus.num_cpus),
                    dir,
                    &target_endianness.unwrap_or_else(|| BE::NAME.into()),
                )?;
            }
        }
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => {
            if args.basename.with_extension(EF_EXTENSION).exists() {
                let seq_graph = BVGraph::with_basename(&args.basename)
                    .endianness::<LE>()
                    .load()?;

                BVComp::parallel_endianness(
                    args.new_basename,
                    &seq_graph,
                    seq_graph.num_nodes(),
                    args.ca.into(),
                    Threads::Num(args.num_cpus.num_cpus),
                    dir,
                    &target_endianness.unwrap_or_else(|| LE::NAME.into()),
                )?;
            } else {
                log::warn!("The .ef file does not exist. The graph will be sequentially which will result in slower compression. If you can, run `build_ef` before recompressing.");
                let seq_graph = BVGraphSeq::with_basename(&args.basename)
                    .endianness::<LE>()
                    .load()?;

                BVComp::parallel_endianness(
                    args.new_basename,
                    &seq_graph,
                    seq_graph.num_nodes(),
                    args.ca.into(),
                    Threads::Num(args.num_cpus.num_cpus),
                    dir,
                    &target_endianness.unwrap_or_else(|| LE::NAME.into()),
                )?;
            }
        }
        e => panic!("Unknown endianness: {}", e),
    };

    log::info!(
        "The compression took {:.3} seconds",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
