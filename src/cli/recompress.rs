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

pub const COMMAND_NAME: &str = "recompress";

#[derive(Args, Debug)]
#[command(about = "Recompress a BVGraph", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: String,
    /// The basename for the newly compressed graph.
    new_basename: String,

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

    let target_endianness = args.ca.endianess.clone();
    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => {
            let seq_graph = BVGraphSeq::with_basename(&args.basename)
                .endianness::<BE>()
                .load()?;

            BVComp::parallel_endianness(
                args.new_basename,
                &seq_graph,
                seq_graph.num_nodes(),
                args.ca.into(),
                args.num_cpus.num_cpus,
                temp_dir(args.pa.temp_dir),
                &target_endianness.unwrap_or_else(|| BE::NAME.into()),
            )?;
        }
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => {
            let seq_graph = BVGraphSeq::with_basename(&args.basename)
                .endianness::<LE>()
                .load()?;

            BVComp::parallel_endianness(
                args.new_basename,
                &seq_graph,
                seq_graph.num_nodes(),
                args.ca.into(),
                args.num_cpus.num_cpus,
                temp_dir(args.pa.temp_dir),
                &target_endianness.unwrap_or_else(|| LE::NAME.into()),
            )?;
        }
        e => panic!("Unknown endianness: {}", e),
    };

    Ok(())
}
