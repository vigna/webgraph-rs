/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use crate::cli::utils::NumCpusArg;

pub const COMMAND_NAME: &str = "hyperball";

#[derive(Args, Debug)]
#[command(about = "Create the .hyperball.X files for approximated centralities", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: String,

    /// The prefix of the files, it defaults to the basename of the graph.
    dst: Option<String>,

    /// Log2 of the number of registers to use for each hyperloglog counter
    #[arg(short = 'p', long, default_value = "4")]
    log2_precision: usize,

    #[arg(short = 'r', long, default_value_t = 500)]
    /// The size of the chunks each thread processes for the LLP.
    granularity: usize,

    #[clap(flatten)]
    num_cpus: NumCpusArg,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => hyperball::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => hyperball::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn hyperball<E: Endianness + Sync + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let graph = BVGraph::with_basename(&args.basename)
        .endianness::<E>()
        .load()?;

    crate::algo::hyperball(
        &graph,
        args.dst.as_ref().unwrap_or(&args.basename),
        args.log2_precision,
        Some(args.num_cpus.num_cpus),
        args.granularity,
    )
}
