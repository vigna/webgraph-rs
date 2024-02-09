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

pub const COMMAND_NAME: &str = "simplify";

#[derive(Args, Debug)]
#[command(about = "Simplify a BVGraph, i.e. make it undirected and remove duplicates and selfloops", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
    /// The basename of the transposed graph. Defaults to `basename` + `.simple`.
    simplified: Option<PathBuf>,

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

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => simplify::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => simplify::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn simplify<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let simplified = args
        .simplified
        .unwrap_or_else(|| suffix_path(&args.basename, ".simple"));

    let seq_graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.basename)
        .endianness::<E>()
        .load()?;

    // transpose the graph
    let sorted = crate::algo::simplify(&seq_graph, args.pa.batch_size).unwrap();

    let target_endianness = args.ca.endianess.clone();
    BVComp::parallel_endianness(
        simplified,
        &sorted,
        sorted.num_nodes(),
        args.ca.into(),
        args.num_cpus.num_cpus,
        temp_dir(args.pa.temp_dir),
        &target_endianness.unwrap_or_else(|| E::NAME.into()),
    )?;

    Ok(())
}
