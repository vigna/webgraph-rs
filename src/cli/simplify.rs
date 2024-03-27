/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{append, utils::*};
use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use std::path::PathBuf;
use tempfile::Builder;

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

    match get_endianness(&args.basename)?.as_str() {
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
    // TODO!: speed it up by using random access graph if possible
    let simplified = args
        .simplified
        .unwrap_or_else(|| append(&args.basename, "-simple"));

    let seq_graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.basename)
        .endianness::<E>()
        .load()?;

    // transpose the graph
    let sorted = crate::transform::simplify(&seq_graph, args.pa.batch_size).unwrap();

    let target_endianness = args.ca.endianess.clone();
    let dir = Builder::new().prefix("CompressSimplified").tempdir()?;
    BVComp::parallel_endianness(
        simplified,
        &sorted,
        sorted.num_nodes(),
        args.ca.into(),
        Threads::Num(args.num_cpus.num_cpus),
        dir,
        &target_endianness.unwrap_or_else(|| E::NAME.into()),
    )?;

    Ok(())
}
