/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::SequentialLabeling;
use crate::{graphs::bvgraph::get_endianness, prelude::MemBufReader};
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::*;
use std::convert::Infallible;
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "ascii";

#[derive(Args, Debug)]
#[command(about = "Dumps a graph in ASCII format: a line for each node with successors separated by tabs.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => ascii_convert::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => ascii_convert::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn ascii_convert<E: Endianness>(args: CliArgs) -> Result<()>
where
    for<'a> MemBufReader<'a, E>: CodesRead<E, Error = Infallible> + BitSeek,
{
    let seq_graph = crate::graphs::bvgraph::sequential::BvGraphSeq::with_basename(args.src)
        .endianness::<E>()
        .load()?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true).item_name("offset");
    pl.start("Computing offsets...");

    let mut iter = seq_graph.iter();
    while let Some((node_id, successors)) = iter.next() {
        println!(
            "{}\t{}",
            node_id,
            successors
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join("\t")
        );
    }

    pl.done();

    Ok(())
}
