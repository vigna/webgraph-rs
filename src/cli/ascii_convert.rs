/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::bvgraph::{get_endianness, CodeRead};
use crate::traits::SequentialLabeling;
use anyhow::Result;
use clap::{Arg, ArgMatches, Command};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use lender::*;

pub const COMMAND_NAME: &str = "ascii-convert";

pub fn cli(command: Command) -> Command {
    command
        .subcommand(
            Command::new(COMMAND_NAME)
                .about("Dumps a graph in ascii format, i.e. a line for each node with its successors separated by tabs.")
                .long_about(None)
                .arg(Arg::new("basename")
                    .help("The basename of the graph")
                    .required(true))
        )
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let basename = submatches.get_one::<String>("basename").unwrap();
    match get_endianness(basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => ascii_convert::<BE>(basename),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => ascii_convert::<LE>(basename),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn ascii_convert<E: Endianness + 'static>(basename: &str) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let seq_graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(basename)
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
