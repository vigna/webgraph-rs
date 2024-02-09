/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::bvgraph::{get_endianess, CodeRead};
use crate::traits::SequentialLabeling;
use anyhow::Result;
use clap::{Arg, ArgMatches, Command};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use lender::*;
use std::io::Write;

pub const COMMAND_NAME: &str = "to-csv";

pub fn cli(command: Command) -> Command {
    command.subcommand(
        Command::new(COMMAND_NAME)
            .about("Dumps a graph as an COO arc list.")
            .long_about(None)
            .arg(
                Arg::new("basename")
                    .help("The basename of the graph")
                    .required(true),
            )
            .arg(
                Arg::new("csv_separator")
                    .long("csv-separator")
                    .help("The character used to separate the fields in the CSV")
                    .default_value(","),
            ),
    )
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let basename = submatches.get_one::<String>("basename").unwrap();
    let sep = submatches.get_one::<String>("csv-separator").unwrap();

    match get_endianess(basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => to_csv::<BE>(basename, sep),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => to_csv::<LE>(basename, sep),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn to_csv<E: Endianness + 'static>(basename: &str, sep: &str) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(basename)
        .endianness::<E>()
        .load()?;
    let num_nodes = graph.num_nodes();

    // read the csv and put it inside the sort pairs
    let mut stdout = std::io::BufWriter::new(std::io::stdout().lock());
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("nodes")
        .expected_updates(Some(num_nodes));
    pl.start("Reading BVGraph");

    for_! ( (src, succ) in graph.iter() {
        for dst in succ {
            writeln!(stdout, "{}{}{}", src, sep, dst)?;
        }
        pl.light_update();
    });

    pl.done();
    Ok(())
}
