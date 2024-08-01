/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::bvgraph::{get_endianness, CodeRead};
use crate::traits::SequentialLabeling;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::*;
use std::io::Write;
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "csv";

#[derive(Args, Debug)]
#[command(about = "Dumps a graph as an ASCII list of arcs to stdout.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    #[arg(long, default_value_t = ',')]
    /// The separator between source and target nodes.
    pub separator: char,
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
        BE::NAME => to_csv::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => to_csv::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn to_csv<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(args.src)
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
            writeln!(stdout, "{}{}{}", src, args.separator, dst)?;
        }
        pl.light_update();
    });

    pl.done();
    Ok(())
}
