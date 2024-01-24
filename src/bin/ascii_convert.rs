/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use lender::*;
use webgraph::graphs::bvgraph::{get_endianess, CodeRead};
use webgraph::traits::SequentialLabelling;

#[derive(Parser, Debug)]
#[command(about = "Dumps a graph as an COO arc list", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

fn ascii_convert<E: Endianness + 'static>(args: Args) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let seq_graph = webgraph::graphs::bvgraph::sequential::with_basename(&args.basename)
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

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    match get_endianess(&args.basename)?.as_str() {
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
