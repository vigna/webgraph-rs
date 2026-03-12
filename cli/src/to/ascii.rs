/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::LogIntervalArg;
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::*;
use std::path::PathBuf;
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::traits::SequentialLabeling;
use webgraph::utils::MmapHelper;

#[derive(Parser, Debug)]
#[command(name = "ascii", about = "Writes a graph in ASCII to standard output as a line for each node with successors separated by TABs.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => ascii_convert::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => ascii_convert::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn ascii_convert<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(args.basename)
        .endianness::<E>()
        .load()?;

    let mut pl = progress_logger![
        display_memory = true,
        item_name = "node",
        expected_updates = Some(seq_graph.num_nodes())
    ];

    if let Some(duration) = args.log_interval.log_interval {
        pl.log_interval(duration);
    }

    pl.start("Writing graph in ASCII format...");

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
        pl.light_update();
    }

    pl.done();

    Ok(())
}
