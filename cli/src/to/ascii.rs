/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
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
#[command(name = "ascii", about = "Dumps a graph in ASCII format: a line for each node with successors separated by tabs.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => ascii_convert::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => ascii_convert::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn ascii_convert<E: Endianness + 'static>(global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(args.src)
        .endianness::<E>()
        .load()?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true).item_name("offset");

    if let Some(duration) = global_args.log_interval {
        pl.log_interval(duration);
    }

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
