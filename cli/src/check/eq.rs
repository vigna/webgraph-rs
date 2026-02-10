/*
 * SPDX-FileCopyrightText: 2024 Davide Cologni
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use anyhow::Result;
use clap::Args;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use std::{path::PathBuf, process::exit};
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::traits::{SequentialLabeling, graph};
use webgraph::utils::MmapHelper;

#[derive(Args, Debug)]
#[command(name = "eq", about = "Checks that two graphs have the same contents, listed in the same order. Useful to check equality when two graph are compressed with different parameters or with different algorithms (think about reference selection).", long_about = None)]
pub struct CliArgs {
    /// The basename of the first graph.
    pub first_basename: PathBuf,
    /// The basename of the second graph.
    pub second_basename: PathBuf,
}

pub fn main(_global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.first_basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => compare_graphs::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => compare_graphs::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn compare_graphs<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let first_graph =
        webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.first_basename)
            .endianness::<E>()
            .load()?;
    let second_graph =
        webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.second_basename)
            .endianness::<E>()
            .load()?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("compare graphs")
        .expected_updates(Some(first_graph.num_nodes()));

    pl.start("Start comparing the graphs...");

    let result = graph::eq(&first_graph, &second_graph);
    if let Err(eq_error) = result {
        eprintln!("{}", eq_error);
        exit(1);
    }

    pl.done();
    Ok(())
}
