/*
 * SPDX-FileCopyrightText: 2024 Davide Cologni
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Args;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use std::{path::PathBuf, process::exit};
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::traits::graph;
use webgraph::utils::MmapHelper;

#[derive(Args, Debug)]
#[command(
    name = "eq",
    about = "Checks that two graphs have the same contents, listed in the same order.",
    long_about = "Checks that two graphs have the same contents, listed in the same order. Useful to verify equality when two graphs are compressed with different parameters or algorithms (e.g., reference selection).",
    next_line_help = true
)]
pub struct CliArgs {
    /// The basename of the first graph.​
    pub first_basename: PathBuf,
    /// The basename of the second graph.​
    pub second_basename: PathBuf,
}

pub fn main(args: CliArgs) -> Result<()> {
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

    log::info!("Comparing graphs...");
    let result = graph::eq(&first_graph, &second_graph);
    if let Err(eq_error) = result {
        eprintln!("{}", eq_error);
        exit(1);
    }
    log::info!("Graphs are equal.");

    Ok(())
}
