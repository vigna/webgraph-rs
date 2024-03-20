/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use std::path::PathBuf;
use sux::prelude::*;

pub const COMMAND_NAME: &str = "deg_cef";

#[derive(Args, Debug)]
#[command(about = "Builds an Elias–Fano representation of the degree cumulative function of a graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,
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
        BE::NAME => build_deg_cef::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => build_deg_cef::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_deg_cef<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let basename = args.basename;
    let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
    let f = File::open(&properties_path).with_context(|| {
        format!(
            "Could not open properties file: {}",
            properties_path.display()
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;
    let num_arcs = map.get("arcs").unwrap().parse::<usize>()?;

    // TODO : not +1
    let mut efb = EliasFanoBuilder::new(num_nodes + 1, num_arcs + 1);

    let ef_path = basename.with_extension(DEG_CUMUL_EXTENSION);
    let mut ef_file = BufWriter::new(
        File::create(&ef_path)
            .with_context(|| format!("Could not create {}", ef_path.display()))?,
    );

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));
    info!("The offsets file does not exists, reading the graph.");
    let seq_graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&basename)
        .endianness::<E>()
        .load()
        .with_context(|| format!("Could not load graph at {}", basename.display()))?;
    // otherwise directly read the graph
    // progress bar
    pl.start("Building the degree cumulative function...");
    // read the graph a write the offsets
    let mut iter = seq_graph.offset_deg_iter();
    let mut cumul_deg = 0;

    efb.push(0)?;
    for (_new_offset, degree) in iter.by_ref() {
        cumul_deg += degree;
        // write where
        efb.push(cumul_deg as _).context("Could not write gamma")?;
        // decode the next nodes so we know where the next node_id starts
        pl.light_update();
    }
    pl.done();

    let ef = efb.build();
    let ef: DCF = ef.convert_to().unwrap();

    info!("Writing to disk...");

    ef.serialize(&mut ef_file).with_context(|| {
        format!(
            "Could not serialize degree cumulative list to {}",
            ef_path.display()
        )
    })?;

    info!("Completed.");

    Ok(())
}
