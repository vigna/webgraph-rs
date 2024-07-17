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
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::PathBuf,
};

pub const COMMAND_NAME: &str = "offsets";

#[derive(Args, Debug)]
#[command(about = "Builds the .offsets file for a graph.", long_about = None)]

pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => build_offsets::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => build_offsets::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_offsets<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
    for<'a> BufBitReader<E, WordAdapter<u32, BufReader<File>>>: CodeRead<E> + BitSeek,
{
    // Create the sequential iterator over the graph
    let seq_graph = BVGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()?;
    let offsets = args.src.with_extension(OFFSETS_EXTENSION);
    let file = std::fs::File::create(&offsets)
        .with_context(|| format!("Could not create {}", offsets.display()))?;
    // create a bit writer on the file
    let mut writer = <BufBitWriter<BE, _>>::new(<WordAdapter<u64, _>>::new(
        BufWriter::with_capacity(1 << 20, file),
    ));
    // progress bar
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(seq_graph.num_nodes()));
    pl.start("Computing offsets...");
    // read the graph a write the offsets
    let mut offset = 0;
    let mut degs_iter = seq_graph.offset_deg_iter();
    for (new_offset, _degree) in &mut degs_iter {
        // write where
        writer
            .write_gamma((new_offset - offset) as _)
            .context("Could not write gamma")?;
        offset = new_offset;
        // decode the next nodes so we know where the next node_id starts
        pl.light_update();
    }
    // write the last offset, this is done to avoid decoding the last node
    writer
        .write_gamma((degs_iter.get_pos() - offset) as _)
        .context("Could not write final gamma")?;
    pl.light_update();
    pl.done();
    Ok(())
}
