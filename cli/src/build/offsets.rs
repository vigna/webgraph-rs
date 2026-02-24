/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "offsets", about = "Builds the offsets file of a graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => build_offsets::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => build_offsets::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_offsets<E: Endianness + 'static>(global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    // Creates the sequential iterator over the graph
    let seq_graph = BvGraphSeq::with_basename(&args.basename)
        .endianness::<E>()
        .load()?;
    let offsets = args.basename.with_extension(OFFSETS_EXTENSION);
    // create a bit writer on the file
    let mut writer = buf_bit_writer::from_path::<BE, usize>(&offsets)
        .with_context(|| format!("Could not create {}", offsets.display()))?;
    // progress bar
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(seq_graph.num_nodes()));
    if let Some(duration) = global_args.log_interval {
        pl.log_interval(duration);
    }
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
