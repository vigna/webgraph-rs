/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use webgraph::graphs::bvgraph::{EF, EF_EXTENSION, OFFSETS_EXTENSION, PROPERTIES_EXTENSION};
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use sux::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "ef", about = "Checks that the '.ef' file (and `.offsets` if present) is consistent with the graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    let properties_path = args.src.with_extension(PROPERTIES_EXTENSION);
    let f = File::open(&properties_path).with_context(|| {
        format!(
            "Could not load properties file: {}",
            properties_path.display()
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;

    // Creates the offsets file
    let of_file_path = args.src.with_extension(OFFSETS_EXTENSION);

    let ef = EF::mmap(args.src.with_extension(EF_EXTENSION), Flags::default())?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));
    if let Some(duration) = &global_args.log_interval {
        pl.log_interval(*duration);
    }

    // if the offset files exists, read it to build elias-fano
    if of_file_path.exists() {
        let of_file = BufReader::with_capacity(1 << 20, File::open(of_file_path)?);
        // create a bit reader on the file
        let mut reader = BufBitReader::<BE, _>::new(<WordAdapter<u32, _>>::new(of_file));
        // progress bar
        pl.start("Checking offsets file against Elias-Fano...");
        // read the graph a write the offsets
        let mut offset = 0;
        for node_id in 0..num_nodes + 1 {
            // write where
            offset += reader.read_gamma()?;
            // read ef
            let ef_res = ef.get(node_id as _);
            assert_eq!(offset, ef_res as u64, "node_id: {}", node_id);
            // decode the next nodes so we know where the next node_id starts
            pl.light_update();
        }
    } else {
        info!("No offsets file, checking against graph file only");
    }

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));
    if let Some(duration) = global_args.log_interval {
        pl.log_interval(duration);
    }

    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
        .endianness::<BE>()
        .load()?;
    // otherwise directly read the graph
    // progress bar
    pl.start("Checking graph against Elias-Fano...");
    // read the graph a write the offsets
    for (node, (new_offset, _degree)) in seq_graph.offset_deg_iter().enumerate() {
        // decode the next nodes so we know where the next node_id starts
        // read ef
        let ef_res = ef.get(node as _);
        assert_eq!(new_offset, ef_res as u64, "node_id: {}", node);
        pl.light_update();
    }
    pl.done();
    Ok(())
}
