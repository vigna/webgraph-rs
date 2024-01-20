/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::BufReader;
use sux::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Thest that the '.ef' file (and `.offsets` if present) is coherent with the graph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let f = File::open(format!("{}.properties", args.basename)).with_context(|| {
        format!(
            "Could not load properties file: {}.properties",
            args.basename
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;

    // Create the offsets file
    let of_file_str = format!("{}.offsets", args.basename);
    let of_file_path = std::path::Path::new(&of_file_str);

    let ef = <webgraph::graph::bvgraph::EF<Vec<usize>, Vec<u64>>>::mmap(
        format!("{}.ef", args.basename),
        Flags::default(),
    )?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));

    // if the offset files exists, read it to build elias-fano
    if of_file_path.exists() {
        info!("The offsets file exists, reading it to build Elias-Fano");
        let of_file = BufReader::with_capacity(1 << 20, File::open(of_file_path)?);
        // create a bit reader on the file
        let mut reader = BufBitReader::<BE, _>::new(<WordAdapter<u32, _>>::new(of_file));
        // progress bar
        pl.start("Translating offsets to EliasFano...");
        // read the graph a write the offsets
        let mut offset = 0;
        for node_id in 0..num_nodes + 1 {
            // write where
            offset += reader.read_gamma()?;
            // read ef
            let ef_res = ef.get(node_id as _);
            assert_eq!(offset, ef_res as _, "node_id: {}", node_id);
            // decode the next nodes so we know where the next node_id starts
            pl.light_update();
        }
    }

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));

    info!("The offsets file does not exists, reading the graph to build Elias-Fano");
    let seq_graph = webgraph::graph::bvgraph::load_seq::<NE, _>(&args.basename)?;
    let seq_graph = seq_graph.map_codes_reader_builder(|x| x.to_skipper());
    // otherwise directly read the graph
    // progress bar
    pl.start("Building EliasFano...");
    // read the graph a write the offsets
    for (new_offset, node_id, _degree) in seq_graph.iter_degrees() {
        // decode the next nodes so we know where the next node_id starts
        // read ef
        let ef_res = ef.get(node_id as _);
        assert_eq!(new_offset, ef_res as _, "node_id: {}", node_id);
        pl.light_update();
    }
    pl.done();
    Ok(())
}
