/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::LogIntervalArg;
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use log::info;
use std::path::PathBuf;
use value_traits::slices::SliceByValue;
use webgraph::graphs::bvgraph::{EF, EF_EXTENSION, OFFSETS_EXTENSION, PROPERTIES_EXTENSION};
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "ef", about = "Checks that the \".ef\" file (and \".offsets\" if present) is consistent with the graph.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    dispatch_endianness!(&args.basename, |E| check_ef::<E>(args))
}

pub fn check_ef<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let (num_nodes, _, _) =
        parse_properties::<E>(args.basename.with_extension(PROPERTIES_EXTENSION))?;

    // Creates the offsets file
    let of_file_path = args.basename.with_extension(OFFSETS_EXTENSION);

    let ef = unsafe { EF::mmap(args.basename.with_extension(EF_EXTENSION), Flags::default()) }?;
    let ef = ef.uncase();

    let mut pl = progress_logger![
        display_memory = true,
        item_name = "offset",
        expected_updates = Some(num_nodes),
        log_interval = args.log_interval.log_interval,
    ];

    // if the offset files exists, read it to build elias-fano
    if of_file_path.exists() {
        // create a bit reader on the file
        let mut reader = buf_bit_reader::from_path::<BE, u32>(of_file_path)?;
        // progress bar
        pl.start("Checking offsets file against Elias–Fano...");
        // read the graph a write the offsets
        let mut offset = 0;
        for node_id in 0..num_nodes + 1 {
            // write where
            offset += reader.read_gamma()?;
            // read ef
            let ef_res = ef.index_value(node_id as _);
            assert_eq!(offset, ef_res, "node_id: {}", node_id);
            // decode the next nodes so we know where the next node_id starts
            pl.light_update();
        }
    } else {
        info!("No offsets file, checking against graph file only");
    }

    let mut pl = progress_logger![
        display_memory = true,
        item_name = "offset",
        expected_updates = Some(num_nodes),
        log_interval = args.log_interval.log_interval,
    ];

    let seq_graph =
        webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.basename)
            .endianness::<E>()
            .load()?;
    // otherwise directly read the graph
    // progress bar
    pl.start("Checking graph against Elias–Fano...");
    // read the graph a write the offsets
    let mut degs_iter = seq_graph.offset_deg_iter();
    for (node, (new_offset, _degree)) in (&mut degs_iter).enumerate() {
        // decode the next nodes so we know where the next node_id starts
        // read ef
        let ef_res = ef.index_value(node as _);
        assert_eq!(new_offset, ef_res, "node_id: {}", node);
        pl.light_update();
    }
    // The final entry is the total length of the graph bitstream.
    let ef_res = ef.index_value(num_nodes as _);
    assert_eq!(
        degs_iter.get_pos(),
        ef_res,
        "final offset (node_id: {num_nodes})"
    );
    pl.done();
    Ok(())
}
