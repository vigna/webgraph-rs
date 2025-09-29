/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use sux::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "dcf", about = "Builds the Elias–Fano representation of the degree cumulative function of a graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => build_dcf::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => build_dcf::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_dcf<E: Endianness + 'static>(global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let basename = args.src;
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

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, num_arcs);

    let ef_path = basename.with_extension(DEG_CUMUL_EXTENSION);
    let mut ef_file = BufWriter::new(
        File::create(&ef_path)
            .with_context(|| format!("Could not create {}", ef_path.display()))?,
    );

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));
    if let Some(duration) = global_args.log_interval {
        pl.log_interval(duration);
    }
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&basename)
        .endianness::<E>()
        .load()
        .with_context(|| format!("Could not load graph at {}", basename.display()))?;
    // otherwise directly read the graph
    // progress bar
    pl.start("Building the degree cumulative function...");
    // read the graph a write the offsets
    let mut iter = seq_graph.offset_deg_iter();
    let mut cumul_deg = 0;

    efb.push(0);
    for (_new_offset, degree) in iter.by_ref() {
        cumul_deg += degree;
        // write where
        efb.push(cumul_deg as _);
        // decode the next nodes so we know where the next node_id starts
        pl.light_update();
    }
    pl.done();

    let ef = efb.build();
    let ef: DCF = unsafe {
        ef.map_high_bits(|bits| {
            SelectZeroAdaptConst::<_, _, 12, 4>::new(SelectAdaptConst::<_, _, 12, 4>::new(bits))
        })
    };

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
