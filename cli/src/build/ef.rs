/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::LogIntervalArg;
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::path::PathBuf;
use sux::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug, Clone)]
#[command(name = "ef", about = "Builds the Elias–Fano representation of the offsets of a graph.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph or label files.​
    pub basename: PathBuf,
    /// The number of nodes. When specified, the .properties file is not needed,
    /// making it possible to build Elias–Fano for offsets of non-graph data.​
    pub number_of_nodes: Option<usize>,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    let ef_path = args.basename.with_extension(EF_EXTENSION);
    if ef_path.exists() && ef_path.metadata()?.permissions().readonly() {
        return Err(anyhow::anyhow!(
            "The file is not writable: {}",
            ef_path.display()
        ));
    }

    dispatch_endianness!(&args.basename, |E| build_elias_fano::<E>(args))
}

fn build_elias_fano<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let mut pl = progress_logger![
        display_memory = true,
        item_name = "node",
        log_interval = args.log_interval.log_interval
    ];

    let basename = &args.basename;
    let of_file_path = basename.with_extension(OFFSETS_EXTENSION);

    let bitstream_path = [GRAPH_EXTENSION, LABELS_EXTENSION]
        .map(|ext| basename.with_extension(ext))
        .into_iter()
        .find(|path| path.exists())
        .with_context(|| {
            format!(
                "Neither {} nor {} exist",
                basename.with_extension(GRAPH_EXTENSION).display(),
                basename.with_extension(LABELS_EXTENSION).display()
            )
        })?;
    info!("Bitstream: '{}'", bitstream_path.display());
    let file_len = 8 * std::fs::metadata(&bitstream_path)
        .with_context(|| {
            format!(
                "Could not read the metadata of {}",
                bitstream_path.display()
            )
        })?
        .len();
    info!("Bitstream file size: {} bits", file_len);

    let num_nodes = match args.number_of_nodes {
        Some(num_nodes) => num_nodes,
        None => {
            let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
            info!(
                "Reading num_nodes from properties file at '{}'",
                properties_path.display()
            );
            let f = File::open(&properties_path).with_context(|| {
                format!(
                    "Could not open properties file: {}",
                    properties_path.display()
                )
            })?;
            java_properties::read(std::io::BufReader::new(f))?
                .get("nodes")
                .with_context(|| format!("Missing 'nodes' in {}", properties_path.display()))?
                .parse()?
        }
    };
    pl.expected_updates(num_nodes);

    let ef = if of_file_path.exists() {
        info!(
            "Building Elias–Fano from offsets at '{}'",
            of_file_path.display()
        );
        pl.start("Building Elias–Fano from offsets...");
        let ef = build_ef(num_nodes, file_len, &of_file_path, &mut pl)?;
        pl.done();
        ef
    } else {
        info!("No offsets file, reading the graph to build Elias–Fano");
        let seq_graph = BvGraphSeq::with_basename(basename)
            .endianness::<E>()
            .load()
            .with_context(|| format!("Could not load graph at {}", basename.display()))?;
        pl.start("Building Elias–Fano from graph...");
        let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len);
        let mut iter = seq_graph.offset_deg_iter();
        for (offset, _degree) in iter.by_ref() {
            efb.push(offset as _);
            pl.light_update();
        }
        efb.push(iter.get_pos() as _);
        pl.done();
        info!("Building the index over the high bits...");
        ef_from_builder(efb)?
    };

    let ef_path = basename.with_extension(EF_EXTENSION);
    info!("Writing Elias–Fano to '{}'", ef_path.display());
    unsafe { ef.store(&ef_path) }
        .with_context(|| format!("Could not serialize Elias–Fano to {}", ef_path.display()))?;

    Ok(())
}
