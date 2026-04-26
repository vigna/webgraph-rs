/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
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
use std::io::{BufWriter, Seek};
use std::path::{Path, PathBuf};
use sux::prelude::*;
use sux::traits::TryIntoUnaligned;
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

/// Returns the length in bits of the given file.
fn file_len_bits(path: &Path) -> Result<u64> {
    let mut file =
        File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let len = 8 * file
        .seek(std::io::SeekFrom::End(0))
        .with_context(|| format!("Could not seek to end of {}", path.display()))?;
    Ok(len)
}

pub fn main(args: CliArgs) -> Result<()> {
    let ef_path = args.basename.with_extension(EF_EXTENSION);
    if ef_path.exists() && ef_path.metadata()?.permissions().readonly() {
        return Err(anyhow::anyhow!(
            "The file is not writable: {}",
            ef_path.display()
        ));
    }

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => build_elias_fano::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => build_elias_fano::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn build_elias_fano<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let mut pl = progress_logger![display_memory = true, item_name = "node"];
    if let Some(duration) = args.log_interval.log_interval {
        pl.log_interval(duration);
    }

    let basename = &args.basename;
    let of_file_path = basename.with_extension(OFFSETS_EXTENSION);

    let graph_path = basename.with_extension(GRAPH_EXTENSION);
    let labels_file_path = basename.with_extension(LABELS_EXTENSION);
    let file_len = if graph_path.exists() {
        info!("Bitstream: '{}'", graph_path.display());
        file_len_bits(&graph_path)?
    } else if labels_file_path.exists() {
        info!("Bitstream: '{}'", labels_file_path.display());
        file_len_bits(&labels_file_path)?
    } else {
        anyhow::bail!(
            "Neither {} nor {} exist",
            graph_path.display(),
            labels_file_path.display()
        );
    };
    info!("Bitstream file size: {} bits", file_len);

    let num_nodes = args
        .number_of_nodes
        .map(Ok::<_, anyhow::Error>)
        .unwrap_or_else(|| {
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
            let map = java_properties::read(std::io::BufReader::new(f))?;
            Ok(map.get("nodes").unwrap().parse::<usize>()?)
        })?;
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
        let ef = efb.build();
        unsafe {
            ef.map_high_bits(
                SelectAdaptConst::<_, _, LOG2_ONES_PER_INVENTORY, LOG2_WORDS_PER_SUBINVENTORY>::new,
            )
            .try_into_unaligned()?
        }
    };

    let ef_path = basename.with_extension(EF_EXTENSION);
    info!("Writing Elias–Fano to '{}'", ef_path.display());
    let mut ef_file = BufWriter::new(
        File::create(&ef_path)
            .with_context(|| format!("Could not create {}", ef_path.display()))?,
    );

    unsafe {
        ef.serialize(&mut ef_file)
            .with_context(|| format!("Could not serialize Elias–Fano to {}", ef_path.display()))
    }?;

    Ok(())
}
