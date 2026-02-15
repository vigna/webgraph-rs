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
use mmap_rs::MmapFlags;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use std::path::{Path, PathBuf};
use sux::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug, Clone)]
#[command(name = "ef", about = "Builds the Elias-Fano representation of the offsets of a graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph (or labels).
    pub src: PathBuf,
    /// The number of nodes of the graph. When passed, we don't need to load the
    /// ".properties" file. This allows to build Elias-Fano from the offsets of
    /// something that might not be a graph but that has offsets, like labels.
    /// For this reason, if passed, we will also try to read the ".labeloffsets"
    /// file and then fallback to the usual ".offsets" file.
    pub number_of_nodes: Option<usize>,
}

/// Returns the length in bits of the given file.
fn file_len_bits(path: &Path) -> Result<usize> {
    let mut file =
        File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let len = 8 * file
        .seek(std::io::SeekFrom::End(0))
        .with_context(|| format!("Could not seek to end of {}", path.display()))?;
    Ok(len as usize)
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    let ef_path = args.src.with_extension(EF_EXTENSION);
    // check that ef_path is writable, this is the only portable way I found
    // to check that the file is writable.
    if ef_path.exists() && ef_path.metadata()?.permissions().readonly() {
        return Err(anyhow::anyhow!(
            "The file is not writable: {}",
            ef_path.display()
        ));
    }

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => build_elias_fano::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => build_elias_fano::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_elias_fano<E: Endianness + 'static>(
    global_args: GlobalArgs,
    args: CliArgs,
) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodesRead<E> + BitSeek,
{
    let mut pl = ProgressLogger::default();
    pl.display_memory(true).item_name("offset");
    if let Some(duration) = &global_args.log_interval {
        pl.log_interval(*duration);
    }

    let basename = args.src.clone();

    // When number_of_nodes is provided and label offsets exist, use them
    // instead of graph offsets.
    if let Some(num_nodes) = args.number_of_nodes {
        let label_offsets_path = basename.with_extension(LABELOFFSETS_EXTENSION);
        if label_offsets_path.exists() {
            let file_len = file_len_bits(&basename.with_extension(LABELS_EXTENSION))?;
            pl.expected_updates(Some(num_nodes));
            let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len);
            info!("The label offsets file exists, reading it to build Elias-Fano");
            let of = <MmapHelper<u32>>::mmap(label_offsets_path, MmapFlags::SEQUENTIAL)?;
            build_elias_fano_from_offsets(
                &global_args,
                &args,
                num_nodes,
                of.new_reader(),
                &mut pl,
                &mut efb,
            )?;
            return serialize_elias_fano(&global_args, &args, efb, &mut pl);
        }
    }

    // Standard graph case
    let of_file_path = basename.with_extension(OFFSETS_EXTENSION);

    let graph_path = basename.with_extension(GRAPH_EXTENSION);
    info!("Getting size of graph at '{}'", graph_path.display());
    let file_len = file_len_bits(&graph_path)?;
    info!("Graph file size: {} bits", file_len);

    // if the num_of_nodes is not present, read it from the properties file
    // otherwise use the provided value, this is so we can build the Elias-Fano
    // for offsets of any custom format that might not use the standard
    // properties file
    let num_nodes = args
        .number_of_nodes
        .map(Ok::<_, anyhow::Error>)
        .unwrap_or_else(|| {
            let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
            info!(
                "Reading num_of_nodes from properties file at '{}'",
                properties_path.display()
            );
            let f = File::open(&properties_path).with_context(|| {
                format!(
                    "Could not open properties file: {}",
                    properties_path.display()
                )
            })?;
            let map = java_properties::read(BufReader::new(f))?;
            Ok(map.get("nodes").unwrap().parse::<usize>()?)
        })?;
    pl.expected_updates(Some(num_nodes));

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len);

    info!("Checking if offsets exists at '{}'", of_file_path.display());
    // if the offset files exists, read it to build elias-fano
    if of_file_path.exists() {
        info!("The offsets file exists, reading it to build Elias-Fano");
        let of = <MmapHelper<u32>>::mmap(of_file_path, MmapFlags::SEQUENTIAL)?;
        build_elias_fano_from_offsets(
            &global_args,
            &args,
            num_nodes,
            of.new_reader(),
            &mut pl,
            &mut efb,
        )?;
    } else {
        build_elias_fano_from_graph(&args, &mut pl, &mut efb)?;
    }

    serialize_elias_fano(&global_args, &args, efb, &mut pl)
}

pub fn build_elias_fano_from_graph(
    args: &CliArgs,
    pl: &mut impl ProgressLog,
    efb: &mut EliasFanoBuilder,
) -> Result<()> {
    info!("The offsets file does not exists, reading the graph to build Elias-Fano");
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => build_elias_fano_from_graph_with_endianness::<BE>(args, pl, efb),
        #[cfg(feature = "le_bins")]
        LE::NAME => build_elias_fano_from_graph_with_endianness::<LE>(args, pl, efb),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_elias_fano_from_offsets<E: Endianness>(
    _global_args: &GlobalArgs,
    _args: &CliArgs,
    num_nodes: usize,
    mut reader: impl GammaRead<E>,
    pl: &mut impl ProgressLog,
    efb: &mut EliasFanoBuilder,
) -> Result<()> {
    info!("Building Elias-Fano from offsets...");

    // progress bar
    pl.start("Translating offsets to EliasFano...");
    // read the graph a write the offsets
    let mut offset = 0;
    for _node_id in 0..num_nodes + 1 {
        // write where
        offset += reader.read_gamma().context("Could not read gamma")?;
        efb.push(offset as _);
        // decode the next nodes so we know where the next node_id starts
        pl.light_update();
    }
    pl.done();
    Ok(())
}

pub fn build_elias_fano_from_graph_with_endianness<E: Endianness>(
    args: &CliArgs,
    pl: &mut impl ProgressLog,
    efb: &mut EliasFanoBuilder,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()
        .with_context(|| format!("Could not load graph at {}", args.src.display()))?;
    // otherwise directly read the graph
    // progress bar
    pl.start("Building EliasFano...");
    // read the graph a write the offsets
    let mut iter = seq_graph.offset_deg_iter();
    for (new_offset, _degree) in iter.by_ref() {
        // write where
        efb.push(new_offset as _);
        // decode the next nodes so we know where the next node_id starts
        pl.light_update();
    }
    efb.push(iter.get_pos() as _);
    Ok(())
}

pub fn serialize_elias_fano(
    global_args: &GlobalArgs,
    args: &CliArgs,
    efb: EliasFanoBuilder,
    pl: &mut impl ProgressLog,
) -> Result<()> {
    let ef = efb.build();
    pl.done();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true);
    if let Some(duration) = &global_args.log_interval {
        pl.log_interval(*duration);
    }
    pl.start("Building the Index over the ones in the high-bits...");
    let ef: EF = unsafe { ef.map_high_bits(SelectAdaptConst::<_, _, 12, 4>::new) };
    pl.done();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true);
    if let Some(duration) = &global_args.log_interval {
        pl.log_interval(*duration);
    }
    pl.start("Writing to disk...");

    let ef_path = args.src.with_extension(EF_EXTENSION);
    info!("Creating Elias-Fano at '{}'", ef_path.display());
    let mut ef_file = BufWriter::new(
        File::create(&ef_path)
            .with_context(|| format!("Could not create {}", ef_path.display()))?,
    );

    // serialize and dump the schema to disk
    unsafe {
        ef.serialize(&mut ef_file)
            .with_context(|| format!("Could not serialize EliasFano to {}", ef_path.display()))
    }?;

    pl.done();
    Ok(())
}
