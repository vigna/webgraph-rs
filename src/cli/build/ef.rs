/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use std::path::PathBuf;
use sux::prelude::*;

pub const COMMAND_NAME: &str = "ef";

#[derive(Args, Debug, Clone)]
#[command(about = "Builds the Elias-Fano representation of the offsets of a graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
    /// The number of elements to be inserted in the Elias-Fano
    /// starting from a label offset file. It is usually one more than
    /// the number of nodes in the graph.
    pub n: Option<usize>,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    let basename = &args.src;

    if let Some(num_nodes) = args.n {
        // Horribly temporary duplicated code for the case of label offsets.
        let of_file_path = basename.with_extension(LABELOFFSETS_EXTENSION);
        if of_file_path.exists() {
            let labels_path = basename.with_extension(LABELS_EXTENSION);
            let mut file = File::open(&labels_path)
                .with_context(|| format!("Could not open {}", labels_path.display()))?;
            let file_len = 8 * file
                .seek(std::io::SeekFrom::End(0))
                .with_context(|| format!("Could not seek to end of {}", labels_path.display()))?;

            let mut efb = EliasFanoBuilder::new(num_nodes, file_len as usize);

            info!("The offsets file exists, reading it to build Elias-Fano");
            let of_file = BufReader::with_capacity(
                1 << 20,
                File::open(&of_file_path)
                    .with_context(|| format!("Could not open {}", of_file_path.display()))?,
            );
            // create a bit reader on the file
            let mut reader = BufBitReader::<BE, _>::new(<WordAdapter<u32, _>>::new(of_file));
            // progress bar
            let mut pl = ProgressLogger::default();
            pl.display_memory(true)
                .item_name("offset")
                .expected_updates(Some(num_nodes));
            pl.start("Translating offsets to EliasFano...");
            // read the graph a write the offsets
            let mut offset = 0;
            for _node_id in 0..num_nodes {
                // write where
                offset += reader.read_gamma().context("Could not read gamma")?;
                efb.push(offset as _);
                // decode the next nodes so we know where the next node_id starts
                pl.light_update();
            }
            let ef = efb.build();

            let mut pl = ProgressLogger::default();
            pl.display_memory(true);
            pl.start("Building the Index over the ones in the high-bits...");
            let ef: EF = unsafe { ef.map_high_bits(SelectAdaptConst::<_, _, 12, 4>::new) };
            pl.done();

            let mut pl = ProgressLogger::default();
            pl.display_memory(true);
            pl.start("Writing to disk...");
            // serialize and dump the schema to disk
            let ef_path = basename.with_extension(EF_EXTENSION);
            let mut ef_file = BufWriter::new(
                File::create(&ef_path)
                    .with_context(|| format!("Could not create {}", ef_path.display()))?,
            );
            ef.serialize(&mut ef_file)
                .with_context(|| format!("Could not serialize EF to {}", ef_path.display()))?;
            pl.done();
            return Ok(());
        }
    }

    // Create the offsets file
    let of_file_path = basename.with_extension(OFFSETS_EXTENSION);

    let ef_path = basename.with_extension(EF_EXTENSION);
    info!("Creating Elias-Fano at '{}'", ef_path.display());
    let mut ef_file = BufWriter::new(
        File::create(&ef_path)
            .with_context(|| format!("Could not create {}", ef_path.display()))?,
    );

    let graph_path = basename.with_extension(GRAPH_EXTENSION);
    info!("Getting size of graph at '{}'", graph_path.display());
    let mut file = File::open(&graph_path)
        .with_context(|| format!("Could not open {}", graph_path.display()))?;
    let file_len = 8 * file
        .seek(std::io::SeekFrom::End(0))
        .with_context(|| format!("Could not seek in {}", graph_path.display()))?;
    info!("Graph file size: {} bits", file_len);

    // if the num_of_nodes is not present, read it from the properties file
    // otherwise use the provided value, this is so we can build the Elias-Fano
    // for offsets of any custom format that might not use the standard
    // properties file
    let num_nodes = args.n.map(Ok::<_, anyhow::Error>).unwrap_or_else(|| {
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
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len as usize);

    info!("Checking if offsets exists at '{}'", of_file_path.display());
    // if the offset files exists, read it to build elias-fano
    if of_file_path.exists() {
        info!("The offsets file exists, reading it to build Elias-Fano");
        let of_file = BufReader::with_capacity(
            1 << 20,
            File::open(&of_file_path)
                .with_context(|| format!("Could not open {}", of_file_path.display()))?,
        );
        // create a bit reader on the file
        let mut reader = BufBitReader::<BE, _>::new(<WordAdapter<u32, _>>::new(of_file));
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
    } else {
        build_eliasfano_from_graph(&args, &mut pl, &mut efb)?;
    }
    let ef = efb.build();
    pl.done();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true);
    pl.start("Building the Index over the ones in the high-bits...");
    let ef: EF = unsafe { ef.map_high_bits(SelectAdaptConst::<_, _, 12, 4>::new) };
    pl.done();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true);
    pl.start("Writing to disk...");
    // serialize and dump the schema to disk
    ef.serialize(&mut ef_file)
        .with_context(|| format!("Could not serialize EliasFano to {}", ef_path.display()))?;

    pl.done();
    Ok(())
}

pub fn build_eliasfano_from_graph(
    args: &CliArgs,
    pl: &mut impl ProgressLog,
    efb: &mut EliasFanoBuilder,
) -> Result<()> {
    info!("The offsets file does not exists, reading the graph to build Elias-Fano");
    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => build_eliasfano::<BE>(args, pl, efb),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => build_eliasfano::<LE>(args, pl, efb),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_eliasfano<E: Endianness>(
    args: &CliArgs,
    pl: &mut impl ProgressLog,
    efb: &mut EliasFanoBuilder,
) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let seq_graph = crate::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
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
