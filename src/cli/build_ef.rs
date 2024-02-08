/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use sux::prelude::*;
use webgraph::prelude::*;

pub const COMMAND_NAME: &str = "build_ef";

#[derive(Args, Debug)]
#[command(about = "Builds the .ef file for a graph.", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: String,
    /// The number of elements to be inserted in the Elias-Fano
    /// starting from a label offset file. It is usually one more than
    /// the number of nodes in the graph.
    n: Option<usize>,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => build_eliasfano::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => build_eliasfano::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn build_eliasfano<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    if let Some(num_nodes) = args.n {
        // Horribly temporary duplicated code for the case of label offsets.
        let of_file_str = format!("{}.labeloffsets", args.basename);
        let of_file_path = std::path::Path::new(&of_file_str);
        if of_file_path.exists() {
            let mut file = File::open(format!("{}.labels", args.basename))?;
            let file_len = 8 * file.seek(std::io::SeekFrom::End(0))?;

            let mut efb = EliasFanoBuilder::new(num_nodes, file_len as usize);

            info!("The offsets file exists, reading it to build Elias-Fano");
            let of_file = BufReader::with_capacity(1 << 20, File::open(of_file_path)?);
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
                offset += reader.read_gamma()?;
                efb.push(offset as _)?;
                // decode the next nodes so we know where the next node_id starts
                pl.light_update();
            }
            let ef = efb.build();

            let mut pl = ProgressLogger::default();
            pl.display_memory(true);
            pl.start("Building the Index over the ones in the high-bits...");
            let ef: EF = ef.convert_to().unwrap();
            pl.done();

            let mut pl = ProgressLogger::default();
            pl.display_memory(true);
            pl.start("Writing to disk...");
            // serialize and dump the schema to disk
            let mut ef_file = BufWriter::new(File::create(format!("{}.ef", args.basename))?);
            ef.serialize(&mut ef_file)?;
            pl.done();
            return Ok(());
        }
    }
    let f = File::open(format!("{}.properties", args.basename)).with_context(|| {
        format!(
            "Could not load properties file: {}.properties",
            args.basename
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;

    let mut file = File::open(format!("{}.graph", args.basename))?;
    let file_len = 8 * file.seek(std::io::SeekFrom::End(0))?;

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len as usize);

    let mut ef_file = BufWriter::new(File::create(format!("{}.ef", args.basename))?);

    // Create the offsets file
    let of_file_str = format!("{}.offsets", args.basename);
    let of_file_path = std::path::Path::new(&of_file_str);

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
        for _node_id in 0..num_nodes + 1 {
            // write where
            offset += reader.read_gamma()?;
            efb.push(offset as _)?;
            // decode the next nodes so we know where the next node_id starts
            pl.light_update();
        }
    } else {
        info!("The offsets file does not exists, reading the graph to build Elias-Fano");
        let seq_graph =
            webgraph::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.basename)
                .endianness::<E>()
                .load()?;

        // otherwise directly read the graph
        // progress bar
        pl.start("Building EliasFano...");
        // read the graph a write the offsets
        let mut iter = seq_graph.offset_deg_iter();
        for (new_offset, _degree) in iter.by_ref() {
            // write where
            efb.push(new_offset as _)?;
            // decode the next nodes so we know where the next node_id starts
            pl.light_update();
        }
        efb.push(iter.get_pos() as _)?;
    }
    pl.done();

    let ef = efb.build();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true);
    pl.start("Building the Index over the ones in the high-bits...");
    let ef: EF = ef.convert_to().unwrap();
    pl.done();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true);
    pl.start("Writing to disk...");
    // serialize and dump the schema to disk
    ef.serialize(&mut ef_file)?;

    pl.done();
    Ok(())
}
