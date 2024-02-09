/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use std::path::PathBuf;
use sux::prelude::*;

pub const COMMAND_NAME: &str = "ef";

#[derive(Args, Debug)]
#[command(about = "Builds the .ef file for a graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,
    /// The number of elements to be inserted in the Elias-Fano
    /// starting from a label offset file. It is usually one more than
    /// the number of nodes in the graph.
    pub n: Option<usize>,
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

pub fn build_eliasfano<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    if let Some(num_nodes) = args.n {
        // Horribly temporary duplicated code for the case of label offsets.
        let of_file_path = suffix_path(&args.basename, ".labeloffsets");
        if of_file_path.exists() {
            let labels_path = suffix_path(&args.basename, ".labels");
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
                efb.push(offset as _).context("Could not write offset")?;
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
            let ef_path = suffix_path(&args.basename, ".ef");
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

    let properties_path = suffix_path(&args.basename, ".properties");
    let f = File::open(&properties_path).with_context(|| {
        format!(
            "Could not open properties file: {}",
            properties_path.display()
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;

    let graph_path = suffix_path(&args.basename, ".graph");
    let mut file = File::open(&graph_path)
        .with_context(|| format!("Could not open {}", graph_path.display()))?;
    let file_len = 8 * file
        .seek(std::io::SeekFrom::End(0))
        .with_context(|| format!("Could not seek in {}", graph_path.display()))?;

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len as usize);

    let ef_path = suffix_path(&args.basename, ".ef");
    let mut ef_file = BufWriter::new(
        File::create(&ef_path)
            .with_context(|| format!("Could not create {}", ef_path.display()))?,
    );

    // Create the offsets file
    let of_file_path = suffix_path(&args.basename, ".offsets");

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("offset")
        .expected_updates(Some(num_nodes));

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
            efb.push(offset as _).context("Could not write gamma")?;
            // decode the next nodes so we know where the next node_id starts
            pl.light_update();
        }
    } else {
        info!("The offsets file does not exists, reading the graph to build Elias-Fano");
        let seq_graph =
            crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.basename)
                .endianness::<E>()
                .load()
                .with_context(|| format!("Could not load graph at {}", args.basename.display()))?;
        // otherwise directly read the graph
        // progress bar
        pl.start("Building EliasFano...");
        // read the graph a write the offsets
        let mut iter = seq_graph.offset_deg_iter();
        for (new_offset, _degree) in iter.by_ref() {
            // write where
            efb.push(new_offset as _).context("Could not write gamma")?;
            // decode the next nodes so we know where the next node_id starts
            pl.light_update();
        }
        efb.push(iter.get_pos() as _)
            .context("Could not write final gamma")?;
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
    ef.serialize(&mut ef_file)
        .with_context(|| format!("Could not serialize EliasFano to {}", ef_path.display()))?;

    pl.done();
    Ok(())
}
