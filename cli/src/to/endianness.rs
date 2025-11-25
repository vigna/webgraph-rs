/*
* SPDX-FileCopyrightText: 2023 Tommaso Fontana
*
* SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
*/

use crate::{GlobalArgs, create_parent_dir};
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use log::info;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "endianness", about = "Inverts the endianness of a BvGraph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the source graph.
    pub src: PathBuf,
    /// The basename of the destination graph.
    pub dst: PathBuf,
}

macro_rules! impl_convert {
    ($global_args:expr, $args:expr, $src:ty, $dst:ty) => {
        info!(
            "The source graph was {}-endian, converting to {}-endian",
            <$src>::NAME,
            <$dst>::NAME
        );

        let properties_path = $args.src.with_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<$src>(&properties_path)?;
        // also extract the bitstream length
        let f = std::fs::File::open(&properties_path)
            .with_context(|| format!("Cannot open property file {}", &properties_path.display()))?;
        let map = java_properties::read(std::io::BufReader::new(f)).with_context(|| {
            format!(
                "cannot parse {} as a java properties file",
                &properties_path.display()
            )
        })?;
        let bitstream_len = map
            .get("length")
            .with_context(|| format!("Missing 'arcs' property in {}", &properties_path.display()))?
            .parse::<u64>()
            .with_context(|| {
                format!(
                    "Cannot parse arcs as usize in {}",
                    &properties_path.display()
                )
            })?;

        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("node")
            .expected_updates(Some(num_arcs as usize));

        if let Some(duration) = $global_args.log_interval {
            pl.log_interval(duration);
        }

        let seq_graph = BvGraphSeq::with_basename(&$args.src)
            .endianness::<$src>()
            .load()
            .with_context(|| format!("Could not load graph {}", $args.src.display()))?;
        // build the encoder with the opposite endianness
        std::fs::write(
            &properties_path,
            comp_flags.to_properties::<$dst>(num_nodes, num_arcs, bitstream_len)?,
        )
        .with_context(|| {
            format!(
                "Could not write properties to {}",
                properties_path.display()
            )
        })?;
        let target_graph_path = $args.dst.with_extension(GRAPH_EXTENSION);
        let writer = <BufBitWriter<$dst, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
            File::create(&target_graph_path)
                .with_context(|| format!("Could not create {}", target_graph_path.display()))?,
        )));
        let encoder = <DynCodesEncoder<$dst, _>>::new(writer, &comp_flags)?;
        // build the iterator that will read the graph and write it to the encoder

        let offsets_path = $args.dst.with_extension(OFFSETS_EXTENSION);
        let mut offsets_writer =
            <BufBitWriter<BE, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
                File::create(&offsets_path)
                    .with_context(|| format!("Could not create {}", offsets_path.display()))?,
            )));

        pl.start("Inverting endianness...");

        let mut iter = seq_graph
            .offset_deg_iter()
            .map_decoder(move |decoder| Converter {
                decoder,
                encoder,
                offset: 0,
            });

        let mut offset = 0;
        for _ in 0..num_nodes {
            iter.next_degree()?;
            let new_offset = iter.get_decoder().offset;
            offsets_writer
                .write_gamma((new_offset - offset) as u64)
                .context("Could not write gamma")?;
            offset = new_offset;
            pl.light_update();
        }
        let new_offset = iter.get_decoder().offset;
        offsets_writer
            .write_gamma((new_offset - offset) as u64)
            .context("Could not write gamma")?;
        pl.light_update();
        pl.done();
        offsets_writer.flush().context("Could not flush offsets")?;
    };
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => {
            impl_convert!(global_args, args, BE, LE);
        }
        #[cfg(feature = "le_bins")]
        LE::NAME => {
            impl_convert!(global_args, args, LE, BE);
        }
        e => panic!("Unknown endianness: {}", e),
    };

    Ok(())
}
