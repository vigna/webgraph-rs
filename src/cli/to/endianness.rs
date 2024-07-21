/*
* SPDX-FileCopyrightText: 2023 Tommaso Fontana
*
* SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
*/

use crate::prelude::*;
use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use log::info;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "endianness";

#[derive(Args, Debug)]
#[command(about = "Invert the endianness of a BVGraph, this can be done using recompress but this is faster.", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    src_basename: PathBuf,
    /// The basename for the newly compressed graph.
    dst_basename: PathBuf,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

macro_rules! impl_convert {
    ($args:expr, $src:ty, $dst:ty) => {
        info!(
            "The source graph was {}-endian, converting to {}-endian",
            <$src>::NAME,
            <$dst>::NAME
        );

        let properties_path = $args.src_basename.with_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<$src>(&properties_path)?;
        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("node")
            .expected_updates(Some(num_arcs as usize));

        let seq_graph = BVGraphSeq::with_basename(&$args.src_basename)
            .endianness::<$src>()
            .load()
            .with_context(|| format!("Could not load graph {}", $args.src_basename.display()))?;
        // build the encoder with the opposite endianness
        std::fs::write(
            &properties_path,
            comp_flags.to_properties::<$dst>(num_nodes, num_arcs)?,
        )
        .with_context(|| {
            format!(
                "Could not write properties to {}",
                properties_path.display()
            )
        })?;
        let target_graph_path = $args.dst_basename.with_extension(GRAPH_EXTENSION);
        let writer = <BufBitWriter<$dst, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
            File::create(&target_graph_path)
                .with_context(|| format!("Could not create {}", target_graph_path.display()))?,
        )));
        let encoder = <DynCodesEncoder<$dst, _>>::new(writer, &comp_flags);
        // build the iterator that will read the graph and write it to the encoder

        pl.start("Inverting endianness...");

        let mut iter = seq_graph
            .offset_deg_iter()
            .map_decoder(move |decoder| Converter { decoder, encoder });
        // consume the graph iterator reading all codes, but do nothing with them
        for _ in 0..num_nodes {
            iter.next_degree()?;
            pl.light_update();
        }
        pl.done();
    };
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.src_basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => {
            impl_convert!(args, BE, LE);
        }
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => {
            impl_convert!(args, LE, BE);
        }
        e => panic!("Unknown endianness: {}", e),
    };

    Ok(())
}

/// A decoder that encodes the read values using the given encoder.
pub struct Converter<D: Decode, E: Encode> {
    decoder: D,
    encoder: E,
}

impl<D: Decode, E: Encode> Decode for Converter<D, E> {
    // TODO: implement correctly start_node/end_node
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        let res = self.decoder.read_outdegree();
        self.encoder.write_outdegree(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        let res = self.decoder.read_reference_offset();
        self.encoder.write_reference_offset(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        let res = self.decoder.read_block_count();
        self.encoder.write_block_count(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        let res = self.decoder.read_block();
        self.encoder.write_block(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        let res = self.decoder.read_interval_count();
        self.encoder.write_interval_count(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        let res = self.decoder.read_interval_start();
        self.encoder.write_interval_start(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        let res = self.decoder.read_interval_len();
        self.encoder.write_interval_len(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        let res = self.decoder.read_first_residual();
        self.encoder.write_first_residual(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        let res = self.decoder.read_residual();
        self.encoder.write_residual(res).unwrap();
        res
    }
}
