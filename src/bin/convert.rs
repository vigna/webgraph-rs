/*
* SPDX-FileCopyrightText: 2023 Tommaso Fontana
*
* SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
*/

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use webgraph::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use dsi_progress_logger::*;

/// A decoder that encodes the read values using the given encoder.
pub struct Converter<D: Decoder, E: Encoder> {
    decoder: D,
    encoder: E,
}

impl<D: Decoder, E: Encoder> Decoder for Converter<D, E> {
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
 
#[derive(Parser, Debug)]
#[command(about = "Invert the endianness of a BVGraph, this can be done using recompress but this is faster.", long_about = None)]
struct Args {
    /// The basename of the graph.
    src_basename: String,
    /// The basename for the newly compressed graph.
    dst_basename: String,
}
macro_rules! impl_convert {
    ($args:expr, $src:ty, $dst:ty) => {
        println!("The source graph was {}, converting to {}", <$src>::NAME, <$dst>::NAME);

        let (num_nodes, num_arcs, comp_flags) = parse_properties::<$src>(format!("{}.properties", $args.src_basename))?;
        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("arcs")
            .expected_updates(Some(num_arcs as usize));
        pl.start("Converting graph");

        let seq_graph = BVGraphSeq::with_basename(&$args.src_basename)
                .endianness::<$src>()
                .load()?;
        // build the encoder with the opposite endianness
        std::fs::write(format!("{}.properties", $args.dst_basename), comp_flags.to_properties::<$dst>(num_nodes, num_arcs)?)?;
        let writer = <BufBitWriter<$dst, _>>::new(<WordAdapter<usize, _>>::new(
            BufWriter::new(File::create(&format!("{}.graph", $args.dst_basename))?),
        ));
        let encoder = <DynCodesEncoder<$dst, _>>::new(writer, &comp_flags);
        // build the iterator that will read the graph and write it to the encoder
        let mut iter = seq_graph.offset_deg_iter()
            .map_decoder(move |decoder| {
                Converter {
                    decoder,
                    encoder,
                }
        });
        // consume the graph iterator reading all codes, but do nothing with them
        for _ in 0..num_nodes {
            pl.update_with_count(iter.next_degree()?);
        }
        pl.done();
    };
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    match get_endianess(&args.src_basename)?.as_str() {
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
