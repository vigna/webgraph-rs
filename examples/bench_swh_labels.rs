/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::codes::GammaRead;
use dsi_bitstream::traits::{BitRead, BitSeek, BE};
use dsi_progress_logger::prelude::*;
use lender::*;
use std::hint::black_box;
use std::path::PathBuf;
use webgraph::prelude::bitstream::BitStream;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Benchmarks a sequential scan of labels stored as a bitstream in SWH format.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: PathBuf,
    /// The width label elements. No check is performed on the actual width of
    /// the label elements.
    width: usize,
}

/// A [`BitDeserializer`] for the labels stored in the bitstream.
///
/// Labels are deserialized as a sequence of `u64` values, each of which is
/// `width` bits wide. The length of the sequence is read using a [γ
/// code](GammaRead), and then each value is obtained by reading `width` bits.
struct SwhDeserializer {
    width: usize,
}

impl SwhDeserializer {
    /// Creates a new [`SwhDeserializer`] with the given width.
    pub fn new(width: usize) -> Self {
        Self { width }
    }
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>> BitDeserializer<BE, BR> for SwhDeserializer {
    type DeserType = Vec<u64>;

    fn deserialize(
        &self,
        bitstream: &mut BR,
    ) -> std::result::Result<Self::DeserType, <BR as BitRead<BE>>::Error> {
        let num_labels = bitstream.read_gamma().unwrap() as usize;
        let mut labels = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            labels.push(bitstream.read_bits(self.width)?);
        }
        Ok(labels)
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    let labels = BitStream::mmap(&args.basename, SwhDeserializer::new(args.width))?;

    for _ in 0..10 {
        let mut pl = ProgressLogger::default();
        pl.start("Standard graph lender...");
        let mut iter = labels.iter();
        while let Some((x, s)) = iter.next() {
            black_box(x);
            for i in s {
                black_box(i);
            }
        }
        pl.done_with_count(labels.num_nodes());
    }

    Ok(())
}
