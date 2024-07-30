/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use bitstream::{MmapReaderSupplier, Supply};
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
#[command(about = "Benchmarks a sequential scan of labels stored as a bitstream.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: PathBuf,
}

struct SwhDeserializer<BR> {
    width: usize,
    _marker: std::marker::PhantomData<BR>,
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>> BitDeserializer<BE, BR> for SwhDeserializer<BR> {
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

struct SwhDeserializerSupplier<BR> {
    width: usize,
    _marker: std::marker::PhantomData<BR>,
}

impl<BR> SwhDeserializerSupplier<BR> {
    pub fn new(width: usize) -> Self {
        Self {
            width,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<BR> Supply for SwhDeserializerSupplier<BR> {
    type Item<'a> = SwhDeserializer<BR> where BR: 'a;

    fn request(&self) -> Self::Item<'_> {
        SwhDeserializer {
            width: self.width,
            _marker: std::marker::PhantomData,
        }
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    let labels = BitStream::load_from_file(&args.basename, SwhDeserializerSupplier::new(7))?;

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
