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

struct SwhDeserializer<BR, const WIDTH: usize> {
    _marker: std::marker::PhantomData<BR>,
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>, const WIDTH: usize> BitDeserializer<BE, BR>
    for SwhDeserializer<BR, WIDTH>
{
    type DeserType = Vec<u64>;

    fn deserialize(
        &self,
        bitstream: &mut BR,
    ) -> std::result::Result<Self::DeserType, <BR as BitRead<BE>>::Error> {
        let num_labels = bitstream.read_gamma().unwrap() as usize;
        let mut labels = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            labels.push(bitstream.read_bits(WIDTH)?);
        }
        Ok(labels)
    }
}

struct SwhDeserializerSupplier<BR, const WIDTH: usize> {
    _marker: std::marker::PhantomData<BR>,
}

impl<BR, const WIDTH: usize> SwhDeserializerSupplier<BR, WIDTH> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<BR, const WIDTH: usize> Supply for SwhDeserializerSupplier<BR, WIDTH> {
    type Item<'a> = SwhDeserializer<BR, WIDTH> where BR: 'a;

    fn request(&self) -> Self::Item<'_> {
        SwhDeserializer {
            _marker: std::marker::PhantomData,
        }
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    let labels = BitStream::load_from_file(&args.basename, SwhDeserializerSupplier::<_, 7>::new())?;

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
