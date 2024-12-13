/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(clippy::type_complexity)]

use anyhow::{Context, Result};
use bitstream::Supply;
use clap::Parser;
use dsi_bitstream::codes::GammaRead;
use dsi_bitstream::impls::{BufBitReader, MemWordReader};
use dsi_bitstream::traits::{BitRead, BitSeek, Endianness, BE};
use dsi_progress_logger::prelude::*;
use epserde::deser::{DeserType, Deserialize, Flags, MemCase};
use lender::*;
use mmap_rs::MmapFlags;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use webgraph::prelude::bitstream::BitStreamLabeling;
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

pub struct MmapReaderSupplier<E: Endianness> {
    backend: MmapHelper<u32>,
    _marker: std::marker::PhantomData<E>,
}

impl Supply for MmapReaderSupplier<BE> {
    type Item<'a>
        = BufBitReader<BE, MemWordReader<u32, &'a [u32]>>
    where
        Self: 'a;

    fn request(&self) -> Self::Item<'_> {
        BufBitReader::<BE, _>::new(MemWordReader::new(self.backend.as_ref()))
    }
}

pub fn mmap<D>(
    path: impl AsRef<Path>,
    bit_deser: D,
) -> Result<BitStreamLabeling<BE, MmapReaderSupplier<BE>, D, MemCase<DeserType<'static, EF>>>>
where
    for<'a> D: BitDeserializer<BE, <MmapReaderSupplier<BE> as Supply>::Item<'a>>,
{
    let path = path.as_ref();
    let labels_path = path.with_extension("labels");
    let ef_path = path.with_extension("ef");
    Ok(BitStreamLabeling::new(
        MmapReaderSupplier {
            backend: MmapHelper::<u32>::mmap(&labels_path, MmapFlags::empty())
                .with_context(|| format!("Could not mmap {}", labels_path.display()))?,
            _marker: std::marker::PhantomData,
        },
        bit_deser,
        EF::mmap(&ef_path, Flags::empty())
            .with_context(|| format!("Could not parse {}", ef_path.display()))?,
    ))
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    let labels = mmap(&args.basename, SwhDeserializer::new(args.width))?;

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
