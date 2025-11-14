/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::hint::black_box;

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::traits::BitRead;
use dsi_bitstream::traits::BitWrite;
use dsi_bitstream::traits::{Endianness, BE};
use dsi_progress_logger::prelude::{ProgressLog, ProgressLogger};
use rand::rngs::SmallRng;
use rand::RngCore;
use rand::SeedableRng;
use tempfile::Builder;
use webgraph::prelude::*;
use webgraph::utils::gaps::GapsCodec;

#[derive(Parser, Debug)]
#[command(about = "Tests the merge speed of SortPairs", long_about = None)]
struct Args {
    n: usize,
    batch: usize,
    /// Use 128-bit labels that are neither read nor written.
    #[arg(short = 'l', long)]
    labeled: bool,
}

/// No-op serializer/deserializer (as we want to check the merge speed)
#[derive(Debug, Clone)]
struct Mock();
impl<E: Endianness, W: BitWrite<E>> BitSerializer<E, W> for Mock {
    type SerType = u128;

    fn serialize(
        &self,
        _value: &Self::SerType,
        _bitstream: &mut W,
    ) -> Result<usize, <W as BitWrite<E>>::Error> {
        Ok(0)
    }
}
impl<E: Endianness, W: BitRead<E>> BitDeserializer<E, W> for Mock {
    type DeserType = u128;

    fn deserialize(&self, _bitstream: &mut W) -> Result<Self::DeserType, <W as BitRead<E>>::Error> {
        Ok(0)
    }
}

#[allow(dead_code)] // I have no idea why this happens https://github.com/rust-lang/rust/issues/12327
pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    let dir = Builder::new().prefix("bench_sort_pairs").tempdir()?;

    if args.labeled {
        let mut sp = SortPairs::new_labeled(
            MemoryUsage::BatchSize(args.batch),
            dir.path(),
            GapsCodec::<BE, _, _>::new(Mock(), Mock()),
        )?;

        let mut r = SmallRng::seed_from_u64(0);

        let mut pl = ProgressLogger::default();

        pl.start("Writing...");
        for _ in 0..args.n {
            sp.push_labeled(r.next_u64() as usize, r.next_u64() as usize, 0)?;
            pl.light_update();
        }
        pl.done();

        let mut iter = sp.iter()?;

        pl.start("Reading...");
        for _ in 0..args.n {
            black_box(iter.next().unwrap());
            pl.light_update();
        }
        pl.done();
    } else {
        let mut sp = SortPairs::new(MemoryUsage::BatchSize(args.batch), dir.path())?;

        let mut r = SmallRng::seed_from_u64(0);

        let mut pl = ProgressLogger::default();

        pl.start("Writing...");
        for _ in 0..args.n {
            sp.push(r.next_u64() as usize, r.next_u64() as usize)?;
            pl.light_update();
        }
        pl.done();

        let mut iter = sp.iter()?;

        pl.start("Reading...");
        for _ in 0..args.n {
            black_box(iter.next().unwrap());
            pl.light_update();
        }
        pl.done();
    }
    Ok(())
}
