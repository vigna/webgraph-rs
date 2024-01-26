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
use dsi_bitstream::traits::Endianness;
use dsi_progress_logger::*;
use rand::rngs::SmallRng;
use rand::RngCore;
use rand::SeedableRng;
use webgraph::prelude::*;
#[derive(Parser, Debug)]
#[command(about = "Tests the speed of SortPairs", long_about = None)]
struct Args {
    n: usize,
    batch: usize,
    #[arg(short = 'l', long)]
    labelled: bool,
}

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

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let dir = tempfile::tempdir()?;

    if args.labelled {
        let mut sp =
            SortPairs::<Mock, Mock>::new_labelled(args.batch, dir.into_path(), Mock(), Mock())?;

        let mut r = SmallRng::seed_from_u64(0);

        let mut pl = ProgressLogger::default();

        pl.start("Writing...");
        for _ in 0..args.n {
            sp.push_labelled(r.next_u64() as usize, r.next_u64() as usize, 0)?;
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
        return Ok(());
    } else {
        let mut sp = SortPairs::new(args.batch, dir.into_path())?;

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
