/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::hint::black_box;

use anyhow::Result;
use clap::Parser;
use dsi_progress_logger::*;
use rand::rngs::SmallRng;
use rand::RngCore;
use rand::SeedableRng;
use webgraph::prelude::*;
#[derive(Parser, Debug)]
#[command(about = "Breadth-first visits a graph.", long_about = None)]
struct Args {
    n: usize,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let dir = tempfile::tempdir()?;

    let mut sp = SortPairs::new(1_000_000, dir.into_path())?;

    let mut r = SmallRng::seed_from_u64(0);

    for _ in 0..args.n {
        sp.push(r.next_u64() as usize, r.next_u64() as usize)?;
    }
    let mut iter = sp.iter()?;

    let mut pl = ProgressLogger::default();
    pl.start("Reading...");
    for _ in 0..args.n {
        black_box(iter.next().unwrap());
        pl.light_update();
    }
    pl.done();

    Ok(())
}
