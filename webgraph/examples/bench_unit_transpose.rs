/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(clippy::type_complexity)]
use std::hint::black_box;

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use std::path::PathBuf;
use webgraph::{prelude::*, transform};

#[derive(Parser, Debug)]
#[command(about = "Benchmark direct transposition and labeled transposition on a unit graph.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: PathBuf,
}

fn bench_impl<E: Endianness>(args: Args) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(args.basename)
        .endianness::<E>()
        .load()?;

    let unit = UnitLabelGraph(&graph);

    for _ in 0..10 {
        let mut pl = ProgressLogger::default();
        pl.start("Transposing standard graph...");

        let transposed = transform::transpose(&graph, MemoryUsage::BatchSize(10_000_000))?;
        let mut iter = transposed.iter();
        while let Some((x, s)) = iter.next() {
            black_box(x);
            for i in s {
                black_box(i);
            }
        }
        pl.done_with_count(graph.num_nodes());

        pl.start("Transposing unit graph...");
        let transposed = transform::transpose_labeled(
            &unit,
            MemoryUsage::BatchSize(10_000_000),
            (),
            (),
        )?;
        let mut iter = transposed.iter();
        while let Some((x, s)) = iter.next() {
            black_box(x);
            for (i, _label) in s {
                black_box(i);
            }
        }
        pl.done_with_count(unit.num_nodes());
    }

    Ok(())
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    match get_endianness(&args.basename)?.as_str() {
        BE::NAME => bench_impl::<BE>(args),
        LE::NAME => bench_impl::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}
