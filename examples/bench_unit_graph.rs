/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

mod bench_sort_pairs;

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::codes::dispatch_factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::*;
use std::hint::black_box;
use std::path::PathBuf;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Breadth-first visits a graph.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: PathBuf,
}

fn bench_impl<E: Endianness>(args: Args) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let graph = BvGraph::with_basename(&args.basename)
        .endianness::<E>()
        .load()?;
    let unit = UnitLabelGraph(&graph);
    let labeled = Zip(
        BvGraph::with_basename(&args.basename)
            .endianness::<E>()
            .load()?,
        BvGraph::with_basename(&args.basename)
            .endianness::<E>()
            .load()?,
    );
    for _ in 0..10 {
        let mut pl = ProgressLogger::default();
        pl.start("Standard graph lender...");
        let mut iter = graph.iter();
        while let Some((x, s)) = iter.next() {
            black_box(x);
            for i in s {
                black_box(i);
            }
        }
        pl.done_with_count(graph.num_nodes());

        pl.start("Unit graph lender...");
        let mut iter = unit.iter();
        while let Some((x, s)) = iter.next() {
            black_box(x);
            for i in s {
                black_box(i);
            }
        }
        pl.done_with_count(unit.num_nodes());

        let mut pl = ProgressLogger::default();
        pl.start("Standard graph successors...");
        for x in 0..graph.num_nodes() {
            black_box(x);
            for i in graph.successors(x) {
                black_box(i);
            }
        }
        pl.done_with_count(graph.num_nodes());

        pl.start("Unit graph successors...");
        for x in 0..unit.num_nodes() {
            black_box(x);
            for i in unit.successors(x) {
                black_box(i);
            }
        }
        pl.done_with_count(unit.num_nodes());

        pl.start("Zipped-projected graph successors...");
        for x in 0..unit.num_nodes() {
            black_box(x);
            for (i, _) in labeled.successors(x) {
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
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => bench_impl::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => bench_impl::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}
