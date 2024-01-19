/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::hint::black_box;

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use lender::prelude::*;
use webgraph::graph::arc_list_graph::{self, ArcListGraph};
use webgraph::utils::proj::Left;
use webgraph::{algorithms, prelude::*};
#[derive(Parser, Debug)]
#[command(about = "Benchmark direct transposition and labelled transposition on a unit graph.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

pub fn transpose(
    graph: &impl SequentialGraph,
    batch_size: usize,
) -> Result<
    Left<
        ArcListGraph<
            std::iter::Map<
                std::iter::Map<
                    KMergeIters<BatchIterator>,
                    fn((usize, usize, ())) -> (usize, usize),
                >,
                fn((usize, usize)) -> (usize, usize, ()),
            >,
        >,
    >,
> {
    let dir = tempfile::tempdir()?;
    let mut sorted = SortPairs::new(batch_size, dir.into_path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node")
        .expected_updates(Some(graph.num_nodes()));
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_! ( (src, succ) in graph.iter() {
        for dst in succ {
            sorted.push(dst, src)?;
        }
        pl.light_update();
    });
    // merge the batches
    let map: fn((usize, usize, ())) -> (usize, usize) = |(src, dst, _)| (src, dst);
    let sorted = arc_list_graph::ArcListGraph::new(graph.num_nodes(), sorted.iter()?.map(map));
    pl.done();

    Ok(Left(sorted))
}

fn bench_impl<E: Endianness + 'static>(args: Args) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>:
        ZetaRead<E> + DeltaRead<E> + GammaRead<E> + BitSeek,
{
    let graph = webgraph::graph::bvgraph::load(&args.basename)?;
    let unit = UnitLabelGraph(&graph);

    for _ in 0..10 {
        let mut pl = ProgressLogger::default();
        pl.start("Transposing standard graph...");

        let mut iter = transpose(&graph, 10_000_000)?.iter();
        while let Some((x, s)) = iter.next() {
            black_box(x);
            for i in s {
                black_box(i);
            }
        }
        pl.done_with_count(graph.num_nodes());

        pl.start("Transposing unit graph...");
        let mut iter = Left(algorithms::transpose_labelled(&unit, 10_000_000, (), ())?).iter();
        while let Some((x, s)) = iter.next() {
            black_box(x);
            for i in s {
                black_box(i);
            }
        }
        pl.done_with_count(unit.num_nodes());
    }

    Ok(())
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    match get_endianess(&args.basename)?.as_str() {
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
        _ => panic!("Unknown endianness"),
    }
}
