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
use lender::*;
use lender_derive::for_;
use webgraph::graph::arc_list_graph;
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
    arc_list_graph::ArcListGraph<
        std::iter::Map<KMergeIters<BatchIterator>, fn((usize, usize, ())) -> (usize, usize)>,
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

    Ok(sorted)
}
pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

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