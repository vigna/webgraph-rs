/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use dsi_progress_logger::*;
use lender::*;
use std::hint::black_box;
use webgraph::prelude::*;
#[derive(Parser, Debug)]
#[command(about = "Breadth-first visits a graph.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
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
    let labelled = Zip(
        webgraph::graph::bvgraph::load(&args.basename)?,
        webgraph::graph::bvgraph::load(&args.basename)?,
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
            for i in unit.labels(x) {
                black_box(i);
            }
        }
        pl.done_with_count(unit.num_nodes());

        pl.start("Zipped-projected graph successors...");
        for x in 0..unit.num_nodes() {
            black_box(x);
            for (i, _) in labelled.labels(x) {
                black_box(i);
            }
        }
        pl.done_with_count(unit.num_nodes());
    }

    Ok(())
}
