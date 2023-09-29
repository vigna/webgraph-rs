/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use bitvec::*;
use clap::Parser;
use dsi_progress_logger::ProgressLogger;
use std::collections::VecDeque;
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
    let num_nodes = graph.num_nodes();
    let mut visited = bitvec![0; num_nodes];
    let mut queue = VecDeque::new();

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("Visiting graph...");

    for start in 0..num_nodes {
        pl.update();
        if visited[start] {
            continue;
        }
        queue.push_back(start as _);
        visited.set(start, true);

        while !queue.is_empty() {
            let current_node = queue.pop_front().unwrap();
            for succ in graph.successors(current_node) {
                if !visited[succ] {
                    queue.push_back(succ);
                    visited.set(succ as _, true);
                }
            }
        }
    }

    pl.done();

    Ok(())
}
