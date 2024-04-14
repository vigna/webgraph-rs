/*
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::collections::VecDeque;

use clap::Parser;
use sux::bits::BitVec;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Prints the nodes of a graph in BFS order", long_about = None)]
struct Args {
    // The basename of the graph.
    basename: String,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // This line will load a big-endian graph (the default). To load
    // a little-endian graph, you need
    //
    // let graph = BVGraph::with_basename(&args.basename).endianness::<LE>().load()?;
    let graph = BVGraph::with_basename(&args.basename).load()?;
    let num_nodes = graph.num_nodes();
    let mut seen = vec![false; num_nodes];
    let mut queue = VecDeque::new();

    for start in 0..num_nodes {
        if seen[start] {
            continue;
        }
        queue.push_back(start as _);
        seen[start] = true;

        while !queue.is_empty() {
            let current_node = queue.pop_front().unwrap();
            println!("{}", current_node);
            for succ in graph.successors(current_node) {
                if !seen[succ] {
                    queue.push_back(succ);
                    seen[succ] = true;
                }
            }
        }
    }

    Ok(())
}
