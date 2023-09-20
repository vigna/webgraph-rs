/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::RandomAccessGraph;
use dsi_progress_logger::ProgressLogger;
use std::collections::VecDeque;
use sux::prelude::BitVec;

/// Visit the graph in BFS order and return a vector with the order in which the
/// nodes were visited.
pub fn bfs_order<G: RandomAccessGraph>(graph: &G) -> Vec<usize> {
    let num_nodes = graph.num_nodes();
    let mut visited = BitVec::new(num_nodes);
    let mut queue = VecDeque::new();

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("Visiting graph in BFS order...");

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
    todo!("TODO: return the order in which the nodes were visited");
}
