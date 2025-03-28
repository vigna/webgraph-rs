/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::RandomAccessGraph;
use dsi_progress_logger::prelude::*;
use std::collections::VecDeque;
use sux::prelude::BitVec;

/// Iterator on all nodes of the graph in a BFS order
pub struct BfsOrder<'a, G: RandomAccessGraph> {
    graph: &'a G,
    pl: ProgressLogger,
    seen: BitVec,
    queue: VecDeque<usize>,
    /// If the queue is empty, resume the BFS from that node.
    ///
    /// This allows initializing the BFS from all orphan nodes without reading
    /// the reverse graph.
    start: usize,
}

impl<G: RandomAccessGraph> BfsOrder<'_, G> {
    pub fn new(graph: &G) -> BfsOrder<G> {
        let num_nodes = graph.num_nodes();
        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("node")
            .local_speed(true)
            .expected_updates(Some(num_nodes));
        pl.start("Visiting graph in BFS order...");
        BfsOrder {
            graph,
            pl,
            seen: BitVec::new(num_nodes),
            queue: VecDeque::new(),
            start: 0,
        }
    }
}

impl<G: RandomAccessGraph> Iterator for BfsOrder<'_, G> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        self.pl.light_update();
        let current_node = match self.queue.pop_front() {
            None => {
                while self.seen[self.start] {
                    self.start += 1;
                    if self.start >= self.graph.num_nodes() {
                        self.pl.done();
                        return None;
                    }
                }
                self.seen.set(self.start, true);
                self.start
            }
            Some(node) => node,
        };

        for succ in self.graph.successors(current_node) {
            if !self.seen[succ] {
                self.queue.push_back(succ);
                self.seen.set(succ as _, true);
            }
        }

        Some(current_node)
    }
}

impl<G: RandomAccessGraph> ExactSizeIterator for BfsOrder<'_, G> {
    fn len(&self) -> usize {
        self.graph.num_nodes()
    }
}
