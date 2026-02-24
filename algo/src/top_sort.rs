/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_progress_logger::ProgressLog;
use no_break::NoBreak;
use std::ops::ControlFlow::Continue;
use webgraph::traits::RandomAccessGraph;
use webgraph::{
    visits::depth_first::SeqPred,
    visits::{Sequential, depth_first::*},
};

/// Returns the nodes of the graph in topological-sort order, if the graph is
/// acyclic.
///
/// Otherwise, the order reflects the exit times from a depth-first visit of the
/// graph.
pub fn top_sort(graph: impl RandomAccessGraph, pl: &mut impl ProgressLog) -> Box<[usize]> {
    let num_nodes = graph.num_nodes();
    pl.item_name("node");
    pl.expected_updates(Some(num_nodes));
    pl.start("Computing topological sort");

    let mut visit = SeqPred::new(&graph);
    let mut top_sort = Box::new_uninit_slice(num_nodes);
    let mut pos = num_nodes;

    visit
        .visit(0..num_nodes, |event| {
            match event {
                EventPred::Previsit { .. } => {
                    pl.light_update();
                }
                EventPred::Postvisit { node, .. } => {
                    pos -= 1;
                    top_sort[pos].write(node);
                }
                _ => (),
            }
            Continue(())
        })
        .continue_value_no_break();

    pl.done();
    // SAFETY: we write in each element of top_sort
    unsafe { top_sort.assume_init() }
}
