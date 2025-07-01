/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_progress_logger::prelude::*;
use std::ops::ControlFlow::{Break, Continue};
use webgraph::traits::RandomAccessGraph;
use webgraph::{
    visits::depth_first::{EventPred, SeqPath},
    visits::{Sequential, StoppedWhenDone},
};

/// Returns whether the graph is acyclic.
///
/// This method performs a depth-first visit of the graph, stopping as soon as
/// a cycle is detected.
pub fn is_acyclic(graph: impl RandomAccessGraph, pl: &mut impl ProgressLog) -> bool {
    let num_nodes = graph.num_nodes();
    pl.item_name("node");
    pl.expected_updates(Some(num_nodes));
    pl.start("Checking acyclicity");

    let mut visit = SeqPath::new(&graph);

    let acyclic = visit.visit(0..num_nodes, |event| {
        // Stop the visit as soon as a back edge is found.
        match event {
            EventPred::Previsit { .. } => {
                pl.light_update();
                Continue(())
            }
            EventPred::Revisit { on_stack: true, .. } => Break(StoppedWhenDone {}),
            _ => Continue(()),
        }
    });

    pl.done();
    acyclic.is_continue()
}
