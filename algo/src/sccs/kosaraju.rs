/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::Sccs;
use crate::{
    prelude::*,
    visits::depth_first::{EventNoPred, SeqNoPred},
};
use dsi_progress_logger::ProgressLog;
use no_break::NoBreak;
use std::ops::ControlFlow::Continue;
use webgraph::traits::RandomAccessGraph;

/// Computes the strongly connected components of a graph using Kosaraju's algorithm.
///
/// # Arguments
///
/// * `graph`: the graph.
///
/// * `transpose`: the transpose of `graph`.
///
/// * `pl`: a progress logger.
pub fn kosaraju(
    graph: impl RandomAccessGraph,
    transpose: impl RandomAccessGraph,
    pl: &mut impl ProgressLog,
) -> Sccs {
    let num_nodes = graph.num_nodes();
    pl.item_name("node");
    pl.expected_updates(Some(num_nodes));
    pl.start("Computing strongly connected components...");

    let top_sort = top_sort(&graph, pl);
    let mut number_of_components = 0;
    let mut visit = SeqNoPred::new(&transpose);
    let mut components = vec![0; num_nodes].into_boxed_slice();

    visit
        .visit(top_sort, |event| {
            match event {
                EventNoPred::Previsit { node, .. } => {
                    pl.light_update();
                    components[node] = number_of_components;
                }
                EventNoPred::Done { .. } => {
                    number_of_components += 1;
                }
                _ => (),
            }
            Continue(())
        })
        .continue_value_no_break();

    pl.done();

    Sccs::new(number_of_components, components)
}
