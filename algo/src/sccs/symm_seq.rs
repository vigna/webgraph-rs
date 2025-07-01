/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::Sccs;
use dsi_progress_logger::ProgressLog;
use no_break::NoBreak;
use std::ops::ControlFlow::Continue;
use webgraph::traits::RandomAccessGraph;
use webgraph::{prelude::*, visits::Sequential};

/// Connected components of symmetric graphs by sequential visits.
pub fn symm_seq(graph: impl RandomAccessGraph, pl: &mut impl ProgressLog) -> Sccs {
    // debug_assert!(check_symmetric(&graph)); requires sync

    let num_nodes = graph.num_nodes();
    pl.item_name("node");
    pl.expected_updates(Some(num_nodes));
    pl.start("Computing connected components...");

    let mut visit = depth_first::SeqNoPred::new(&graph);
    let mut component = Box::new_uninit_slice(num_nodes);
    let mut number_of_components = 0;

    visit
        .visit(0..num_nodes, |event| {
            match event {
                depth_first::EventNoPred::Previsit { node, .. } => {
                    pl.light_update();
                    component[node].write(number_of_components);
                }
                depth_first::EventNoPred::Done { .. } => {
                    number_of_components += 1;
                }
                _ => (),
            }
            Continue(())
        })
        .continue_value_no_break();

    let component = unsafe { component.assume_init() };

    pl.done();

    Sccs::new(number_of_components, component)
}
