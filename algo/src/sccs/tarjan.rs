/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::Sccs;
use crate::visits::{depth_first::*, Sequential, StoppedWhenDone};
use dsi_progress_logger::ProgressLog;
use std::ops::ControlFlow::{Break, Continue};
use sux::bits::BitVec;
use webgraph::traits::RandomAccessGraph;

/// Tarjan's algorithm for strongly connected components.
pub fn tarjan(graph: impl RandomAccessGraph, pl: &mut impl ProgressLog) -> Sccs {
    let num_nodes = graph.num_nodes();
    pl.item_name("node");
    pl.expected_updates(Some(num_nodes));
    pl.start("Computing strongly connected components...");

    let mut visit = SeqPred::new(&graph);
    let mut lead = BitVec::with_capacity(128);
    // Sentinel value guaranteeing that this stack is never empty
    lead.push(true);
    let mut component_stack = Vec::with_capacity(16);
    let mut high_link = vec![0; num_nodes].into_boxed_slice();
    // Node timestamps will start at num_nodes and will decrease with time,
    // that is, they will go in opposite order with respect to the classical
    // implementation. We keep track of the highest index seen, instead
    // of the lowest index seen, and we number compoments starting from
    // zero. We also raise index by the number of elements of each emitted
    // component. In this way unvisited nodes and emitted nodes have always
    // a lower value than index. This strategy is analogous to that
    // described in https://www.timl.id.au/scc, but in that case using
    // increasing timestamps results in components not being labelled
    // starting from zero, which is the case here instead.
    let mut index = num_nodes;
    let mut root_low_link = 0;
    let mut number_of_components = 0;

    if visit
        .visit(0..num_nodes, |event| {
            match event {
                EventPred::Init { .. } => {
                    root_low_link = index;
                }
                EventPred::Previsit { node: curr, .. } => {
                    pl.light_update();
                    high_link[curr] = index; // >= num_nodes, <= umax::SIZE
                    index -= 1;
                    lead.push(true);
                }
                EventPred::Revisit { node, pred, .. } => {
                    // curr has not been emitted yet but it has a higher link
                    if high_link[pred] < high_link[node] {
                        // Safe as the stack is never empty
                        lead.set(lead.len() - 1, false);
                        high_link[pred] = high_link[node];
                        if high_link[pred] == root_low_link && index == 0 {
                            // All nodes have been discovered, and we
                            // found a high link identical to that of the
                            // root: thus, all nodes on the visit path
                            // and all nodes in the component stack
                            // belong to the same component.

                            // pred is the last node on the visit path,
                            // so it won't be returned by the stack method
                            high_link[pred] = number_of_components;
                            for &node in component_stack.iter() {
                                high_link[node] = number_of_components;
                            }
                            // Nodes on the visit path will be assigned
                            // to the same component later
                            return Break(StoppedWhenDone {});
                        }
                    }
                }
                EventPred::Postvisit { node, pred, .. } => {
                    // Safe as the stack is never empty
                    if lead.pop().unwrap() {
                        // Set the component index of nodes in the component
                        // stack with higher link than the current node
                        while let Some(comp_node) = component_stack.pop() {
                            // TODO: ugly
                            if high_link[node] < high_link[comp_node] {
                                component_stack.push(comp_node);
                                break;
                            }
                            index += 1;
                            high_link[comp_node] = number_of_components;
                        }
                        // Set the component index of the current node
                        high_link[node] = number_of_components;
                        index += 1;
                        number_of_components += 1;
                    } else {
                        component_stack.push(node);
                        // Propagate knowledge to the parent
                        if high_link[pred] < high_link[node] {
                            // Safe as the stack is never empty
                            lead.set(lead.len() - 1, false);
                            high_link[pred] = high_link[node];
                        }
                    }
                }
                _ => {}
            }
            Continue(())
        })
        .is_break()
    {
        // In case we exited early, complete the assignment
        for node in visit.stack() {
            high_link[node] = number_of_components;
        }
        number_of_components += 1;
    }
    pl.done();
    Sccs::new(number_of_components, high_link)
}
