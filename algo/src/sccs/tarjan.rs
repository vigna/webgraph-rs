/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::Sccs;
use dsi_progress_logger::ProgressLog;
use std::ops::ControlFlow::{Break, Continue};
use sux::bits::BitVec;
use sux::traits::BitVecOpsMut;
use webgraph::traits::RandomAccessGraph;
use webgraph::visits::{Sequential, StoppedWhenDone, depth_first::*};

/// Computes strongly connected components using Tarjan's algorithm.
///
/// Component numbers are generated starting from zero in order of emission:
/// thus, the component number provides a reverse topological order of the
/// components in the DAG of components.
///
/// # Implementation details
///
/// This implementation is iterative (it uses an explicit visit stack) and is
/// based on the recent survey by Tarjan and Zwick referenced below. It
/// implements all improvements described by the authors; in particular, the
/// early exit when a single remaining component is detected.
///
/// The visit stack in the case of a compressed graph is a much heavier object
/// than in the case of an explicit representation, where a pointer in the
/// adjacency list is sufficient, as we have to store one iterator per node
/// on the stack. While the allocation performed, say, by
/// [`BvGraph`] is essentially
/// constant-size, it is definitely larger than a pointer. It remains true that
/// the sum of the lengths of the visit stack and of the component stack cannot
/// exceed the number of nodes, but this property does not translate immediately
/// into a simple space bound.
///
/// # Further improvements
///
/// This implementation contains two simple original improvements: first, the
/// lead bits are not stored in a vector, as in previous implementations, but in
/// a bit stack. Thus, the space occupancy by the lead bits is bounded by the
/// maximum visit depth. Moreover, access to the stack happens only at the top,
/// whereas access to the bit vector is non-local and can cause cache misses.
///
/// Second, we use reverse timestamps in the range [1 . . *n*], where *n* is the
/// number of nodes. This allows us to assign component indices starting from
/// zero, which means that no renumbering is necessary at the end of the visit.
///
/// # References
///
/// Robert E. Tarjan and Uri Zwick. [Finding strong components using
/// depth-first search]. *European Journal of Combinatorics*, Volume 119,
/// 2024.
///
/// [Finding strong components using depth-first search]: https://doi.org/10.1016/j.ejc.2023.103815
///
/// [`BvGraph`]: webgraph::prelude::BvGraph
pub fn tarjan(graph: impl RandomAccessGraph, pl: &mut impl ProgressLog) -> Sccs {
    let num_nodes = graph.num_nodes();
    pl.item_name("node");
    pl.expected_updates(Some(num_nodes));
    pl.start("Computing strongly connected components...");

    let mut visit = SeqPred::new(&graph);
    let mut lead = BitVec::<Vec<u64>>::with_capacity(128);
    // Sentinel value guaranteeing that this stack is never empty
    lead.push(true);
    let mut component_stack = Vec::with_capacity(16);
    let mut high_link = vec![0; num_nodes].into_boxed_slice();
    // Node timestamps will start at num_nodes and will decrease with time,
    // that is, they will go in opposite order with respect to the classical
    // implementation. We keep track of the highest index seen, instead
    // of the lowest index seen, and we number components starting from
    // zero. We also raise index by the number of elements of each emitted
    // component. In this way unvisited nodes and emitted nodes have always
    // a lower value than index. This strategy is analogous to that
    // described in https://www.timl.id.au/scc, but in that case using
    // increasing timestamps results in components not being labeled
    // starting from zero, which is the case here instead.
    let mut index = num_nodes;
    let mut root_high_link = 0;
    let mut number_of_components = 0;

    if visit
        .visit(0..num_nodes, |event| {
            match event {
                EventPred::Init { .. } => {
                    root_high_link = index;
                }
                EventPred::Previsit { node: curr, .. } => {
                    pl.light_update();
                    high_link[curr] = index; // >= num_nodes, <= usize::MAX
                    index -= 1;
                    lead.push(true);
                }
                EventPred::Revisit { node, pred, .. } => {
                    // curr has not been emitted yet but it has a higher link
                    if high_link[pred] < high_link[node] {
                        // Safe as the stack is never empty
                        lead.set(lead.len() - 1, false);
                        high_link[pred] = high_link[node];
                        if high_link[pred] == root_high_link && index == 0 {
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
                EventPred::Postvisit {
                    node, parent: pred, ..
                } => {
                    // Safe as the stack is never empty
                    if lead.pop().unwrap() {
                        // Set the component index of nodes in the component
                        // stack with higher link than the current node
                        while let Some(comp_node) = component_stack.pop() {
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
