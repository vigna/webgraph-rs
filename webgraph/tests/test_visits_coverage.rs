/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for depth-first visits (breadth-first visits are tested in
//! `test_breadth_first.rs`).

mod common;

use common::test_graph;
use no_break::NoBreak;
use std::ops::ControlFlow::{Break, Continue};
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;
use webgraph::visits::Sequential;
use webgraph::visits::depth_first::{
    DfsOrder, EventNoPred, EventPred, FilterArgsNoPred, FilterArgsPred, SeqNoPred, SeqPath, SeqPred,
};

/// Preorder of the test graph from node 0, with depths.
const PREORDER: [(usize, usize); 8] = [
    (0, 0),
    (1, 1),
    (3, 2),
    (6, 3),
    (2, 4),
    (4, 5),
    (5, 2),
    (7, 3),
];

/// Postorder of the test graph from node 0.
const POSTORDER: [usize; 8] = [4, 2, 6, 3, 7, 5, 1, 0];

/// A depth-first event, independent of the presence of predecessors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ev {
    Init(usize),
    Previsit(usize, usize),
    Revisit(usize),
    Postvisit(usize),
    Done(usize),
}

impl From<EventNoPred> for Ev {
    fn from(event: EventNoPred) -> Self {
        match event {
            EventNoPred::Init { root } => Ev::Init(root),
            EventNoPred::Previsit { node, depth, .. } => Ev::Previsit(node, depth),
            EventNoPred::Revisit { node, .. } => Ev::Revisit(node),
            EventNoPred::Done { root } => Ev::Done(root),
        }
    }
}

impl From<EventPred> for Ev {
    fn from(event: EventPred) -> Self {
        match event {
            EventPred::Init { root } => Ev::Init(root),
            EventPred::Previsit { node, depth, .. } => Ev::Previsit(node, depth),
            EventPred::Revisit { node, .. } => Ev::Revisit(node),
            EventPred::Postvisit { node, .. } => Ev::Postvisit(node),
            EventPred::Done { root } => Ev::Done(root),
        }
    }
}

/// Returns the (node, depth) pairs of the previsit events, in order.
fn previsits(events: &[Ev]) -> Vec<(usize, usize)> {
    events
        .iter()
        .filter_map(|ev| match *ev {
            Ev::Previsit(node, depth) => Some((node, depth)),
            _ => None,
        })
        .collect()
}

/// Returns the previsited nodes, in order.
fn previsited(events: &[Ev]) -> Vec<usize> {
    previsits(events)
        .into_iter()
        .map(|(node, _)| node)
        .collect()
}

/// Generates the tests common to all depth-first visits, given an expression
/// building a visit from a graph and the type of the filter arguments.
macro_rules! dfs_tests {
    ($name:ident, $new:expr, $FilterArgs:ty) => {
        mod $name {
            use super::*;

            /// Visits from `roots` using `filter` and returns all events, in order.
            fn filtered_events(
                graph: &VecGraph,
                roots: &[usize],
                filter: impl FnMut($FilterArgs) -> bool,
            ) -> Vec<Ev> {
                let mut events = vec![];
                $new(graph)
                    .visit_filtered(
                        roots.iter().copied(),
                        |event| {
                            events.push(Ev::from(event));
                            Continue(())
                        },
                        filter,
                    )
                    .continue_value_no_break();
                events
            }

            /// Visits from `roots` and returns all events, in order.
            fn events(graph: &VecGraph, roots: &[usize]) -> Vec<Ev> {
                filtered_events(graph, roots, |_| true)
            }

            #[test]
            fn test_events() {
                let events = events(&test_graph(), &[0]);
                assert_eq!(events[0], Ev::Init(0));
                assert_eq!(*events.last().unwrap(), Ev::Done(0));
                assert_eq!(previsits(&events), PREORDER);
                // Each of the 11 - 7 non-tree arcs generates a revisit
                let mut revisits: Vec<_> = events
                    .iter()
                    .filter_map(|ev| match *ev {
                        Ev::Revisit(node) => Some(node),
                        _ => None,
                    })
                    .collect();
                revisits.sort();
                assert_eq!(revisits, [2, 4, 6, 6]);
            }

            #[test]
            fn test_multiple_roots() {
                // Disconnected: 0 -> 1, 2 -> 3
                let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
                let events = events(&graph, &[0, 2]);
                let inits: Vec<_> = events
                    .iter()
                    .filter(|ev| matches!(ev, Ev::Init(_)))
                    .copied()
                    .collect();
                assert_eq!(inits, [Ev::Init(0), Ev::Init(2)]);
                assert_eq!(previsited(&events), [0, 1, 2, 3]);
            }

            #[test]
            fn test_filter() {
                // 0 -> 1 -> 2 -> 3
                let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
                let events = filtered_events(&graph, &[0], |args| args.node != 2);
                assert_eq!(previsited(&events), [0, 1]);

                let mut visited = vec![];
                $new(&graph)
                    .visit_filtered_with(
                        [0],
                        &mut visited,
                        |visited, event| {
                            if let Ev::Previsit(node, _) = Ev::from(event) {
                                visited.push(node);
                            }
                            Continue(())
                        },
                        |_, args: $FilterArgs| args.node != 2,
                    )
                    .continue_value_no_break();
                assert_eq!(visited, [0, 1]);
            }

            #[test]
            fn test_visit_with() {
                let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
                let mut visited = vec![];
                $new(&graph)
                    .visit_with([0], &mut visited, |visited, event| {
                        if let Ev::Previsit(node, _) = Ev::from(event) {
                            visited.push(node);
                        }
                        Continue(())
                    })
                    .continue_value_no_break();
                assert_eq!(visited, [0, 1, 2]);
            }

            #[test]
            fn test_early_termination() {
                let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
                let mut visited = vec![];
                let result = $new(&graph).visit([0], |event| {
                    if let Ev::Previsit(node, _) = Ev::from(event) {
                        visited.push(node);
                        if node == 2 {
                            return Break(node);
                        }
                    }
                    Continue(())
                });
                assert_eq!(result, Break(2));
                assert_eq!(visited, [0, 1, 2]);
            }

            #[test]
            fn test_reset() {
                let graph = test_graph();
                let mut visit = $new(&graph);
                for _ in 0..2 {
                    let mut visited = vec![];
                    visit
                        .visit([0], |event| {
                            if let Ev::Previsit(node, depth) = Ev::from(event) {
                                visited.push((node, depth));
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                    assert_eq!(visited, PREORDER);
                    visit.reset();
                }
            }
        }
    };
}

dfs_tests!(seq_no_pred, SeqNoPred::new, FilterArgsNoPred);
dfs_tests!(seq_pred, SeqPred::new, FilterArgsPred);
dfs_tests!(seq_path, SeqPath::new, FilterArgsPred);

// ── Visits with predecessors ──

#[test]
fn test_postvisit_and_parents() {
    let graph = test_graph();
    let mut postorder = vec![];
    let mut parents = vec![usize::MAX; graph.num_nodes()];
    SeqPred::new(&graph)
        .visit([0], |event| {
            match event {
                EventPred::Previsit { node, parent, .. } => parents[node] = parent,
                EventPred::Postvisit { node, .. } => postorder.push(node),
                _ => {}
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(postorder, POSTORDER);
    // The parent of the root is the root itself
    assert_eq!(parents, [0, 0, 6, 1, 2, 1, 3, 5]);

    let mut postorder = vec![];
    SeqPath::new(&graph)
        .visit([0], |event| {
            if let EventPred::Postvisit { node, .. } = event {
                postorder.push(node);
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(postorder, POSTORDER);
}

#[test]
fn test_path_on_stack() {
    // Arc 4 -> 6 closes the cycle 6 -> 2 -> 4 -> 6 on the visit path
    let graph = test_graph();
    let result = SeqPath::new(&graph).visit([0], |event| {
        if let EventPred::Revisit {
            node,
            pred,
            on_stack: true,
            ..
        } = event
        {
            return Break((pred, node));
        }
        Continue(())
    });
    assert_eq!(result, Break((4, 6)));
}

#[test]
fn test_interrupted_visit_stack() {
    // Linear chain: 0 -> 1 -> 2 -> 3 -> 4
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visit = SeqPred::new(&graph);
    let interrupted_node = visit.visit([0], |event| {
        if let EventPred::Previsit { node: 3, .. } = event {
            return Break(3);
        }
        Continue(())
    });
    assert_eq!(interrupted_node, Break(3));
    // After interruption at node 3, the stack yields the parents of nodes on
    // the visit path (except for the last one), in reverse order. The
    // interrupted node (3) was never pushed.
    let stack_nodes: Vec<usize> = visit.stack().collect();
    assert_eq!(stack_nodes, [1, 0]);
}

// ── DfsOrder ──

#[test]
fn test_dfs_order() {
    let graph = test_graph();
    let mut visit = SeqPred::new(&graph);
    let order = DfsOrder::new(&mut visit);
    assert_eq!(order.len(), 8);
    let events: Vec<_> = order.collect();
    let node_depths: Vec<_> = events.iter().map(|e| (e.node, e.depth)).collect();
    assert_eq!(node_depths, PREORDER);
    assert!(events.iter().all(|e| e.root == 0));

    // Disconnected: the iterator starts new visits from unvisited nodes
    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let nodes: Vec<_> = SeqPred::new(&graph)
        .into_iter()
        .map(|e| (e.root, e.node, e.parent, e.depth))
        .collect();
    assert_eq!(
        nodes,
        [(0, 0, 0, 0), (0, 1, 0, 1), (2, 2, 2, 0), (2, 3, 2, 1)]
    );

    // Reset
    let mut visit = SeqPred::new(&graph);
    let first: Vec<_> = (&mut visit).into_iter().map(|e| e.node).collect();
    visit.reset();
    let second: Vec<_> = (&mut visit).into_iter().map(|e| e.node).collect();
    assert_eq!(first, second);
}
