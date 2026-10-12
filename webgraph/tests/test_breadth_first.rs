/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for breadth-first visits: the same tests are run on all sequential
//! and parallel implementations.

mod common;

use anyhow::Result;
use common::test_graph;
use no_break::NoBreak;
use std::ops::ControlFlow::{Break, Continue};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(miri))]
use webgraph::utils::Granularity;
use webgraph::visits::breadth_first::{EventNoPred, EventPred, IterEvent, IterFromRootsEvent};
#[cfg(not(miri))]
use webgraph::visits::{Parallel, breadth_first::FilterArgsNoPred};
use webgraph::visits::{Sequential, breadth_first::FilterArgsPred};
use webgraph::{graphs::vec_graph::VecGraph, prelude::*};

/// Computes BFS distances by visiting first from `start`, and then from the
/// following nodes (cyclically) that have not been reached yet.
fn correct_distances<G: RandomAccessGraph>(graph: &G, start: usize) -> Vec<usize> {
    let n = graph.num_nodes();
    let mut distances = vec![usize::MAX; n];
    for i in 0..n {
        let root = (i + start) % n;
        if distances[root] != usize::MAX {
            continue;
        }
        distances[root] = 0;
        let mut frontier = vec![root];
        let mut distance = 1;
        while !frontier.is_empty() {
            let mut next_frontier = vec![];
            for node in frontier {
                for succ in graph.successors(node) {
                    if distances[succ] == usize::MAX {
                        distances[succ] = distance;
                        next_frontier.push(succ);
                    }
                }
            }
            frontier = next_frontier;
            distance += 1;
        }
    }
    distances
}

/// A breadth-first event, independent of the presence of predecessors.
///
/// Visits carry the node, the distance, and the predecessor, and revisits
/// carry the node and the predecessor; predecessors are `None` for visits
/// without predecessors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Ev {
    Init,
    FrontierSize(usize, usize),
    Visit(usize, usize, Option<usize>),
    Revisit(usize, Option<usize>),
    Done,
}

impl From<EventPred> for Ev {
    fn from(event: EventPred) -> Self {
        match event {
            EventPred::Init {} => Ev::Init,
            EventPred::FrontierSize { distance, size } => Ev::FrontierSize(distance, size),
            EventPred::Visit {
                node,
                pred,
                distance,
            } => Ev::Visit(node, distance, Some(pred)),
            EventPred::Revisit { node, pred } => Ev::Revisit(node, Some(pred)),
            EventPred::Done {} => Ev::Done,
        }
    }
}

impl From<EventNoPred> for Ev {
    fn from(event: EventNoPred) -> Self {
        match event {
            EventNoPred::Init {} => Ev::Init,
            EventNoPred::FrontierSize { distance, size } => Ev::FrontierSize(distance, size),
            EventNoPred::Visit { node, distance, .. } => Ev::Visit(node, distance, None),
            EventNoPred::Revisit { node, .. } => Ev::Revisit(node, None),
            EventNoPred::Done {} => Ev::Done,
        }
    }
}

/// Returns the sorted (node, distance) pairs of the visit events.
fn visits(events: &[Ev]) -> Vec<(usize, usize)> {
    let mut visits: Vec<_> = events
        .iter()
        .filter_map(|ev| match *ev {
            Ev::Visit(node, distance, _) => Some((node, distance)),
            _ => None,
        })
        .collect();
    visits.sort();
    visits
}

/// Returns the sorted nodes of the revisit events.
fn revisits(events: &[Ev]) -> Vec<usize> {
    let mut revisits: Vec<_> = events
        .iter()
        .filter_map(|ev| match *ev {
            Ev::Revisit(node, _) => Some(node),
            _ => None,
        })
        .collect();
    revisits.sort();
    revisits
}

/// Returns the sorted arcs, as (predecessor, node) pairs, of the visit events
/// of non-root nodes (i.e., tree arcs) and of the revisit events.
///
/// The result is empty for visits without predecessors. For visits with
/// predecessors in which all nodes are reachable from the roots, each arc of
/// the graph appears exactly once, independently of the visit order.
fn arcs(events: &[Ev]) -> Vec<(usize, usize)> {
    let mut arcs: Vec<_> = events
        .iter()
        .filter_map(|ev| match *ev {
            Ev::Visit(node, _, Some(pred)) if pred != node => Some((pred, node)),
            Ev::Revisit(node, Some(pred)) => Some((pred, node)),
            _ => None,
        })
        .collect();
    arcs.sort();
    arcs
}

/// Returns the sorted (distance, size) pairs of the frontier-size events.
fn frontier_sizes(events: &[Ev]) -> Vec<(usize, usize)> {
    let mut sizes: Vec<_> = events
        .iter()
        .filter_map(|ev| match *ev {
            Ev::FrontierSize(distance, size) => Some((distance, size)),
            _ => None,
        })
        .collect();
    sizes.sort();
    sizes
}

/// Checks that the events start with `Init`, `FrontierSize` at distance 0
/// with the number of roots, and the root visits, and end with `Done`.
fn check_prefix_and_suffix(events: &[Ev], roots: &[usize]) {
    assert_eq!(events[0], Ev::Init);
    assert_eq!(events[1], Ev::FrontierSize(0, roots.len()));
    for ev in &events[2..2 + roots.len()] {
        match ev {
            Ev::Visit(node, 0, _) => assert!(roots.contains(node), "unexpected root {node}"),
            ev => panic!("expected a root visit, got {ev:?}"),
        }
    }
    assert_eq!(*events.last().unwrap(), Ev::Done);
}

/// Generates the tests for a breadth-first visit, given an expression
/// building a visit from a graph, the type of the filter arguments, and the
/// names of the visit methods.
macro_rules! bfs_tests {
    ($name:ident, $new:expr, $FilterArgs:ty, $preds:literal, $visit:ident, $visit_filtered:ident, $visit_with:ident, $visit_filtered_with:ident) => {
        mod $name {
            use super::*;

            /// Visits from `roots` using `filter` and returns all events, in order.
            fn filtered_events<G: RandomAccessGraph + Sync>(
                graph: &G,
                roots: &[usize],
                filter: impl Fn($FilterArgs) -> bool + Sync,
            ) -> Vec<Ev> {
                let events = Mutex::new(vec![]);
                $new(graph)
                    .$visit_filtered(
                        roots.iter().copied(),
                        |event| {
                            events.lock().unwrap().push(Ev::from(event));
                            Continue(())
                        },
                        filter,
                    )
                    .continue_value_no_break();
                events.into_inner().unwrap()
            }

            /// Visits from `roots` and returns all events, in order.
            fn events<G: RandomAccessGraph + Sync>(graph: &G, roots: &[usize]) -> Vec<Ev> {
                filtered_events(graph, roots, |_| true)
            }

            /// Visits from each root in turn (without resetting) and returns
            /// the distances.
            fn distances<G: RandomAccessGraph + Sync>(
                graph: &G,
                roots: impl IntoIterator<Item = usize>,
            ) -> Vec<usize> {
                let mut visit = $new(graph);
                let distances: Vec<_> = (0..graph.num_nodes())
                    .map(|_| AtomicUsize::new(usize::MAX))
                    .collect();
                for root in roots {
                    visit
                        .$visit([root], |event| {
                            if let Ev::Visit(node, distance, _) = Ev::from(event) {
                                distances[node].store(distance, Ordering::Relaxed);
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                }
                distances.into_iter().map(AtomicUsize::into_inner).collect()
            }

            #[test]
            fn test_distances() {
                let graph = test_graph();
                assert_eq!(distances(&graph, [0]), [0, 1, 1, 2, 2, 2, 3, 3]);
                for start in 0..graph.num_nodes() {
                    let roots = (0..graph.num_nodes()).map(|i| (i + start) % graph.num_nodes());
                    assert_eq!(distances(&graph, roots), correct_distances(&graph, start));
                }
            }

            #[test]
            fn test_events() {
                let graph = test_graph();
                let events = events(&graph, &[0]);
                check_prefix_and_suffix(&events, &[0]);
                assert_eq!(
                    visits(&events),
                    [
                        (0, 0),
                        (1, 1),
                        (2, 1),
                        (3, 2),
                        (4, 2),
                        (5, 2),
                        (6, 3),
                        (7, 3)
                    ]
                );
                // Each of the 11 - 7 non-tree arcs generates a revisit
                assert_eq!(revisits(&events), [2, 4, 6, 6]);
                // Visits with predecessors report each arc exactly once
                let expected: &[_] = if $preds {
                    &common::TEST_GRAPH_ARCS
                } else {
                    &[]
                };
                assert_eq!(arcs(&events), expected);
                assert_eq!(frontier_sizes(&events), [(0, 1), (1, 2), (2, 3), (3, 2)]);
            }

            #[test]
            fn test_multiple_roots() {
                // 0 -> 1 -> 2 <- 3, 4 -> 5
                let graph = VecGraph::from_arcs([(0, 1), (1, 2), (3, 2), (4, 5)]);
                let events = events(&graph, &[0, 3]);
                check_prefix_and_suffix(&events, &[0, 3]);
                assert_eq!(visits(&events), [(0, 0), (1, 1), (2, 1), (3, 0)]);
            }

            #[test]
            fn test_filter() {
                // 0 -> 1 -> 2 -> 3
                let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
                let events = filtered_events(&graph, &[0], |args| args.node != 2);
                assert_eq!(visits(&events), [(0, 0), (1, 1)]);
                let events = filtered_events(&graph, &[0], |args| args.distance <= 2);
                assert_eq!(visits(&events), [(0, 0), (1, 1), (2, 2)]);
            }

            #[test]
            fn test_filtered_revisits() {
                // Regression: the sequential BFS emitted Revisit events
                // without consulting the filter, unlike the parallel ones.
                let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
                let events = filtered_events(&graph, &[0], |args| args.distance <= 1);
                assert_eq!(revisits(&events), []);
                let expected: &[_] = if $preds { &[(0, 1), (0, 2)] } else { &[] };
                assert_eq!(arcs(&events), expected);
                let events = filtered_events(&graph, &[0], |args| args.distance <= 2);
                assert_eq!(revisits(&events), [2]);
                // The revisit of 2 comes from 1
                let expected: &[_] = if $preds {
                    &[(0, 1), (0, 2), (1, 2)]
                } else {
                    &[]
                };
                assert_eq!(arcs(&events), expected);
            }

            #[test]
            fn test_visit_with() {
                let graph = test_graph();
                let count = AtomicUsize::new(0);
                $new(&graph)
                    .$visit_with([0], &count, |count, event| {
                        if let Ev::Visit(..) = Ev::from(event) {
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        Continue(())
                    })
                    .continue_value_no_break();
                assert_eq!(count.into_inner(), 8);

                let count = AtomicUsize::new(0);
                $new(&graph)
                    .$visit_filtered_with(
                        [0],
                        &count,
                        |count, event| {
                            if let Ev::Visit(..) = Ev::from(event) {
                                count.fetch_add(1, Ordering::Relaxed);
                            }
                            Continue(())
                        },
                        |_, args: $FilterArgs| args.distance <= 1,
                    )
                    .continue_value_no_break();
                assert_eq!(count.into_inner(), 3);
            }

            #[test]
            fn test_reset() {
                let graph = test_graph();
                let mut visit = $new(&graph);
                for _ in 0..2 {
                    let count = AtomicUsize::new(0);
                    visit
                        .$visit([0], |event| {
                            if let Ev::Visit(..) = Ev::from(event) {
                                count.fetch_add(1, Ordering::Relaxed);
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                    assert_eq!(count.into_inner(), 8);
                    visit.reset();
                }
            }

            #[test]
            fn test_early_termination() {
                let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
                let result = $new(&graph).$visit([0], |event| {
                    if let Ev::Visit(2, ..) = Ev::from(event) {
                        return Break("found");
                    }
                    Continue(())
                });
                assert_eq!(result, Break("found"));
            }
        }
    };
}

/// Generates the tests on cnr-2000 for a breadth-first visit.
macro_rules! bfs_cnr_2000_tests {
    ($name:ident, $new:expr, $visit:ident) => {
        #[cfg(not(miri))]
        mod $name {
            use super::*;

            #[test]
            fn test_cnr_2000() -> Result<()> {
                let graph = BvGraph::with_basename(common::cnr_2000_basename()).load()?;
                let n = graph.num_nodes();
                let distances: Vec<_> = (0..n).map(|_| AtomicUsize::new(usize::MAX)).collect();
                let mut visit = $new(&graph);
                for i in 0..n {
                    visit
                        .$visit([(i + 10000) % n], |event| {
                            if let Ev::Visit(node, distance, _) = Ev::from(event) {
                                distances[node].store(distance, Ordering::Relaxed);
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                }
                let distances: Vec<_> =
                    distances.into_iter().map(AtomicUsize::into_inner).collect();
                assert_eq!(distances, correct_distances(&graph, 10000));
                Ok(())
            }

            #[test]
            fn test_frontier_sizes_cnr_2000() -> Result<()> {
                let graph = BvGraph::with_basename(common::cnr_2000_basename()).load()?;
                let n = graph.num_nodes();
                for roots in [vec![0], vec![0, n / 2, n - 1]] {
                    let events = Mutex::new(vec![]);
                    $new(&graph)
                        .$visit(roots.iter().copied(), |event| {
                            events.lock().unwrap().push(Ev::from(event));
                            Continue(())
                        })
                        .continue_value_no_break();
                    let events = events.into_inner().unwrap();
                    check_prefix_and_suffix(&events, &roots);
                    // The frontier sizes add up to the number of visits at each distance
                    let mut visits_by_distance = std::collections::BTreeMap::new();
                    for (_, distance) in visits(&events) {
                        *visits_by_distance.entry(distance).or_insert(0) += 1;
                    }
                    let sizes: std::collections::BTreeMap<_, _> =
                        frontier_sizes(&events).into_iter().collect();
                    assert_eq!(sizes, visits_by_distance);
                }
                Ok(())
            }
        }
    };
}

bfs_tests!(
    seq,
    webgraph::visits::breadth_first::Seq::new,
    FilterArgsPred,
    true,
    visit,
    visit_filtered,
    visit_with,
    visit_filtered_with
);
bfs_cnr_2000_tests!(
    seq_cnr_2000,
    webgraph::visits::breadth_first::Seq::new,
    visit
);

#[cfg(not(miri))]
mod par {
    use super::*;
    use webgraph::visits::breadth_first::{ParFairNoPred, ParFairPred, ParLowMem};

    bfs_tests!(
        fair_pred,
        |g| ParFairPred::with_granularity(g, Granularity::Nodes(32)),
        FilterArgsPred,
        true,
        par_visit,
        par_visit_filtered,
        par_visit_with,
        par_visit_filtered_with
    );
    bfs_tests!(
        fair_pred_fine,
        |g| ParFairPred::with_granularity(g, Granularity::Nodes(1)),
        FilterArgsPred,
        true,
        par_visit,
        par_visit_filtered,
        par_visit_with,
        par_visit_filtered_with
    );
    bfs_tests!(
        fair_no_pred,
        ParFairNoPred::new,
        FilterArgsNoPred,
        false,
        par_visit,
        par_visit_filtered,
        par_visit_with,
        par_visit_filtered_with
    );
    bfs_tests!(
        fair_no_pred_fine,
        |g| ParFairNoPred::with_granularity(g, Granularity::Nodes(2)),
        FilterArgsNoPred,
        false,
        par_visit,
        par_visit_filtered,
        par_visit_with,
        par_visit_filtered_with
    );
    bfs_tests!(
        low_mem,
        |g| ParLowMem::with_granularity(g, Granularity::Nodes(32)),
        FilterArgsPred,
        true,
        par_visit,
        par_visit_filtered,
        par_visit_with,
        par_visit_filtered_with
    );
    bfs_tests!(
        low_mem_fine,
        |g| ParLowMem::with_granularity(g, Granularity::Nodes(1)),
        FilterArgsPred,
        true,
        par_visit,
        par_visit_filtered,
        par_visit_with,
        par_visit_filtered_with
    );
    bfs_cnr_2000_tests!(
        fair_pred_cnr_2000,
        |g| ParFairPred::with_granularity(g, Granularity::Nodes(32)),
        par_visit
    );
    bfs_cnr_2000_tests!(fair_no_pred_cnr_2000, ParFairNoPred::new, par_visit);
    bfs_cnr_2000_tests!(
        low_mem_cnr_2000,
        |g| ParLowMem::with_granularity(g, Granularity::Nodes(32)),
        par_visit
    );
}

// ── BfsOrder ──

/// Returns the events of the iterator on a sequential visit.
fn bfs_order(graph: &VecGraph) -> Vec<IterEvent> {
    (&mut webgraph::visits::breadth_first::Seq::new(graph))
        .into_iter()
        .collect()
}

/// Returns the (node, distance) pairs of iterator events.
fn node_distances(events: &[IterEvent]) -> Vec<(usize, usize)> {
    events.iter().map(|e| (e.node, e.distance)).collect()
}

fn iter_event(root: usize, parent: usize, node: usize, distance: usize) -> IterEvent {
    IterEvent {
        root,
        parent,
        node,
        distance,
    }
}

#[test]
fn test_start() {
    // 4 -> 0 -> 2
    //       `-> 3
    // 1 -> 5
    let graph = VecGraph::from_arcs([(4, 0), (0, 2), (0, 3), (1, 5)]);
    let order: Vec<_> = bfs_order(&graph).iter().map(|e| e.node).collect();
    assert_eq!(order, [0, 2, 3, 1, 5, 4]);

    // 0 -> 4 -> 2
    //       `-> 3
    // 1 -> 5
    let graph = VecGraph::from_arcs([(0, 4), (4, 2), (4, 3), (1, 5)]);
    let order: Vec<_> = bfs_order(&graph).iter().map(|e| e.node).collect();
    assert_eq!(order, [0, 4, 2, 3, 1, 5]);
}

#[test]
fn test_bfs_order_test_graph() {
    let graph = test_graph();
    let events = bfs_order(&graph);
    assert_eq!(
        node_distances(&events),
        [
            (0, 0),
            (1, 1),
            (2, 1),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 3),
            (7, 3)
        ]
    );
    let parents: Vec<_> = events.iter().map(|e| e.parent).collect();
    assert_eq!(parents, [0, 0, 0, 1, 1, 1, 3, 5]);
    assert!(events.iter().all(|e| e.root == 0));
}

#[test]
fn test_bfs_order_path_and_cycle() {
    // 0 → 1 → 2 → 3
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    assert_eq!(
        bfs_order(&graph),
        [
            iter_event(0, 0, 0, 0),
            iter_event(0, 0, 1, 1),
            iter_event(0, 1, 2, 2),
            iter_event(0, 2, 3, 3),
        ]
    );
    // 0 → 1 → 2 → 0
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert_eq!(node_distances(&bfs_order(&graph)), [(0, 0), (1, 1), (2, 2)]);
}

#[test]
fn test_bfs_order_diamond() {
    //   0
    //  / \
    // 1   2
    //  \ /
    //   3
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    assert_eq!(
        bfs_order(&graph),
        [
            iter_event(0, 0, 0, 0),
            iter_event(0, 0, 1, 1),
            iter_event(0, 0, 2, 1),
            // Discovered first via 1
            iter_event(0, 1, 3, 2),
        ]
    );
}

#[test]
fn test_bfs_order_disconnected() {
    // Component 1: 0 → 1
    // Component 2: 2 → 3
    // Isolated: 4
    let mut graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    graph.add_node(4);
    assert_eq!(
        bfs_order(&graph),
        [
            iter_event(0, 0, 0, 0),
            iter_event(0, 0, 1, 1),
            iter_event(2, 2, 2, 0),
            iter_event(2, 2, 3, 1),
            iter_event(4, 4, 4, 0),
        ]
    );
}

#[test]
fn test_bfs_order_self_loop() {
    // 0 → 0, 0 → 1, 1 → 1
    let graph = VecGraph::from_arcs([(0, 0), (0, 1), (1, 1)]);
    assert_eq!(node_distances(&bfs_order(&graph)), [(0, 0), (1, 1)]);
}

#[test]
fn test_bfs_order_single_node_and_empty() {
    assert_eq!(bfs_order(&VecGraph::empty(1)), [iter_event(0, 0, 0, 0)]);
    // Regression: BfsOrder asserted a non-empty graph, so iterating all
    // nodes of an empty graph (e.g., in 'webgraph perm bfs') panicked.
    assert_eq!(bfs_order(&VecGraph::empty(0)), []);
}

#[test]
fn test_bfs_order_exact_size() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = webgraph::visits::breadth_first::Seq::new(&graph);
    let mut iter = (&mut visit).into_iter();
    for len in (0..=4).rev() {
        assert_eq!(iter.len(), len);
        assert_eq!(iter.next().is_some(), len > 0);
    }
}

#[test]
fn test_bfs_order_reverse_star() {
    // {1, 2, 3} → 0: BFS starts from node 0 (no outgoing), then finds 1, 2,
    // 3 as new roots
    let graph = VecGraph::from_arcs([(1, 0), (2, 0), (3, 0)]);
    assert_eq!(
        bfs_order(&graph),
        [
            iter_event(0, 0, 0, 0),
            iter_event(1, 1, 1, 0),
            iter_event(2, 2, 2, 0),
            iter_event(3, 3, 3, 0),
        ]
    );
}

#[test]
fn test_bfs_order_distances_vs_callback() {
    // BfsOrder gives the same distances as the callback-based visit when
    // visiting all nodes
    let graph = test_graph();
    let mut distances = vec![0; graph.num_nodes()];
    for event in bfs_order(&graph) {
        distances[event.node] = event.distance;
    }
    assert_eq!(distances, correct_distances(&graph, 0));
}

// ── BfsOrderFromRoots ──

/// Returns the events of the iterator on a sequential visit from `roots`.
fn bfs_order_from_roots(graph: &VecGraph, roots: &[usize]) -> Result<Vec<IterFromRootsEvent>> {
    Ok(webgraph::visits::breadth_first::Seq::new(graph)
        .iter_from_roots(roots.iter().copied())?
        .collect())
}

fn from_roots_event(parent: usize, node: usize, distance: usize) -> IterFromRootsEvent {
    IterFromRootsEvent {
        parent,
        node,
        distance,
    }
}

#[test]
fn test_from_roots_single_root() -> Result<()> {
    // 0 → 1 → 2, isolated node 3
    let mut graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    graph.add_node(3);
    assert_eq!(
        bfs_order_from_roots(&graph, &[0])?,
        [
            from_roots_event(0, 0, 0),
            from_roots_event(0, 1, 1),
            from_roots_event(1, 2, 2),
        ]
    );
    // A cycle
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let events = bfs_order_from_roots(&graph, &[0])?;
    let node_distances: Vec<_> = events.iter().map(|e| (e.node, e.distance)).collect();
    assert_eq!(node_distances, [(0, 0), (1, 1), (2, 2)]);
    // A self-loop
    let graph = VecGraph::from_arcs([(0, 0), (0, 1)]);
    assert_eq!(
        bfs_order_from_roots(&graph, &[0])?,
        [from_roots_event(0, 0, 0), from_roots_event(0, 1, 1)]
    );
    // A single node
    assert_eq!(
        bfs_order_from_roots(&VecGraph::empty(1), &[0])?,
        [from_roots_event(0, 0, 0)]
    );
    Ok(())
}

#[test]
fn test_from_roots_multiple_roots() -> Result<()> {
    // Roots first, at distance 0, then their successors
    let graph = VecGraph::from_arcs([(0, 2), (1, 3)]);
    assert_eq!(
        bfs_order_from_roots(&graph, &[0, 1])?,
        [
            from_roots_event(0, 0, 0),
            from_roots_event(1, 1, 0),
            from_roots_event(0, 2, 1),
            from_roots_event(1, 3, 1),
        ]
    );
    // Roots are visited in the order in which they are given
    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    assert_eq!(
        bfs_order_from_roots(&graph, &[2, 0])?,
        [
            from_roots_event(2, 2, 0),
            from_roots_event(0, 0, 0),
            from_roots_event(2, 3, 1),
            from_roots_event(0, 1, 1),
        ]
    );
    // A shared successor is visited only once, via the first root
    let graph = VecGraph::from_arcs([(0, 2), (1, 2)]);
    assert_eq!(
        bfs_order_from_roots(&graph, &[0, 1])?,
        [
            from_roots_event(0, 0, 0),
            from_roots_event(1, 1, 0),
            from_roots_event(0, 2, 1),
        ]
    );
    // A root reachable from another root is returned as a root
    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    assert_eq!(
        bfs_order_from_roots(&graph, &[0, 1])?,
        [
            from_roots_event(0, 0, 0),
            from_roots_event(1, 1, 0),
            from_roots_event(1, 2, 1),
        ]
    );
    // Duplicate roots do not cause double visits of other nodes
    let events = bfs_order_from_roots(&graph, &[0, 0])?;
    let non_root: Vec<_> = events.iter().filter(|e| e.node != 0).copied().collect();
    assert_eq!(
        non_root,
        [from_roots_event(0, 1, 1), from_roots_event(1, 2, 2)]
    );
    // Three roots and a deeper graph
    let graph = VecGraph::from_arcs([(0, 3), (1, 4), (2, 5), (3, 6), (4, 7)]);
    let events = bfs_order_from_roots(&graph, &[0, 1, 2])?;
    let node_distances: Vec<_> = events.iter().map(|e| (e.node, e.distance)).collect();
    assert_eq!(
        node_distances,
        [
            (0, 0),
            (1, 0),
            (2, 0),
            (3, 1),
            (4, 1),
            (5, 1),
            (6, 2),
            (7, 2)
        ]
    );
    Ok(())
}

#[test]
fn test_from_roots_empty_roots_error() {
    let graph = VecGraph::from_arcs([(0, 1)]);
    let mut visit = webgraph::visits::breadth_first::Seq::new(&graph);
    assert!(visit.iter_from_roots(std::iter::empty::<usize>()).is_err());
}

#[test]
fn test_from_roots_distances_vs_callback() -> Result<()> {
    // BfsOrderFromRoots gives the same distances as the callback-based visit
    let graph = test_graph();
    let mut distances = vec![usize::MAX; graph.num_nodes()];
    for event in bfs_order_from_roots(&graph, &[0])? {
        distances[event.node] = event.distance;
    }
    let mut expected = vec![usize::MAX; graph.num_nodes()];
    webgraph::visits::breadth_first::Seq::new(&graph)
        .visit([0], |event| {
            if let EventPred::Visit { node, distance, .. } = event {
                expected[node] = distance;
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(distances, expected);
    Ok(())
}
