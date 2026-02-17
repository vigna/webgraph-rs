/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use no_break::NoBreak;
use std::ops::ControlFlow::Continue;
use std::sync::atomic::{AtomicUsize, Ordering};
use sync_cell_slice::SyncSlice;
use webgraph::prelude::*;
use webgraph::utils::Granularity;
use webgraph::{
    prelude::{BvGraph, VecGraph},
    traits::{RandomAccessGraph, SequentialLabeling},
    visits::{Parallel, Sequential},
};

fn correct_distances<G: RandomAccessGraph>(graph: &G, start: usize) -> Vec<usize> {
    let mut distances = Vec::new();
    let mut visits = vec![-1; graph.num_nodes()];
    let mut current_frontier = Vec::new();
    let mut next_frontier = Vec::new();

    for i in 0..graph.num_nodes() {
        let start_node = (i + start) % graph.num_nodes();
        if visits[start_node] != -1 {
            continue;
        }
        let mut distance = 1;
        visits[start_node] = 0;
        current_frontier.push(start_node);

        while !current_frontier.is_empty() {
            for node in current_frontier {
                for succ in graph.successors(node) {
                    if visits[succ] == -1 {
                        next_frontier.push(succ);
                        visits[succ] = distance;
                    }
                }
            }
            current_frontier = next_frontier;
            next_frontier = Vec::new();
            distance += 1;
        }
    }

    for dist in visits {
        distances.push(dist.try_into().unwrap());
    }

    distances
}

fn into_non_atomic(v: Vec<AtomicUsize>) -> Vec<usize> {
    let mut res = Vec::new();
    for element in v {
        res.push(element.load(Ordering::Relaxed));
    }
    res
}

macro_rules! test_bfv_algo_seq {
    ($bfv:expr, $name:ident) => {
        mod $name {
            use super::*;
            use std::collections::BTreeMap;

            #[test]
            fn test_simple_graph() -> Result<()> {
                let arcs = vec![
                    (0, 1),
                    (0, 2),
                    (1, 3),
                    (1, 4),
                    (1, 5),
                    (2, 4),
                    (3, 6),
                    (4, 6),
                    (5, 6),
                    (5, 7),
                    (6, 2),
                ];
                let graph = VecGraph::from_arcs(arcs);
                let mut visit = $bfv(&graph);
                let distances: Vec<AtomicUsize> = (0..graph.num_nodes())
                    .map(|_| AtomicUsize::new(0))
                    .collect();
                let expected_distances = correct_distances(&graph, 0);

                for root in 0..graph.num_nodes() {
                    visit
                        .visit([root], |event| {
                            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                                distances[node].store(distance, Ordering::Relaxed);
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                }
                let actual_distances = into_non_atomic(distances);

                assert_eq!(actual_distances, expected_distances);

                Ok(())
            }

            #[test]
            fn test_nontrivial_seed() -> Result<()> {
                let arcs = vec![(0, 1), (1, 2), (3, 2)];
                let graph = VecGraph::from_arcs(arcs);
                let mut visit = $bfv(&graph);
                let mut distances = vec![0; graph.num_nodes()];

                visit
                    .visit([0, 3], |event| {
                        if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                            distances[node] = distance;
                        }
                        Continue(())
                    })
                    .continue_value_no_break();

                assert_eq!(distances, [0, 1, 1, 0]);

                Ok(())
            }

            #[test]
            fn test_cnr_2000() -> Result<()> {
                let graph = BvGraph::with_basename("../data/cnr-2000").load()?;
                let mut visit = $bfv(&graph);
                let distances: Vec<AtomicUsize> = (0..graph.num_nodes())
                    .map(|_| AtomicUsize::new(0))
                    .collect();
                let expected_distances = correct_distances(&graph, 10000);

                for i in 0..graph.num_nodes() {
                    let root = (i + 10000) % graph.num_nodes();
                    visit
                        .visit([root], |event| {
                            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                                distances[node].store(distance, Ordering::Relaxed);
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                }

                let actual_distances = into_non_atomic(distances);

                assert_eq!(actual_distances, expected_distances);

                Ok(())
            }

            #[test]
            fn test_distance_event_cnr_2000_single_root() -> Result<()> {
                let graph = BvGraph::with_basename("../data/cnr-2000").load()?;
                let mut visit = $bfv(&graph);
                let mut distance_to_quantity: BTreeMap<usize, usize> = BTreeMap::new();
                let mut expected_distance_to_quantity: BTreeMap<usize, usize> = BTreeMap::new();

                visit
                    .visit([0], |event| {
                        if let breadth_first::EventPred::Visit { distance, .. } = event {
                            *expected_distance_to_quantity.entry(distance).or_insert(0) += 1;
                        }
                        if let breadth_first::EventPred::FrontierSize { distance, size } = event {
                            *distance_to_quantity.entry(distance).or_insert(0) += size;
                        }
                        Continue(())
                    })
                    .continue_value_no_break();

                assert_eq!(distance_to_quantity, expected_distance_to_quantity);

                Ok(())
            }

            #[test]
            fn test_distance_event_cnr_2000_multi_root() -> Result<()> {
                let graph = BvGraph::with_basename("../data/cnr-2000").load()?;
                let mut visit = $bfv(&graph);
                let mut distance_to_quantity: BTreeMap<usize, usize> = BTreeMap::new();
                let mut expected_distance_to_quantity: BTreeMap<usize, usize> = BTreeMap::new();

                visit
                    .visit([0, graph.num_nodes() / 2, graph.num_nodes() - 1], |event| {
                        if let breadth_first::EventPred::Visit { distance, .. } = event {
                            *expected_distance_to_quantity.entry(distance).or_insert(0) += 1;
                        }
                        if let breadth_first::EventPred::FrontierSize { distance, size } = event {
                            *distance_to_quantity.entry(distance).or_insert(0) += size;
                        }
                        Continue(())
                    })
                    .continue_value_no_break();

                assert_eq!(distance_to_quantity, expected_distance_to_quantity);

                Ok(())
            }
        }
    };
}

macro_rules! test_bfv_algo_par {
    ($bfv:expr, $name:ident) => {
        mod $name {
            use super::*;
            use std::collections::BTreeMap;
            use std::sync::Mutex;

            #[test]
            fn test_simple_graph() -> Result<()> {
                let arcs = vec![
                    (0, 1),
                    (0, 2),
                    (1, 3),
                    (1, 4),
                    (1, 5),
                    (2, 4),
                    (3, 6),
                    (4, 6),
                    (5, 6),
                    (5, 7),
                    (6, 2),
                ];
                let graph = VecGraph::from_arcs(arcs);
                let mut visit = $bfv(&graph);
                let distances: Vec<AtomicUsize> = (0..graph.num_nodes())
                    .map(|_| AtomicUsize::new(0))
                    .collect();
                let expected_distances = correct_distances(&graph, 0);

                for root in 0..graph.num_nodes() {
                    visit
                        .par_visit([root], |event| {
                            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                                distances[node].store(distance, Ordering::Relaxed);
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                }
                let actual_distances = into_non_atomic(distances);

                assert_eq!(actual_distances, expected_distances);

                Ok(())
            }

            #[test]
            fn test_nontrivial_seed() -> Result<()> {
                let arcs = vec![(0, 1), (1, 2), (3, 2)];
                let graph = VecGraph::from_arcs(arcs);
                let mut visit = $bfv(&graph);
                let mut distances = vec![0; graph.num_nodes()];
                let sync_distances = distances.as_sync_slice();

                visit
                    .par_visit([0, 3], |event| {
                        if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                            unsafe { sync_distances[node].set(distance) };
                        }
                        Continue(())
                    })
                    .continue_value_no_break();

                assert_eq!(distances, [0, 1, 1, 0]);

                Ok(())
            }

            #[test]
            fn test_cnr_2000() -> Result<()> {
                let graph = BvGraph::with_basename("../data/cnr-2000").load()?;
                let mut visit = $bfv(&graph);
                let distances: Vec<AtomicUsize> = (0..graph.num_nodes())
                    .map(|_| AtomicUsize::new(0))
                    .collect();
                let expected_distances = correct_distances(&graph, 10000);

                for i in 0..graph.num_nodes() {
                    let root = (i + 10000) % graph.num_nodes();
                    visit
                        .par_visit([root], |event| {
                            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                                distances[node].store(distance, Ordering::Relaxed);
                            }
                            Continue(())
                        })
                        .continue_value_no_break();
                }

                let actual_distances = into_non_atomic(distances);

                assert_eq!(actual_distances, expected_distances);

                Ok(())
            }

            #[test]
            fn test_distance_event_cnr_2000_single_root() -> Result<()> {
                let graph = BvGraph::with_basename("../data/cnr-2000").load()?;
                let mut visit = $bfv(&graph);

                let distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());
                let expected_distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());

                visit
                    .par_visit([0], |event| {
                        if let breadth_first::EventPred::Visit { distance, .. } = event {
                            *expected_distance_to_quantity
                                .lock()
                                .unwrap()
                                .entry(distance)
                                .or_insert(0) += 1;
                        }
                        if let breadth_first::EventPred::FrontierSize { distance, size } = event {
                            *distance_to_quantity
                                .lock()
                                .unwrap()
                                .entry(distance)
                                .or_insert(0) += size;
                        }
                        Continue(())
                    })
                    .continue_value_no_break();

                assert_eq!(
                    distance_to_quantity.into_inner().unwrap(),
                    expected_distance_to_quantity.into_inner().unwrap()
                );

                Ok(())
            }

            #[test]
            fn test_distance_event_cnr_2000_multi_root() -> Result<()> {
                let graph = BvGraph::with_basename("../data/cnr-2000").load()?;
                let mut visit = $bfv(&graph);

                let distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());
                let expected_distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());

                visit
                    .par_visit([0, graph.num_nodes() / 2, graph.num_nodes() - 1], |event| {
                        if let breadth_first::EventPred::Visit { distance, .. } = event {
                            *expected_distance_to_quantity
                                .lock()
                                .unwrap()
                                .entry(distance)
                                .or_insert(0) += 1;
                        }
                        if let breadth_first::EventPred::FrontierSize { distance, size } = event {
                            *distance_to_quantity
                                .lock()
                                .unwrap()
                                .entry(distance)
                                .or_insert(0) += size;
                        }
                        Continue(())
                    })
                    .continue_value_no_break();

                assert_eq!(
                    distance_to_quantity.into_inner().unwrap(),
                    expected_distance_to_quantity.into_inner().unwrap()
                );

                Ok(())
            }
        }
    };
}

test_bfv_algo_seq!(webgraph::visits::breadth_first::Seq::<_>::new, sequential);
test_bfv_algo_par!(
    |g| {
        webgraph::visits::breadth_first::ParFairPred::with_granularity(g, Granularity::Nodes(32))
    },
    parallel_fair_pred
);
test_bfv_algo_par!(
    |g| { webgraph::visits::breadth_first::ParLowMem::with_granularity(g, Granularity::Nodes(32)) },
    parallel_fast_callback
);

#[test]
fn test_start() -> Result<()> {
    // 4 -> 0 -> 2
    //       `-> 3
    // 1 -> 5
    let mut graph = webgraph::graphs::vec_graph::VecGraph::new();

    for i in 0..=5 {
        graph.add_node(i);
    }
    graph.add_arc(4, 0);
    graph.add_arc(0, 2);
    graph.add_arc(0, 3);
    graph.add_arc(1, 5);

    let order: Vec<_> = webgraph::visits::breadth_first::Seq::new(&graph)
        .into_iter()
        .map(|x| x.node)
        .collect();

    assert_eq!(order, vec![0, 2, 3, 1, 5, 4]);

    Ok(())
}

#[test]
fn test_start_orphan() -> Result<()> {
    // 0 -> 4 -> 2
    //       `-> 3
    // 1 -> 5
    let mut graph = webgraph::graphs::vec_graph::VecGraph::new();

    for i in 0..=5 {
        graph.add_node(i);
    }
    graph.add_arc(0, 4);
    graph.add_arc(4, 2);
    graph.add_arc(4, 3);
    graph.add_arc(1, 5);

    let order: Vec<_> = webgraph::visits::breadth_first::Seq::new(&graph)
        .into_iter()
        .map(|x| x.node)
        .collect();

    assert_eq!(order, vec![0, 4, 2, 3, 1, 5]);

    Ok(())
}

// ── BfsOrder tests ──────────────────────────────────────────────────────

#[test]
fn test_bfs_order_path() {
    // 0 → 1 → 2 → 3
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 4);
    // All nodes in a single component rooted at 0
    for e in &events {
        assert_eq!(e.root, 0);
    }
    assert_eq!(
        events,
        vec![
            breadth_first::IterEvent {
                root: 0,
                parent: 0,
                node: 0,
                distance: 0
            },
            breadth_first::IterEvent {
                root: 0,
                parent: 0,
                node: 1,
                distance: 1
            },
            breadth_first::IterEvent {
                root: 0,
                parent: 1,
                node: 2,
                distance: 2
            },
            breadth_first::IterEvent {
                root: 0,
                parent: 2,
                node: 3,
                distance: 3
            },
        ]
    );
}

#[test]
fn test_bfs_order_tree() {
    //       0
    //      / \
    //     1   2
    //    / \   \
    //   3   4   5
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (1, 4), (2, 5)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 6);
    // Check distances
    let distances: Vec<_> = events.iter().map(|e| (e.node, e.distance)).collect();
    assert_eq!(
        distances,
        vec![(0, 0), (1, 1), (2, 1), (3, 2), (4, 2), (5, 2)]
    );
    // Check parents
    assert_eq!(events[0].parent, 0); // root is its own parent
    assert_eq!(events[1].parent, 0); // 1's parent is 0
    assert_eq!(events[2].parent, 0); // 2's parent is 0
    assert_eq!(events[3].parent, 1); // 3's parent is 1
    assert_eq!(events[4].parent, 1); // 4's parent is 1
    assert_eq!(events[5].parent, 2); // 5's parent is 2
}

#[test]
fn test_bfs_order_cycle() {
    // 0 → 1 → 2 → 0 (cycle)
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    // All three nodes are visited exactly once
    assert_eq!(events.len(), 3);
    let nodes: Vec<_> = events.iter().map(|e| e.node).collect();
    assert_eq!(nodes, vec![0, 1, 2]);
    assert_eq!(events[0].distance, 0);
    assert_eq!(events[1].distance, 1);
    assert_eq!(events[2].distance, 2);
}

#[test]
fn test_bfs_order_diamond() {
    //   0
    //  / \
    // 1   2
    //  \ /
    //   3
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 4);
    // Node 3 should be visited exactly once, at distance 2
    let node3 = events.iter().find(|e| e.node == 3).unwrap();
    assert_eq!(node3.distance, 2);
    // Its parent should be 1 (discovered first via BFS)
    assert_eq!(node3.parent, 1);
}

#[test]
fn test_bfs_order_disconnected() {
    // Component 1: 0 → 1
    // Component 2: 2 → 3
    // Isolated: 4
    let mut graph = VecGraph::new();
    graph.add_node(4);
    graph.add_arc(0, 1);
    graph.add_arc(2, 3);

    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 5);

    // First component: root=0
    assert_eq!(
        events[0],
        breadth_first::IterEvent {
            root: 0,
            parent: 0,
            node: 0,
            distance: 0
        }
    );
    assert_eq!(
        events[1],
        breadth_first::IterEvent {
            root: 0,
            parent: 0,
            node: 1,
            distance: 1
        }
    );
    // Second component: root=2
    assert_eq!(
        events[2],
        breadth_first::IterEvent {
            root: 2,
            parent: 2,
            node: 2,
            distance: 0
        }
    );
    assert_eq!(
        events[3],
        breadth_first::IterEvent {
            root: 2,
            parent: 2,
            node: 3,
            distance: 1
        }
    );
    // Isolated node: root=4
    assert_eq!(
        events[4],
        breadth_first::IterEvent {
            root: 4,
            parent: 4,
            node: 4,
            distance: 0
        }
    );
}

#[test]
fn test_bfs_order_self_loop() {
    // 0 → 0, 0 → 1, 1 → 1
    let graph = VecGraph::from_arcs([(0, 0), (0, 1), (1, 1)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].node, 0);
    assert_eq!(events[0].distance, 0);
    assert_eq!(events[1].node, 1);
    assert_eq!(events[1].distance, 1);
}

#[test]
fn test_bfs_order_single_node() {
    let mut graph = VecGraph::new();
    graph.add_node(0);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        breadth_first::IterEvent {
            root: 0,
            parent: 0,
            node: 0,
            distance: 0
        }
    );
}

#[test]
fn test_bfs_order_exact_size() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let mut iter = (&mut visit).into_iter();

    assert_eq!(iter.len(), 4);
    iter.next();
    assert_eq!(iter.len(), 3);
    iter.next();
    assert_eq!(iter.len(), 2);
    iter.next();
    assert_eq!(iter.len(), 1);
    iter.next();
    assert_eq!(iter.len(), 0);
    assert!(iter.next().is_none());
}

#[test]
fn test_bfs_order_star() {
    // 0 → {1, 2, 3, 4}
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (0, 3), (0, 4)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 5);
    assert_eq!(events[0].distance, 0);
    for e in &events[1..] {
        assert_eq!(e.distance, 1);
        assert_eq!(e.parent, 0);
    }
}

#[test]
fn test_bfs_order_reverse_star() {
    // {1, 2, 3} → 0, nodes 1..3 have no incoming from 0
    let mut graph = VecGraph::new();
    graph.add_node(3);
    graph.add_arc(1, 0);
    graph.add_arc(2, 0);
    graph.add_arc(3, 0);

    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 4);
    // BFS starts from node 0 (no outgoing), then finds 1, 2, 3 as new roots
    assert_eq!(
        events[0],
        breadth_first::IterEvent {
            root: 0,
            parent: 0,
            node: 0,
            distance: 0
        }
    );
    assert_eq!(events[1].root, 1);
    assert_eq!(events[1].distance, 0);
    assert_eq!(events[2].root, 2);
    assert_eq!(events[2].distance, 0);
    assert_eq!(events[3].root, 3);
    assert_eq!(events[3].distance, 0);
}

#[test]
fn test_bfs_order_multi_level() {
    // 0 → 1 → 3
    //  \→ 2 → 4 → 5
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 4), (4, 5)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&graph)).into_iter().collect();

    assert_eq!(events.len(), 6);
    let dist_map: Vec<_> = events.iter().map(|e| (e.node, e.distance)).collect();
    assert_eq!(
        dist_map,
        vec![(0, 0), (1, 1), (2, 1), (3, 2), (4, 2), (5, 3)]
    );
}

// ── BfsOrderFromRoots tests ────────────────────────────────────────────

#[test]
fn test_from_roots_single_root() -> Result<()> {
    // 0 → 1 → 2
    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0])?.collect();

    assert_eq!(events.len(), 3);
    assert_eq!(
        events,
        vec![
            breadth_first::IterFromRootsEvent {
                parent: 0,
                node: 0,
                distance: 0
            },
            breadth_first::IterFromRootsEvent {
                parent: 0,
                node: 1,
                distance: 1
            },
            breadth_first::IterFromRootsEvent {
                parent: 1,
                node: 2,
                distance: 2
            },
        ]
    );
    Ok(())
}

#[test]
fn test_from_roots_two_roots() -> Result<()> {
    // 0 → 2, 1 → 3
    let graph = VecGraph::from_arcs([(0, 2), (1, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0, 1])?.collect();

    assert_eq!(events.len(), 4);
    // Roots first, at distance 0
    assert_eq!(
        events[0],
        breadth_first::IterFromRootsEvent {
            parent: 0,
            node: 0,
            distance: 0
        }
    );
    assert_eq!(
        events[1],
        breadth_first::IterFromRootsEvent {
            parent: 1,
            node: 1,
            distance: 0
        }
    );
    // Then their successors at distance 1
    assert_eq!(
        events[2],
        breadth_first::IterFromRootsEvent {
            parent: 0,
            node: 2,
            distance: 1
        }
    );
    assert_eq!(
        events[3],
        breadth_first::IterFromRootsEvent {
            parent: 1,
            node: 3,
            distance: 1
        }
    );
    Ok(())
}

#[test]
fn test_from_roots_shared_successor() -> Result<()> {
    // 0 → 2, 1 → 2
    // Node 2 should be visited only once (via the first root that discovers it).
    let graph = VecGraph::from_arcs([(0, 2), (1, 2)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0, 1])?.collect();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].node, 0);
    assert_eq!(events[1].node, 1);
    assert_eq!(events[2].node, 2);
    assert_eq!(events[2].distance, 1);
    // Discovered via root 0 (first in queue)
    assert_eq!(events[2].parent, 0);
    Ok(())
}

#[test]
fn test_from_roots_root_reachable_from_other_root() -> Result<()> {
    // 0 → 1, and both 0 and 1 are roots.
    // Node 1 should be returned as a root (distance 0), not as a successor.
    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0, 1])?.collect();

    assert_eq!(events.len(), 3);
    // Both roots at distance 0
    assert_eq!(
        events[0],
        breadth_first::IterFromRootsEvent {
            parent: 0,
            node: 0,
            distance: 0
        }
    );
    assert_eq!(
        events[1],
        breadth_first::IterFromRootsEvent {
            parent: 1,
            node: 1,
            distance: 0
        }
    );
    // Node 2 at distance 1 (successor of 1, but 1 is already visited as root
    // so it won't be re-discovered from 0)
    assert_eq!(
        events[2],
        breadth_first::IterFromRootsEvent {
            parent: 1,
            node: 2,
            distance: 1
        }
    );
    Ok(())
}

#[test]
fn test_from_roots_unreachable_nodes() -> Result<()> {
    // 0 → 1, isolated node 2
    // Starting from root 0, node 2 should not be visited.
    let mut graph = VecGraph::new();
    graph.add_node(2);
    graph.add_arc(0, 1);

    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0])?.collect();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].node, 0);
    assert_eq!(events[1].node, 1);
    Ok(())
}

#[test]
fn test_from_roots_empty_roots_error() {
    let graph = VecGraph::from_arcs([(0, 1)]);
    let mut visit = breadth_first::Seq::new(&graph);
    assert!(visit.iter_from_roots(std::iter::empty::<usize>()).is_err());
}

#[test]
fn test_from_roots_cycle() -> Result<()> {
    // 0 → 1 → 2 → 0
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0])?.collect();

    assert_eq!(events.len(), 3);
    let nodes: Vec<_> = events.iter().map(|e| e.node).collect();
    assert_eq!(nodes, vec![0, 1, 2]);
    assert_eq!(events[0].distance, 0);
    assert_eq!(events[1].distance, 1);
    assert_eq!(events[2].distance, 2);
    Ok(())
}

#[test]
fn test_from_roots_diamond() -> Result<()> {
    //   0
    //  / \
    // 1   2
    //  \ /
    //   3
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0])?.collect();

    assert_eq!(events.len(), 4);
    let node3 = events.iter().find(|e| e.node == 3).unwrap();
    assert_eq!(node3.distance, 2);
    assert_eq!(node3.parent, 1);
    Ok(())
}

#[test]
fn test_from_roots_self_loop() -> Result<()> {
    // 0 → 0, 0 → 1
    let graph = VecGraph::from_arcs([(0, 0), (0, 1)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0])?.collect();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].node, 0);
    assert_eq!(events[0].distance, 0);
    assert_eq!(events[1].node, 1);
    assert_eq!(events[1].distance, 1);
    Ok(())
}

#[test]
fn test_from_roots_single_node_no_arcs() -> Result<()> {
    let mut graph = VecGraph::new();
    graph.add_node(0);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0])?.collect();

    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        breadth_first::IterFromRootsEvent {
            parent: 0,
            node: 0,
            distance: 0
        }
    );
    Ok(())
}

#[test]
fn test_from_roots_three_roots_deeper_graph() -> Result<()> {
    // 0 → 3 → 6
    // 1 → 4 → 7
    // 2 → 5
    let graph = VecGraph::from_arcs([(0, 3), (1, 4), (2, 5), (3, 6), (4, 7)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0, 1, 2])?.collect();

    assert_eq!(events.len(), 8);
    // Roots at distance 0
    assert_eq!(events[0].node, 0);
    assert_eq!(events[0].distance, 0);
    assert_eq!(events[1].node, 1);
    assert_eq!(events[1].distance, 0);
    assert_eq!(events[2].node, 2);
    assert_eq!(events[2].distance, 0);
    // Distance 1
    let dist1: Vec<_> = events
        .iter()
        .filter(|e| e.distance == 1)
        .map(|e| e.node)
        .collect();
    assert_eq!(dist1, vec![3, 4, 5]);
    // Distance 2
    let dist2: Vec<_> = events
        .iter()
        .filter(|e| e.distance == 2)
        .map(|e| e.node)
        .collect();
    assert_eq!(dist2, vec![6, 7]);
    Ok(())
}

#[test]
fn test_from_roots_distances_vs_callback() -> Result<()> {
    // Verify BfsOrderFromRoots gives the same distances as the callback-based visit.
    let graph = VecGraph::from_arcs([
        (0, 1),
        (0, 2),
        (1, 3),
        (1, 4),
        (2, 4),
        (2, 5),
        (3, 6),
        (4, 6),
        (5, 7),
        (6, 7),
    ]);

    // Callback-based distances
    let mut cb_distances = vec![0; graph.num_nodes()];
    let mut visit = breadth_first::Seq::new(&graph);
    visit
        .visit([0], |event| {
            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                cb_distances[node] = distance;
            }
            Continue(())
        })
        .continue_value_no_break();

    // Iterator-based distances
    let mut iter_distances = vec![0; graph.num_nodes()];
    let mut visit2 = breadth_first::Seq::new(&graph);
    for event in visit2.iter_from_roots([0])? {
        iter_distances[event.node] = event.distance;
    }

    assert_eq!(cb_distances, iter_distances);
    Ok(())
}

#[test]
fn test_bfs_order_distances_vs_callback() -> Result<()> {
    // Verify BfsOrder gives the same distances as the callback-based visit
    // when visiting all nodes.
    let graph = VecGraph::from_arcs([
        (0, 1),
        (0, 2),
        (1, 3),
        (1, 4),
        (2, 4),
        (2, 5),
        (3, 6),
        (4, 6),
        (5, 7),
        (6, 7),
    ]);

    // Callback-based distances (visit from all roots)
    let mut cb_distances = vec![0; graph.num_nodes()];
    let mut visit = breadth_first::Seq::new(&graph);
    for root in 0..graph.num_nodes() {
        visit
            .visit([root], |event| {
                if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                    cb_distances[node] = distance;
                }
                Continue(())
            })
            .continue_value_no_break();
    }

    // Iterator-based distances
    let mut iter_distances = vec![0; graph.num_nodes()];
    let mut visit2 = breadth_first::Seq::new(&graph);
    for event in &mut visit2 {
        iter_distances[event.node] = event.distance;
    }

    assert_eq!(cb_distances, iter_distances);
    Ok(())
}

#[test]
fn test_from_roots_duplicate_roots() -> Result<()> {
    // Passing the same root twice should not cause double visits.
    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let events: Vec<_> = visit.iter_from_roots([0, 0])?.collect();

    // Node 0 appears twice as a root (it is enqueued twice)
    // but 1 and 2 should still be visited at the correct distance.
    let non_root: Vec<_> = events.iter().filter(|e| e.node != 0).collect();
    assert_eq!(non_root.len(), 2);
    assert_eq!(non_root[0].node, 1);
    assert_eq!(non_root[0].distance, 1);
    assert_eq!(non_root[1].node, 2);
    assert_eq!(non_root[1].distance, 2);
    Ok(())
}
