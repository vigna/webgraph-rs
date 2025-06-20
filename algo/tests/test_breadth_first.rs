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
use webgraph::utils::Granularity;
use webgraph::{
    prelude::{BvGraph, VecGraph},
    traits::{RandomAccessGraph, SequentialLabeling},
};
use webgraph_algo::prelude::*;
use webgraph_algo::thread_pool;

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
                    (0, 0),
                    (1, 0),
                    (1, 2),
                    (2, 1),
                    (2, 3),
                    (2, 4),
                    (2, 5),
                    (3, 4),
                    (4, 3),
                    (5, 5),
                    (5, 6),
                    (5, 7),
                    (5, 8),
                    (6, 7),
                    (8, 7),
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
                            if let breadth_first::EventPred::Unknown { node, distance, .. } = event
                            {
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
                        if let breadth_first::EventPred::Unknown { node, distance, .. } = event {
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
                            if let breadth_first::EventPred::Unknown { node, distance, .. } = event
                            {
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
                        if let breadth_first::EventPred::Unknown { distance, .. } = event {
                            *expected_distance_to_quantity.entry(distance).or_insert(0) += 1;
                        }
                        if let breadth_first::EventPred::FrontierSize { nodes, distance } = event {
                            *distance_to_quantity.entry(distance).or_insert(0) += nodes;
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
                        if let breadth_first::EventPred::Unknown { distance, .. } = event {
                            *expected_distance_to_quantity.entry(distance).or_insert(0) += 1;
                        }
                        if let breadth_first::EventPred::FrontierSize { nodes, distance } = event {
                            *distance_to_quantity.entry(distance).or_insert(0) += nodes;
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
                    (0, 0),
                    (1, 0),
                    (1, 2),
                    (2, 1),
                    (2, 3),
                    (2, 4),
                    (2, 5),
                    (3, 4),
                    (4, 3),
                    (5, 5),
                    (5, 6),
                    (5, 7),
                    (5, 8),
                    (6, 7),
                    (8, 7),
                ];
                let graph = VecGraph::from_arcs(arcs);
                let mut visit = $bfv(&graph);
                let distances: Vec<AtomicUsize> = (0..graph.num_nodes())
                    .map(|_| AtomicUsize::new(0))
                    .collect();
                let expected_distances = correct_distances(&graph, 0);

                let t = thread_pool![];

                for root in 0..graph.num_nodes() {
                    visit
                        .par_visit(
                            [root],
                            |event| {
                                if let breadth_first::EventPred::Unknown {
                                    node, distance, ..
                                } = event
                                {
                                    distances[node].store(distance, Ordering::Relaxed);
                                }
                                Continue(())
                            },
                            &t,
                        )
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
                    .par_visit(
                        [0, 3],
                        |event| {
                            if let breadth_first::EventPred::Unknown { node, distance, .. } = event
                            {
                                unsafe { sync_distances[node].set(distance) };
                            }
                            Continue(())
                        },
                        &thread_pool![],
                    )
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
                let t = thread_pool![];

                for i in 0..graph.num_nodes() {
                    let root = (i + 10000) % graph.num_nodes();
                    visit
                        .par_visit(
                            [root],
                            |event| {
                                if let breadth_first::EventPred::Unknown {
                                    node, distance, ..
                                } = event
                                {
                                    distances[node].store(distance, Ordering::Relaxed);
                                }
                                Continue(())
                            },
                            &t,
                        )
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
                let t = thread_pool![];

                let distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());
                let expected_distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());

                visit
                    .par_visit(
                        [0],
                        |event| {
                            if let breadth_first::EventPred::Unknown { distance, .. } = event {
                                *expected_distance_to_quantity
                                    .lock()
                                    .unwrap()
                                    .entry(distance)
                                    .or_insert(0) += 1;
                            }
                            if let breadth_first::EventPred::FrontierSize { nodes, distance } =
                                event
                            {
                                *distance_to_quantity
                                    .lock()
                                    .unwrap()
                                    .entry(distance)
                                    .or_insert(0) += nodes;
                            }
                            Continue(())
                        },
                        &t,
                    )
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
                let t = thread_pool![];

                let distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());
                let expected_distance_to_quantity: Mutex<BTreeMap<usize, usize>> =
                    Mutex::new(BTreeMap::new());

                visit
                    .par_visit(
                        [0, graph.num_nodes() / 2, graph.num_nodes() - 1],
                        |event| {
                            if let breadth_first::EventPred::Unknown { distance, .. } = event {
                                *expected_distance_to_quantity
                                    .lock()
                                    .unwrap()
                                    .entry(distance)
                                    .or_insert(0) += 1;
                            }
                            if let breadth_first::EventPred::FrontierSize { nodes, distance } =
                                event
                            {
                                *distance_to_quantity
                                    .lock()
                                    .unwrap()
                                    .entry(distance)
                                    .or_insert(0) += nodes;
                            }
                            Continue(())
                        },
                        &t,
                    )
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

test_bfv_algo_seq!(
    webgraph_algo::prelude::breadth_first::Seq::<_>::new,
    sequential
);
test_bfv_algo_par!(
    |g| {
        webgraph_algo::prelude::breadth_first::ParFairPred::with_granularity(
            g,
            Granularity::Nodes(32),
        )
    },
    parallel_fair_pred
);
test_bfv_algo_par!(
    |g| {
        webgraph_algo::prelude::breadth_first::ParLowMem::with_granularity(
            g,
            Granularity::Nodes(32),
        )
    },
    parallel_fast_callback
);
