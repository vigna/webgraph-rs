/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_progress_logger::no_logging;
use webgraph::prelude::VecGraph;
use webgraph_algo::prelude::{is_acyclic, top_sort};

#[test]
fn test_top_sort() {
    assert_eq!(
        vec![0, 1, 2].into_boxed_slice(),
        top_sort(VecGraph::from_arcs([(1, 2), (0, 1)]), no_logging![])
    );

    assert_eq!(
        vec![0, 1, 2].into_boxed_slice(),
        top_sort(VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]), no_logging![])
    );

    assert_eq!(
        vec![0, 2, 1, 3].into_boxed_slice(),
        top_sort(
            VecGraph::from_arcs([(0, 1), (0, 2), (2, 3), (1, 3)]),
            no_logging![]
        )
    );
}

#[test]
fn test_acyclicity() {
    let graph = VecGraph::from_arcs([(1, 2), (0, 1)]);

    assert!(is_acyclic(&graph, no_logging![]));

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);

    assert!(!is_acyclic(&graph, no_logging![]));

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (2, 3), (1, 3)]);

    assert!(is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_acyclic_empty_graph() {
    let graph = VecGraph::from_arcs([] as [(usize, usize); 0]);
    assert!(is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_acyclic_dag() {
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 4)]);
    assert!(is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_not_acyclic_self_loop() {
    let graph = VecGraph::from_arcs([(0, 0)]);
    assert!(!is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_not_acyclic_mutual() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0)]);
    assert!(!is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_top_sort_single_node() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let ts = top_sort(g, no_logging![]);
    assert_eq!(ts.as_ref(), &[0]);
}

#[test]
fn test_top_sort_no_edges() {
    let mut g = VecGraph::new();
    for i in 0..5 {
        g.add_node(i);
    }
    let ts = top_sort(g, no_logging![]);
    assert_eq!(ts.len(), 5);
    let mut sorted = ts.to_vec();
    sorted.sort();
    assert_eq!(sorted, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_top_sort_diamond() {
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let ts = top_sort(graph, no_logging![]);
    let pos: std::collections::HashMap<usize, usize> =
        ts.iter().enumerate().map(|(i, &n)| (n, i)).collect();
    assert!(pos[&0] < pos[&1]);
    assert!(pos[&0] < pos[&2]);
    assert!(pos[&1] < pos[&3]);
    assert!(pos[&2] < pos[&3]);
}
