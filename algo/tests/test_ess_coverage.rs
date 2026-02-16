/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_progress_logger::no_logging;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph_algo::distances::exact_sum_sweep::{self, Level};
use webgraph_algo::prelude::*;

#[test]
fn test_ess_diameter_only() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::Diameter::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.diameter, 4);
    Ok(())
}

#[test]
fn test_ess_radius_only() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::Radius::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.radius, 3);
    Ok(())
}

#[test]
fn test_ess_all_forward() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::AllForward::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.diameter, 4);
    assert_eq!(result.radius, 3);
    assert_eq!(result.forward_eccentricities.as_ref(), &[3, 3, 3, 4, 0]);
    Ok(())
}

#[test]
fn test_ess_radius_diameter() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::RadiusDiameter::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.diameter, 4);
    assert_eq!(result.radius, 3);
    Ok(())
}

#[test]
fn test_ess_all_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
    assert!(result.eccentricities.len() == 5);
}

#[test]
fn test_ess_all_forward_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::AllForward::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_diameter_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::Diameter::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
}

#[test]
fn test_ess_radius_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::Radius::run_symm(&graph, no_logging![]);

    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_radius_diameter_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::RadiusDiameter::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_symm_path() {
    // A path graph: 0 - 1 - 2 - 3
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 2)]);

    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 3);
    assert_eq!(result.radius, 2);
}

#[test]
fn test_acyclic_single_node() {
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
    // All nodes should be present
    let mut sorted = ts.to_vec();
    sorted.sort();
    assert_eq!(sorted, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_top_sort_diamond() {
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let ts = top_sort(graph, no_logging![]);
    // 0 must come before 1 and 2, 1 and 2 must come before 3
    let pos: std::collections::HashMap<usize, usize> =
        ts.iter().enumerate().map(|(i, &n)| (n, i)).collect();
    assert!(pos[&0] < pos[&1]);
    assert!(pos[&0] < pos[&2]);
    assert!(pos[&1] < pos[&3]);
    assert!(pos[&2] < pos[&3]);
}

#[test]
fn test_ess_diameter_cycle() {
    // Simple 4-cycle
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3)]);
    let result = exact_sum_sweep::Diameter::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.diameter, 3);
}

#[test]
fn test_ess_all_forward_cycle() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3)]);
    let result = exact_sum_sweep::AllForward::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.diameter, 3);
    assert_eq!(result.radius, 3);
    // All nodes in a cycle have equal eccentricity
    for &ecc in result.forward_eccentricities.iter() {
        assert_eq!(ecc, 3);
    }
}

#[test]
fn test_ess_all_star_graph() {
    // Star graph: 0 → 1, 0 → 2, 0 → 3 (all symmetric)
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (0, 2), (2, 0), (0, 3), (3, 0)]);

    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);
    assert_eq!(result.diameter, 2);
    assert_eq!(result.radius, 1);
    assert_eq!(result.radial_vertex, 0);
}
