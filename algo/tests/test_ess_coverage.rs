/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_progress_logger::no_logging;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph_algo::distances::exact_sum_sweep::{self, Level};

/// Directed graph with a cycle and a sink: 0→1→2→3→0, 2→4.
fn directed_graph() -> (VecGraph, VecGraph) {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);
    (graph, transpose)
}

/// Symmetric graph with two components: a triangle {0,1,2} and an edge {3,4}.
fn symm_graph() -> VecGraph {
    VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ])
}

#[test]
fn test_ess_diameter_only() -> Result<()> {
    let (graph, transpose) = directed_graph();
    let result = exact_sum_sweep::Diameter::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.diameter, 4);
    Ok(())
}

#[test]
fn test_ess_radius_only() -> Result<()> {
    let (graph, transpose) = directed_graph();
    let result = exact_sum_sweep::Radius::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.radius, 3);
    Ok(())
}

#[test]
fn test_ess_all_forward() -> Result<()> {
    let (graph, transpose) = directed_graph();
    let result = exact_sum_sweep::AllForward::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.diameter, 4);
    assert_eq!(result.radius, 3);
    assert_eq!(result.forward_eccentricities.as_ref(), &[3, 3, 3, 4, 0]);
    Ok(())
}

#[test]
fn test_ess_radius_diameter() -> Result<()> {
    let (graph, transpose) = directed_graph();
    let result = exact_sum_sweep::RadiusDiameter::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.diameter, 4);
    assert_eq!(result.radius, 3);
    Ok(())
}

#[test]
fn test_ess_all_symm() {
    let graph = symm_graph();
    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);
    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
    assert_eq!(result.eccentricities.as_ref(), &[1, 1, 1, 1, 1]);
}

#[test]
fn test_ess_all_forward_symm() {
    let graph = symm_graph();
    let result = exact_sum_sweep::AllForward::run_symm(&graph, no_logging![]);
    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_diameter_symm() {
    let graph = symm_graph();
    let result = exact_sum_sweep::Diameter::run_symm(&graph, no_logging![]);
    assert_eq!(result.diameter, 1);
}

#[test]
fn test_ess_radius_symm() {
    let graph = symm_graph();
    let result = exact_sum_sweep::Radius::run_symm(&graph, no_logging![]);
    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_radius_diameter_symm() {
    let graph = symm_graph();
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
    // Star graph: 0 -> 1, 0 -> 2, 0 -> 3 (all symmetric)
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (0, 2), (2, 0), (0, 3), (3, 0)]);

    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);
    assert_eq!(result.diameter, 2);
    assert_eq!(result.radius, 1);
    assert_eq!(result.radial_vertex, 0);
}
