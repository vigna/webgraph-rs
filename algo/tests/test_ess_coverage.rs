/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

mod common;

use common::{TEST_GRAPH_ARCS, ess, graph_and_transpose, symm_graph};
use webgraph::graphs::vec_graph::VecGraph;
use webgraph_algo::distances::exact_sum_sweep::{
    All, AllForward, Diameter, Radius, RadiusDiameter,
};

/// The test graph and its transpose.
fn directed_graph() -> (VecGraph, VecGraph) {
    graph_and_transpose(8, TEST_GRAPH_ARCS)
}

/// Symmetric graph with two components: a triangle {0,1,2} and an edge {3,4}.
fn triangle_and_edge() -> VecGraph {
    symm_graph(5, [(0, 1), (1, 2), (2, 0), (3, 4)])
}

/// Symmetric path graph: 0 - 1 - 2 - 3.
fn symm_path() -> VecGraph {
    symm_graph(4, [(0, 1), (1, 2), (2, 3)])
}

/// Symmetric star graph: 0 is the centre, connected to 1, 2, 3.
fn symm_star() -> VecGraph {
    symm_graph(4, [(0, 1), (0, 2), (0, 3)])
}

#[test]
fn test_ess_directed() {
    let (graph, transpose) = directed_graph();
    for result in ess::run::<Diameter>(&graph, &transpose, || None) {
        assert_eq!(result.diameter, 3);
    }
    for result in ess::run::<Radius>(&graph, &transpose, || None) {
        assert_eq!(result.radius, 2);
    }
    for result in ess::run::<RadiusDiameter>(&graph, &transpose, || None) {
        assert_eq!((result.radius, result.diameter), (2, 3));
    }
    for result in ess::run::<AllForward>(&graph, &transpose, || None) {
        assert_eq!((result.radius, result.diameter), (2, 3));
        assert_eq!(
            result.forward_eccentricities.as_ref(),
            &[3, 3, 2, 3, 2, 3, 2, 0]
        );
    }
}

#[test]
fn test_ess_cycle() {
    let (graph, transpose) = graph_and_transpose(4, [(0, 1), (1, 2), (2, 3), (3, 0)]);
    for result in ess::run::<Diameter>(&graph, &transpose, || None) {
        assert_eq!(result.diameter, 3);
    }
    for result in ess::run::<AllForward>(&graph, &transpose, || None) {
        assert_eq!((result.radius, result.diameter), (3, 3));
        // All nodes in a cycle have equal eccentricity
        assert_eq!(result.forward_eccentricities.as_ref(), &[3, 3, 3, 3]);
    }
}

#[test]
fn test_ess_symm() {
    let graph = triangle_and_edge();
    for result in ess::run_symm::<All>(&graph) {
        assert_eq!((result.radius, result.diameter), (1, 1));
        assert_eq!(result.eccentricities.as_ref(), &[1, 1, 1, 1, 1]);
    }
    for result in ess::run_symm::<AllForward>(&graph) {
        assert_eq!((result.radius, result.diameter), (1, 1));
    }
    for result in ess::run_symm::<Diameter>(&graph) {
        assert_eq!(result.diameter, 1);
    }
    for result in ess::run_symm::<Radius>(&graph) {
        assert_eq!(result.radius, 1);
    }
    for result in ess::run_symm::<RadiusDiameter>(&graph) {
        assert_eq!((result.radius, result.diameter), (1, 1));
    }

    for result in ess::run_symm::<All>(&symm_path()) {
        assert_eq!((result.radius, result.diameter), (2, 3));
    }
    for result in ess::run_symm::<All>(&symm_star()) {
        assert_eq!((result.radius, result.diameter), (1, 2));
        assert_eq!(result.radial_vertex, 0);
    }
}

/// Runs all levels on a symmetric graph through both `run_symm`
/// (symmetric path) and `run` with the graph as its own transpose
/// (directed path), checking that the results match.
#[test]
fn test_ess_symmetric_vs_non_symmetric() {
    for graph in [triangle_and_edge(), symm_path(), symm_star()] {
        let symm = ess::run_symm::<All>(&graph);
        let dir = ess::run::<All>(&graph, &graph, || None);
        let (radius, diameter) = (symm[0].radius, symm[0].diameter);
        for (symm, dir) in symm.iter().zip(&dir) {
            assert_eq!((symm.radius, symm.diameter), (radius, diameter));
            assert_eq!((dir.radius, dir.diameter), (radius, diameter));
            // Forward eccentricities from the directed run must match the
            // symmetric eccentricities.
            assert_eq!(
                dir.forward_eccentricities.as_ref(),
                symm.eccentricities.as_ref()
            );
        }

        for result in ess::run_symm::<RadiusDiameter>(&graph) {
            assert_eq!((result.radius, result.diameter), (radius, diameter));
        }
        for result in ess::run::<RadiusDiameter>(&graph, &graph, || None) {
            assert_eq!((result.radius, result.diameter), (radius, diameter));
        }
        for result in ess::run_symm::<Diameter>(&graph) {
            assert_eq!(result.diameter, diameter);
        }
        for result in ess::run::<Diameter>(&graph, &graph, || None) {
            assert_eq!(result.diameter, diameter);
        }
        for result in ess::run_symm::<Radius>(&graph) {
            assert_eq!(result.radius, radius);
        }
        for result in ess::run::<Radius>(&graph, &graph, || None) {
            assert_eq!(result.radius, radius);
        }
    }
}
