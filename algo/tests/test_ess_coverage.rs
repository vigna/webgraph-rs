/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_progress_logger::no_logging;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph_algo::distances::exact_sum_sweep::{self, Level};

/// Canonical test graph (8 nodes, 11 arcs).
///
/// - Outdegree 0: node 7 (sink)
/// - Outdegree 1: nodes 2, 3, 4, 6
/// - Outdegree 2: nodes 0, 5
/// - Outdegree 3: node 1
/// - Indegree 0: node 0 (source)
/// - Indegree 1: nodes 1, 3, 5, 7
/// - Indegree 2: nodes 2, 4
/// - Indegree 3: node 6
/// - Cycle: 2 → 4 → 6 → 2
fn directed_graph() -> (VecGraph, VecGraph) {
    let graph = VecGraph::from_arcs([
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
    ]);
    let transpose = VecGraph::from_arcs([
        (1, 0),
        (2, 0),
        (3, 1),
        (4, 1),
        (5, 1),
        (4, 2),
        (6, 3),
        (6, 4),
        (6, 5),
        (7, 5),
        (2, 6),
    ]);
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

/// Symmetric path graph: 0 - 1 - 2 - 3.
fn symm_path() -> VecGraph {
    VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 2)])
}

/// Symmetric star graph: 0 is the centre, connected to 1, 2, 3.
fn symm_star() -> VecGraph {
    VecGraph::from_arcs([(0, 1), (1, 0), (0, 2), (2, 0), (0, 3), (3, 0)])
}

#[test]
fn test_ess_diameter_only() -> Result<()> {
    let (graph, transpose) = directed_graph();
    for result in [
        <exact_sum_sweep::Diameter as Level>::run(&graph, &transpose, None, no_logging![]),
        <exact_sum_sweep::Diameter as Level<false>>::run(&graph, &transpose, None, no_logging![]),
    ] {
        assert_eq!(result.diameter, 3);
    }
    Ok(())
}

#[test]
fn test_ess_radius_only() -> Result<()> {
    let (graph, transpose) = directed_graph();
    for result in [
        <exact_sum_sweep::Radius as Level>::run(&graph, &transpose, None, no_logging![]),
        <exact_sum_sweep::Radius as Level<false>>::run(&graph, &transpose, None, no_logging![]),
    ] {
        assert_eq!(result.radius, 2);
    }
    Ok(())
}

#[test]
fn test_ess_all_forward() -> Result<()> {
    let (graph, transpose) = directed_graph();
    for result in [
        <exact_sum_sweep::AllForward as Level>::run(&graph, &transpose, None, no_logging![]),
        <exact_sum_sweep::AllForward as Level<false>>::run(
            &graph,
            &transpose,
            None,
            no_logging![],
        ),
    ] {
        assert_eq!(result.diameter, 3);
        assert_eq!(result.radius, 2);
        assert_eq!(
            result.forward_eccentricities.as_ref(),
            &[3, 3, 2, 3, 2, 3, 2, 0]
        );
    }
    Ok(())
}

#[test]
fn test_ess_radius_diameter() -> Result<()> {
    let (graph, transpose) = directed_graph();
    for result in [
        <exact_sum_sweep::RadiusDiameter as Level>::run(
            &graph,
            &transpose,
            None,
            no_logging![],
        ),
        <exact_sum_sweep::RadiusDiameter as Level<false>>::run(
            &graph,
            &transpose,
            None,
            no_logging![],
        ),
    ] {
        assert_eq!(result.diameter, 3);
        assert_eq!(result.radius, 2);
    }
    Ok(())
}

#[test]
fn test_ess_all_symm() {
    let graph = symm_graph();
    for result in [
        <exact_sum_sweep::All as Level>::run_symm(&graph, no_logging![]),
        <exact_sum_sweep::All as Level<false>>::run_symm(&graph, no_logging![]),
    ] {
        assert_eq!(result.diameter, 1);
        assert_eq!(result.radius, 1);
        assert_eq!(result.eccentricities.as_ref(), &[1, 1, 1, 1, 1]);
    }
}

#[test]
fn test_ess_all_forward_symm() {
    let graph = symm_graph();
    for result in [
        <exact_sum_sweep::AllForward as Level>::run_symm(&graph, no_logging![]),
        <exact_sum_sweep::AllForward as Level<false>>::run_symm(&graph, no_logging![]),
    ] {
        assert_eq!(result.diameter, 1);
        assert_eq!(result.radius, 1);
    }
}

#[test]
fn test_ess_diameter_symm() {
    let graph = symm_graph();
    for result in [
        <exact_sum_sweep::Diameter as Level>::run_symm(&graph, no_logging![]),
        <exact_sum_sweep::Diameter as Level<false>>::run_symm(&graph, no_logging![]),
    ] {
        assert_eq!(result.diameter, 1);
    }
}

#[test]
fn test_ess_radius_symm() {
    let graph = symm_graph();
    for result in [
        <exact_sum_sweep::Radius as Level>::run_symm(&graph, no_logging![]),
        <exact_sum_sweep::Radius as Level<false>>::run_symm(&graph, no_logging![]),
    ] {
        assert_eq!(result.radius, 1);
    }
}

#[test]
fn test_ess_radius_diameter_symm() {
    let graph = symm_graph();
    for result in [
        <exact_sum_sweep::RadiusDiameter as Level>::run_symm(&graph, no_logging![]),
        <exact_sum_sweep::RadiusDiameter as Level<false>>::run_symm(&graph, no_logging![]),
    ] {
        assert_eq!(result.diameter, 1);
        assert_eq!(result.radius, 1);
    }
}

#[test]
fn test_ess_symm_path() {
    // A path graph: 0 - 1 - 2 - 3
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 2)]);

    for result in [
        <exact_sum_sweep::All as Level>::run_symm(&graph, no_logging![]),
        <exact_sum_sweep::All as Level<false>>::run_symm(&graph, no_logging![]),
    ] {
        assert_eq!(result.diameter, 3);
        assert_eq!(result.radius, 2);
    }
}

#[test]
fn test_ess_diameter_cycle() {
    // Simple 4-cycle
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3)]);
    for result in [
        <exact_sum_sweep::Diameter as Level>::run(&graph, &transpose, None, no_logging![]),
        <exact_sum_sweep::Diameter as Level<false>>::run(&graph, &transpose, None, no_logging![]),
    ] {
        assert_eq!(result.diameter, 3);
    }
}

#[test]
fn test_ess_all_forward_cycle() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3)]);
    for result in [
        <exact_sum_sweep::AllForward as Level>::run(&graph, &transpose, None, no_logging![]),
        <exact_sum_sweep::AllForward as Level<false>>::run(
            &graph,
            &transpose,
            None,
            no_logging![],
        ),
    ] {
        assert_eq!(result.diameter, 3);
        assert_eq!(result.radius, 3);
        // All nodes in a cycle have equal eccentricity
        for &ecc in result.forward_eccentricities.iter() {
            assert_eq!(ecc, 3);
        }
    }
}

#[test]
fn test_ess_all_star_graph() {
    // Star graph: 0 -> 1, 0 -> 2, 0 -> 3 (all symmetric)
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (0, 2), (2, 0), (0, 3), (3, 0)]);

    for result in [
        <exact_sum_sweep::All as Level>::run_symm(&graph, no_logging![]),
        <exact_sum_sweep::All as Level<false>>::run_symm(&graph, no_logging![]),
    ] {
        assert_eq!(result.diameter, 2);
        assert_eq!(result.radius, 1);
        assert_eq!(result.radial_vertex, 0);
    }
}

/// Runs all levels on a symmetric graph through both `run_symm`
/// (SYMMETRIC=true) and `run` with the graph as its own transpose
/// (SYMMETRIC=false), checking that the results match.
#[test]
fn test_ess_symmetric_vs_non_symmetric() {
    for graph in [symm_graph(), symm_path(), symm_star()] {
        // All
        let symm = <exact_sum_sweep::All as Level>::run_symm(&graph, no_logging![]);
        let symm_no_tot =
            <exact_sum_sweep::All as Level<false>>::run_symm(&graph, no_logging![]);
        let dir = <exact_sum_sweep::All as Level>::run(&graph, &graph, None, no_logging![]);
        let dir_no_tot =
            <exact_sum_sweep::All as Level<false>>::run(&graph, &graph, None, no_logging![]);

        for (label, result) in [
            ("symm", (symm.diameter, symm.radius)),
            ("symm_no_tot", (symm_no_tot.diameter, symm_no_tot.radius)),
            ("dir", (dir.diameter, dir.radius)),
            ("dir_no_tot", (dir_no_tot.diameter, dir_no_tot.radius)),
        ] {
            assert_eq!(
                result,
                (symm.diameter, symm.radius),
                "All: {label} disagrees: got {result:?}, expected ({}, {})",
                symm.diameter,
                symm.radius
            );
        }
        // Forward eccentricities from the directed run must match the
        // symmetric eccentricities.
        assert_eq!(
            dir.forward_eccentricities.as_ref(),
            symm.eccentricities.as_ref(),
            "All: forward eccentricities differ"
        );
        assert_eq!(
            dir_no_tot.forward_eccentricities.as_ref(),
            symm_no_tot.eccentricities.as_ref(),
            "All (no tot): forward eccentricities differ"
        );

        // RadiusDiameter
        let symm_rd =
            <exact_sum_sweep::RadiusDiameter as Level>::run_symm(&graph, no_logging![]);
        let symm_rd_no_tot =
            <exact_sum_sweep::RadiusDiameter as Level<false>>::run_symm(&graph, no_logging![]);
        let dir_rd =
            <exact_sum_sweep::RadiusDiameter as Level>::run(&graph, &graph, None, no_logging![]);
        let dir_rd_no_tot = <exact_sum_sweep::RadiusDiameter as Level<false>>::run(
            &graph,
            &graph,
            None,
            no_logging![],
        );
        for (label, d, r) in [
            ("symm", symm_rd.diameter, symm_rd.radius),
            (
                "symm_no_tot",
                symm_rd_no_tot.diameter,
                symm_rd_no_tot.radius,
            ),
            ("dir", dir_rd.diameter, dir_rd.radius),
            ("dir_no_tot", dir_rd_no_tot.diameter, dir_rd_no_tot.radius),
        ] {
            assert_eq!(
                (d, r),
                (symm.diameter, symm.radius),
                "RadiusDiameter: {label} disagrees"
            );
        }

        // Diameter
        let symm_d = <exact_sum_sweep::Diameter as Level>::run_symm(&graph, no_logging![]);
        let symm_d_no_tot =
            <exact_sum_sweep::Diameter as Level<false>>::run_symm(&graph, no_logging![]);
        let dir_d =
            <exact_sum_sweep::Diameter as Level>::run(&graph, &graph, None, no_logging![]);
        let dir_d_no_tot = <exact_sum_sweep::Diameter as Level<false>>::run(
            &graph,
            &graph,
            None,
            no_logging![],
        );
        for (label, d) in [
            ("symm", symm_d.diameter),
            ("symm_no_tot", symm_d_no_tot.diameter),
            ("dir", dir_d.diameter),
            ("dir_no_tot", dir_d_no_tot.diameter),
        ] {
            assert_eq!(d, symm.diameter, "Diameter: {label} disagrees");
        }

        // Radius
        let symm_r = <exact_sum_sweep::Radius as Level>::run_symm(&graph, no_logging![]);
        let symm_r_no_tot =
            <exact_sum_sweep::Radius as Level<false>>::run_symm(&graph, no_logging![]);
        let dir_r =
            <exact_sum_sweep::Radius as Level>::run(&graph, &graph, None, no_logging![]);
        let dir_r_no_tot = <exact_sum_sweep::Radius as Level<false>>::run(
            &graph,
            &graph,
            None,
            no_logging![],
        );
        for (label, r) in [
            ("symm", symm_r.radius),
            ("symm_no_tot", symm_r_no_tot.radius),
            ("dir", dir_r.radius),
            ("dir_no_tot", dir_r_no_tot.radius),
        ] {
            assert_eq!(r, symm.radius, "Radius: {label} disagrees");
        }
    }
}
