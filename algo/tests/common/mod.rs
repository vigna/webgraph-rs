/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![allow(dead_code)]

use std::path::PathBuf;
use webgraph::graphs::vec_graph::VecGraph;

/// The arcs of the canonical [test graph](test_graph).
pub const TEST_GRAPH_ARCS: [(usize, usize); 11] = [
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
pub fn test_graph() -> VecGraph {
    VecGraph::from_arcs(TEST_GRAPH_ARCS)
}

/// Returns a graph with the given arcs and its transpose; both have at least
/// `num_nodes` nodes (duplicate arcs are added once).
pub fn graph_and_transpose(
    num_nodes: usize,
    arcs: impl IntoIterator<Item = (usize, usize)>,
) -> (VecGraph, VecGraph) {
    let arcs: Vec<_> = arcs.into_iter().collect();
    let num_nodes = arcs
        .iter()
        .map(|&(s, d)| s.max(d) + 1)
        .max()
        .unwrap_or(0)
        .max(num_nodes);
    let mut graph = VecGraph::empty(num_nodes);
    graph.add_arcs(arcs.iter().copied());
    let mut transpose = VecGraph::empty(num_nodes);
    transpose.add_arcs(arcs.iter().map(|&(s, d)| (d, s)));
    (graph, transpose)
}

/// Returns a symmetric graph containing the given arcs and their reversals.
pub fn symm_graph(num_nodes: usize, arcs: impl IntoIterator<Item = (usize, usize)>) -> VecGraph {
    let mut graph = VecGraph::empty(num_nodes);
    graph.add_arcs(arcs.into_iter().flat_map(|(s, d)| [(s, d), (d, s)]));
    graph
}

/// Returns the basename for the cnr-2000 test graph, selecting the
/// platform-appropriate data directory.
pub fn cnr_2000_basename() -> PathBuf {
    #[cfg(target_pointer_width = "64")]
    return PathBuf::from("../data/cnr-2000");
    #[cfg(not(target_pointer_width = "64"))]
    return PathBuf::from("../data/cnr-2000_32/cnr-2000");
}

/// Returns the basename for the transpose of the cnr-2000 test graph,
/// selecting the platform-appropriate data directory.
pub fn cnr_2000_t_basename() -> PathBuf {
    #[cfg(target_pointer_width = "64")]
    return PathBuf::from("../data/cnr-2000-t");
    #[cfg(not(target_pointer_width = "64"))]
    return PathBuf::from("../data/cnr-2000_32/cnr-2000-t");
}

/// Helpers running ExactSumSweep with and without total-distance
/// accumulators.
#[cfg(not(miri))]
pub mod ess {
    use dsi_progress_logger::no_logging;
    use sux::bits::AtomicBitVec;
    use webgraph::traits::RandomAccessGraph;
    use webgraph_algo::distances::exact_sum_sweep::Level;

    /// Runs `L` on a graph and its transpose, with and without total-distance
    /// accumulators, using the radial vertices returned by `radial`.
    pub fn run<L: Level>(
        graph: &(impl RandomAccessGraph + Sync),
        transpose: &(impl RandomAccessGraph + Sync),
        radial: impl Fn() -> Option<AtomicBitVec>,
    ) -> [L::Output; 2] {
        [true, false].map(|use_tot| L::run(graph, transpose, radial(), use_tot, no_logging![]))
    }

    /// Runs `L` on a symmetric graph, with and without total-distance
    /// accumulators.
    pub fn run_symm<L: Level>(graph: &(impl RandomAccessGraph + Sync)) -> [L::OutputSymm; 2] {
        [true, false].map(|use_tot| L::run_symm(graph, use_tot, no_logging![]))
    }
}
