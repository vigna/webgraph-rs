/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use super::Sccs;
use crate::prelude::*;
use dsi_progress_logger::ProgressLog;
use webgraph::traits::RandomAccessGraph;

/// Computes the strongly connected components of a graph using Kosaraju's algorithm.
///
/// # Arguments
///
/// * `graph` - the graph.
///
/// * `transpose` - the transpose of `graph`.
///
/// * `pl` - a progress logger.
pub fn kosaraju(
    graph: impl RandomAccessGraph,
    transpose: impl RandomAccessGraph,
    pl: &mut impl ProgressLog,
) -> Sccs {
    // The topological sort uses (and completes) the progress logger by itself
    let top_sort = top_sort(&graph, pl);
    super::components_by_dfs(
        transpose,
        top_sort,
        "Computing strongly connected components...",
        pl,
    )
}
