/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use super::Sccs;
use dsi_progress_logger::ProgressLog;
use webgraph::traits::RandomAccessGraph;

/// Computes connected components of symmetric graphs by sequential visits.
pub fn symm_seq(graph: impl RandomAccessGraph, pl: &mut impl ProgressLog) -> Sccs {
    // debug_assert!(check_symmetric(&graph)); requires sync
    let num_nodes = graph.num_nodes();
    super::components_by_dfs(graph, 0..num_nodes, "Computing connected components...", pl)
}
