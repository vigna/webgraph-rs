/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(dead_code)]

use anyhow::Result;
use dsi_progress_logger::no_logging;
use std::path::{Path, PathBuf};
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::store_ef_with_data;

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
    VecGraph::from_arcs([
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
    ])
}

/// Builds the Elias–Fano representation from a γ-coded delta offsets
/// file and serializes it.
pub fn build_ef_from_offsets(
    num_nodes: usize,
    data_path: &Path,
    offsets_path: &Path,
    ef_path: &Path,
) -> Result<()> {
    store_ef_with_data(num_nodes, data_path, offsets_path, ef_path, &mut no_logging![])
}

/// Builds the Elias–Fano representation of offsets for a graph basename.
///
/// Reads `num_nodes` from the `.properties` file.
pub fn build_ef(basename: &Path) -> Result<()> {
    let properties_path = basename.with_extension("properties");
    let props = std::fs::read_to_string(&properties_path)?;
    let num_nodes: usize = props
        .lines()
        .find(|l| l.starts_with("nodes="))
        .unwrap()
        .strip_prefix("nodes=")
        .unwrap()
        .parse()?;

    build_ef_from_offsets(
        num_nodes,
        &basename.with_extension("graph"),
        &basename.with_extension("offsets"),
        &basename.with_extension("ef"),
    )
}

/// Returns the basename for the cnr-2000 test graph, selecting the
/// platform-appropriate data directory.
///
/// On 64-bit platforms this returns `../data/cnr-2000`; on 32-bit platforms
/// it returns `../data/cnr-2000_32/cnr-2000`, whose `.ef` and `.dcf` files
/// are built with 32-bit `usize`.
pub fn cnr_2000_basename() -> PathBuf {
    #[cfg(target_pointer_width = "64")]
    return PathBuf::from("../data/cnr-2000");
    #[cfg(not(target_pointer_width = "64"))]
    return PathBuf::from("../data/cnr-2000_32/cnr-2000");
}

/// Returns the basename for the cnr-2000-t (transpose) test graph,
/// selecting the platform-appropriate data directory.
///
/// See [`cnr_2000_basename`] for details on the 32-bit strategy.
pub fn cnr_2000_t_basename() -> PathBuf {
    #[cfg(target_pointer_width = "64")]
    return PathBuf::from("../data/cnr-2000-t");
    #[cfg(not(target_pointer_width = "64"))]
    return PathBuf::from("../data/cnr-2000_32/cnr-2000-t");
}
