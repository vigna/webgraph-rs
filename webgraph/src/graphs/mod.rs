/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Implementations of graphs.

pub mod arc_list_graph;
pub mod btree_graph;
pub mod bvgraph;
pub mod csr_graph;
pub mod no_selfloops_graph;
pub mod parallel_dcf_graph;
pub mod parallel_graph;
pub mod permuted_graph;
pub mod random;
pub mod sorted_graph;
pub mod union_graph;
pub mod vec_graph;

pub mod prelude {
    pub use super::btree_graph::BTreeGraph;
    pub use super::bvgraph::*;
    pub use super::csr_graph::{CsrGraph, CsrSortedGraph};
    pub use super::no_selfloops_graph::NoSelfLoopsGraph;
    pub use super::parallel_dcf_graph::ParallelDcfGraph;
    pub use super::parallel_graph::ParallelGraph;
    pub use super::permuted_graph::PermutedGraph;
    pub use super::sorted_graph::{SortedGraph, SortedLabeledGraph};
    pub use super::union_graph::UnionGraph;
    pub use super::vec_graph::VecGraph;
}
