/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Graph implementations.
//!
//! - [`bvgraph`]: the compressed BV graph format ([`BvGraph`] for random
//!   access, [`BvGraphSeq`] for sequential access, [`BvCompConfig`] for
//!   compression);
//! - [`vec_graph`] / [`btree_graph`]: mutable in-memory graphs;
//! - [`csr_graph`]: classical CSR representation;
//! - [`arc_list_graph`]: sequential graph from an iterator of arcs;
//! - [`permuted_graph`]: applies a node permutation to an existing graph;
//! - [`union_graph`]: merges the arcs of two graphs;
//! - [`no_selfloops_graph`]: filters out self-loops;
//! - [`par_sorted_graph`]: adapter for parallel iteration.
//!
//! [`BvGraph`]: bvgraph::BvGraph
//! [`BvGraphSeq`]: bvgraph::BvGraphSeq
//! [`BvCompConfig`]: bvgraph::BvCompConfig

pub mod arc_list_graph;
pub mod btree_graph;
pub mod bvgraph;
pub mod csr_graph;
pub mod no_selfloops_graph;
pub mod par_graphs;
pub mod par_sorted_graph;
pub mod permuted_graph;
pub mod random;
pub mod union_graph;
pub mod vec_graph;

pub mod prelude {
    pub use super::btree_graph::BTreeGraph;
    pub use super::bvgraph::*;
    pub use super::csr_graph::{CsrGraph, CsrSortedGraph};
    pub use super::no_selfloops_graph::NoSelfLoopsGraph;
    pub use super::par_graphs::ParGraph;
    pub use super::par_sorted_graph::{ParSortedGraph, ParSortedLabeledGraph};
    pub use super::permuted_graph::PermutedGraph;
    pub use super::union_graph::UnionGraph;
    pub use super::vec_graph::VecGraph;
}
