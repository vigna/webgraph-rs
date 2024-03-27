/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

pub mod arc_list_graph;

pub mod bvgraph;
pub use bvgraph::*;
pub mod permuted_graph;

mod union_graph;
pub use union_graph::UnionGraph;

pub mod random;

pub mod vec_graph;
pub mod prelude {
    pub use super::bvgraph::*;
    pub use super::permuted_graph::PermutedGraph;
    pub use super::vec_graph::VecGraph;
}
