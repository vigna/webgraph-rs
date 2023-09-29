/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

pub mod bvgraph;
pub mod labeled_pairs_graph;
pub mod pairs_graph;
pub mod permuted_graph;
pub mod vec_graph;

pub mod prelude {
    pub use super::bvgraph::*;
    pub use super::permuted_graph::*;
    pub use super::vec_graph::*;
}
