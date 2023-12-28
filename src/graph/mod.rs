/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

pub mod arc_list_graph;
pub mod bvgraph;
pub mod labeled_arc_list_graph;
pub mod permuted_graph;

#[cfg(any(feature = "std", feature = "alloc"))]
pub mod vec_graph;

pub mod prelude {
    pub use super::bvgraph::*;
    pub use super::permuted_graph::*;
    #[cfg(any(feature = "std", feature = "alloc"))]
    pub use super::vec_graph::*;
    //pub use super::vec_labelling::*;
}
