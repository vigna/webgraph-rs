/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Transformations on labelings and graphs.
//!
//! This module provides functions that compute common graph transformations. All
//! functions return the result as a [sequential
//! graph](crate::traits::SequentialGraph), which can then be compressed into a
//! [BvGraph](crate::graphs::bvgraph) using
//! [`BvComp`](crate::graphs::bvgraph::BvComp) or stored in any other format.
//!
//! # Transpose
//!
//! - [`transpose`]: returns the transpose of a graph;
//! - [`transpose_labeled`]: returns the transpose of a labeled graph;
//! - [`transpose_split`]: returns the transpose of a
//!   [splittable](crate::traits::SplitLabeling) graph, sorting in parallel;
//! - [`transpose_labeled_split`]: same, for labeled graphs.
//!
//! # Simplify
//!
//! - [`simplify`]: returns a simplified (undirected and loopless) version of a
//!   graph;
//! - [`simplify_sorted`]: same, but exploits the fact that the input is already
//!   sorted, halving the number of arcs to sort;
//! - [`simplify_split`]: same, using splitting to sort in parallel.
//!
//! # Permute
//!
//! - [`permute`]: returns the graph with nodes permuted according to a given
//!   permutation;
//! - [`permute_split`]: same, using splitting to sort in parallel.
//!
//! # Memory Usage
//!
//! The transpose, simplify, and permute functions internally use
//! [`SortPairs`](crate::utils::SortPairs), which sorts arcs by batching them to
//! temporary files and then merging. The amount of memory used for batching is
//! controlled by the [`MemoryUsage`](crate::utils::MemoryUsage) parameter. The
//! `_split` variants sort in parallel using
//! [`ParSortIters`](crate::utils::ParSortIters) and are significantly faster on
//! [splittable](crate::traits::SplitLabeling) graphs.

mod simplify;
pub use simplify::*;

mod transpose;
pub use transpose::*;

mod perm;
pub use perm::*;
