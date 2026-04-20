/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Transformations on labelings and graphs.
//!
//! This module provides functions that compute common graph transformations.
//!
//! # Transpose
//!
//! - [`transpose()`]: returns the transpose of a graph;
//! - [`transpose_labeled`]: returns the transpose of a labeled graph;
//! - [`transpose_split`]: returns the transpose of a graph implementing
//!   [`IntoParLenders`], sorting in parallel;
//! - [`transpose_labeled_split`]: same, for labeled graphs.
//!
//! # Symmetrize
//!
//! - [`symmetrize()`]: returns a symmetrized version of a graph, optionally
//!   removing self-loops;
//! - [`symmetrize_sorted`]: same, but exploits the fact that the input is
//!   already sorted, halving the number of arcs to sort;
//! - [`symmetrize_split`]: same, using splitting to sort in parallel;
//! - [`symmetrize_sorted_split`]: same, but sorting the transpose arcs in
//!   parallel.
//!
//! The order above is in general from slower to faster, but the actual
//! performance depends on the graph and the hardware.
//!
//! # Permute
//!
//! - [`permute`]: returns the graph with nodes permuted according to a given
//!   permutation;
//! - [`permute_split`]: same, using splitting to sort in parallel.
//!
//! # Map
//!
//! - [`map()`]: returns the graph with nodes mapped through an arbitrary (not
//!   necessarily bijective) function, deduplicating arcs;
//! - [`map_split`]: same, using splitting to sort in parallel.
//!
//! # Memory Usage
//!
//! The transpose, symmetrize, permute, and map functions internally sort arcs
//! by batching them to temporary files and then merging. The amount of memory
//! used for batching is controlled by the [`MemoryUsage`] parameter. The
//! `_split` variants sort in parallel using [`ParSortedGraph`] and accept
//! graphs implementing [`IntoParLenders`].
//!
//! [`MemoryUsage`]: crate::utils::MemoryUsage
//! [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
//! [`IntoParLenders`]: crate::traits::IntoParLenders

mod symmetrize;
pub use symmetrize::*;

mod transpose;
pub use transpose::*;

mod perm;
pub use perm::*;

mod map;
pub use map::*;
