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
//! - [`transpose_par`]: same, sorting in parallel (requires
//!   [`IntoParLenders`]);
//! - [`transpose_labeled_par`]: same, for labeled graphs.
//!
//! # Symmetrize
//!
//! - [`symmetrize()`]: returns a symmetrized version of a graph, optionally
//!   removing self-loops;
//! - [`symmetrize_sorted`]: same, but exploits the fact that the input is
//!   already sorted, halving the number of arcs to sort;
//! - [`symmetrize_par`]: same, sorting in parallel (requires
//!   [`IntoParLenders`]);
//! - [`symmetrize_sorted_par`]: same as `symmetrize_sorted`, but sorting the
//!   transpose arcs in parallel (requires [`SplitLabeling`]).
//!
//! The order above is in general from slower to faster, but the actual
//! performance depends on the graph and the hardware.
//!
//! # Permute
//!
//! - [`permute`]: returns the graph with nodes permuted according to a given
//!   permutation;
//! - [`permute_par`]: same, sorting in parallel (requires
//!   [`IntoParLenders`]).
//!
//! # Map
//!
//! - [`map()`]: returns the graph with nodes mapped through an arbitrary (not
//!   necessarily bijective) function, deduplicating arcs;
//! - [`map_par`]: same, sorting in parallel (requires [`IntoParLenders`]).
//!
//! # Memory Usage
//!
//! The transpose, symmetrize, permute, and map functions internally sort arcs
//! by batching them to temporary files and then merging. The amount of memory
//! used for batching is controlled by the [`MemoryUsage`] parameter. The `_par`
//! variants sort in parallel using [`ParSortedGraph`] and accept graphs
//! implementing [`IntoParLenders`]; [`symmetrize_sorted_par`] additionally
//! requires [`SplitLabeling`] because it re-splits the graph at the sort
//! boundaries.
//!
//! [`MemoryUsage`]: crate::utils::MemoryUsage
//! [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
//! [`IntoParLenders`]: crate::traits::IntoParLenders
//! [`SplitLabeling`]: crate::traits::SplitLabeling

mod symmetrize;
pub use symmetrize::*;

mod transpose;
pub use transpose::*;

mod perm;
pub use perm::*;

mod map;
pub use map::*;
