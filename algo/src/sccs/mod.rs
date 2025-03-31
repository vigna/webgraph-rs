/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Algorithms used to compute and work with strongly connected components.
//!
//! There are two implementations for directed graph: [Tarjan's
//! algorithm](tarjan) and [Kosaraju's algorithm](kosaraju). The former is to be
//! preferred in almost all cases: Kosaraju's algorithm is slower and requires
//! the transpose of the graph—it is mainly useful for testing and debugging.
//!
//! For symmetric (i.e., undirected) graphs there is a [sequential](symm_seq)
//! and a [parallel](symm_par) implementation that computes connected
//! components.
//!
//! # Examples
//! ```
//! use dsi_progress_logger::no_logging;
//! use webgraph::{graphs::vec_graph::VecGraph};
//! use webgraph_algo::sccs::*;
//!
//! let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
//!
//! // Let's build the graph SCCS with Tarjan's algorithm
//! let mut scc = tarjan(graph, no_logging![]);
//!
//! // Let's sort the SCC by size
//! let sizes = scc.sort_by_size();
//!
//! assert_eq!(sizes, vec![3, 1].into_boxed_slice());
//! assert_eq!(scc.components(), &vec![0, 0, 0, 1]);
//! ```

mod tarjan;
pub use tarjan::*;

mod kosaraju;
pub use kosaraju::*;
mod symm_seq;
pub use symm_seq::*;

mod symm_par;
pub use symm_par::*;

use rayon::{
    iter::{IntoParallelRefMutIterator, ParallelIterator},
    slice::ParallelSliceMut,
};
use webgraph::algo::llp;

/// Strongly connected components.
///
/// An instance of this structure stores the [index of the
/// component](Sccs::components) of each node. Components are numbered from 0 to
/// [`num_components`](Sccs::num_components).
///
/// Moreover, this structure makes it possible to [sort the components by
/// size](Sccs::sort_by_size), possibly using [parallel
/// methods](Sccs::par_sort_by_size).
pub struct Sccs {
    num_components: usize,
    components: Box<[usize]>,
}

impl Sccs {
    pub fn new(num_components: usize, components: Box<[usize]>) -> Self {
        Sccs {
            num_components,
            components,
        }
    }

    /// Returns the number of strongly connected components.
    pub fn num_components(&self) -> usize {
        self.num_components
    }

    /// Returns a slice containing, for each node, the index of the component
    /// it belongs to.
    #[inline(always)]
    pub fn components(&self) -> &[usize] {
        &self.components
    }

    /// Returns the sizes of all components.
    pub fn compute_sizes(&self) -> Box<[usize]> {
        let mut sizes = vec![0; self.num_components()];
        for &node_component in self.components() {
            sizes[node_component] += 1;
        }
        sizes.into_boxed_slice()
    }

    /// Renumbers the components by decreasing size.
    ///
    /// After a call to this method, the sizes of strongly connected components
    /// will decreasing in the component index. The method returns the sizes of
    /// the components after the renumbering.
    pub fn sort_by_size(&mut self) -> Box<[usize]> {
        let mut sizes = self.compute_sizes();
        assert!(sizes.len() == self.num_components());
        let mut sort_perm = Vec::from_iter(0..sizes.len());
        sort_perm.sort_unstable_by(|&x, &y| sizes[y].cmp(&sizes[x]));
        let mut inv_perm = vec![0; sizes.len()];
        sort_perm
            .iter()
            .enumerate()
            .for_each(|(i, &x)| inv_perm[x] = i);

        self.components
            .iter_mut()
            .for_each(|node_component| *node_component = inv_perm[*node_component]);
        sizes.sort_by(|&x, &y| y.cmp(&x));
        sizes
    }

    /// Renumbers the components by decreasing size using parallel methods.
    ///
    /// After a call to this method, the sizes of strongly connected components
    /// will decreasing in the component index. The method returns the sizes of
    /// the components after the renumbering.
    pub fn par_sort_by_size(&mut self) -> Box<[usize]> {
        let mut sizes = self.compute_sizes();
        assert!(sizes.len() == self.num_components());
        let mut sort_perm = Vec::from_iter(0..sizes.len());
        sort_perm.par_sort_unstable_by(|&x, &y| sizes[y].cmp(&sizes[x]));
        let mut inv_perm = vec![0; sizes.len()];
        llp::invert_permutation(&sort_perm, &mut inv_perm);
        self.components
            .par_iter_mut()
            .for_each(|node_component| *node_component = inv_perm[*node_component]);
        sizes.sort_by(|&x, &y| y.cmp(&x));
        sizes
    }
}
