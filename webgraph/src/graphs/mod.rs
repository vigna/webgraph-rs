/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Graph implementations.
//!
//! - [`bvgraph`]: the compressed BV graph format ([`BvGraph`] for random
//!   access, [`BvGraphSeq`] for sequential access, [`BvCompConf`] for
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
//! [`BvCompConf`]: bvgraph::BvCompConf

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

/// Implements [`IntoLender`] for references to a labeling by delegating to
/// [`SequentialLabeling::iter`].
///
/// The arguments are the generic parameters of the implementation (in
/// brackets), the labeling type, and optionally the predicates of a `where`
/// clause (in brackets).
///
/// [`IntoLender`]: lender::IntoLender
/// [`SequentialLabeling::iter`]: crate::traits::SequentialLabeling::iter
macro_rules! impl_into_lender {
    ([$($gen:tt)*] $ty:ty $([$($where_clause:tt)*])?) => {
        /// Convenience implementation that makes it possible to iterate
        /// over the graph using the [`for_`] macro
        /// (see the [crate documentation]).
        ///
        /// [`for_`]: lender::for_!
        /// [crate documentation]: crate
        impl<'__lend, $($gen)*> lender::IntoLender for &'__lend $ty
        $(where $($where_clause)*)?
        {
            type Lender = <$ty as $crate::traits::SequentialLabeling>::Lender<'__lend>;

            #[inline(always)]
            fn into_lender(self) -> Self::Lender {
                $crate::traits::SequentialLabeling::iter(self)
            }
        }
    };
}
pub(crate) use impl_into_lender;

/// Implements [`SplitLabeling`] using the ready-made [sequential] (`seq`) or
/// [random-access] (`ra`) implementation, and then [`IntoParLenders`] using
/// [`impl_parallel_from_split!`].
///
/// The arguments are the kind of implementation, the generic parameters of
/// the implementations (in brackets), the labeling type, and the predicates
/// of the `where` clauses of the two implementations (in brackets).
///
/// [`SplitLabeling`]: crate::traits::SplitLabeling
/// [`IntoParLenders`]: crate::traits::IntoParLenders
/// [sequential]: crate::traits::split::seq
/// [random-access]: crate::traits::split::ra
/// [`impl_parallel_from_split!`]: crate::impl_parallel_from_split!
macro_rules! impl_split {
    (seq [$($gen:tt)*] $ty:ty [$($split_where:tt)*] [$($par_where:tt)*]) => {
        impl<$($gen)*> $crate::traits::SplitLabeling for $ty
        where $($split_where)*
        {
            type SplitLender<'__split>
                = $crate::traits::split::seq::Lender<'__split, $ty>
            where
                Self: '__split;
            type IntoIterator<'__split>
                = $crate::traits::split::seq::IntoIterator<'__split, $ty>
            where
                Self: '__split;

            fn split_iter_at(
                &self,
                cutpoints: impl IntoIterator<Item = usize>,
            ) -> Self::IntoIterator<'_> {
                $crate::traits::split::seq::Iter::new(
                    $crate::traits::SequentialLabeling::iter(self),
                    cutpoints,
                )
            }
        }

        $crate::impl_parallel_from_split!([$($gen)*] $ty [$($par_where)*]);
    };
    (ra [$($gen:tt)*] $ty:ty [$($split_where:tt)*] [$($par_where:tt)*]) => {
        impl<$($gen)*> $crate::traits::SplitLabeling for $ty
        where $($split_where)*
        {
            type SplitLender<'__split>
                = $crate::traits::split::ra::Lender<'__split, $ty>
            where
                Self: '__split;
            type IntoIterator<'__split>
                = $crate::traits::split::ra::IntoIterator<'__split, $ty>
            where
                Self: '__split;

            fn split_iter_at(
                &self,
                cutpoints: impl IntoIterator<Item = usize>,
            ) -> Self::IntoIterator<'_> {
                $crate::traits::split::ra::Iter::new(self, cutpoints)
            }
        }

        $crate::impl_parallel_from_split!([$($gen)*] $ty [$($par_where)*]);
    };
}
pub(crate) use impl_split;

/// Implements [`SequentialLabeling`] for a [`RandomAccessLabeling`] using a
/// [`LenderImpl`].
///
/// The arguments are the generic parameters of the implementation (in
/// brackets), the labeling type, the label type, and a closure-like
/// expression computing the number of nodes.
///
/// [`SequentialLabeling`]: crate::traits::SequentialLabeling
/// [`RandomAccessLabeling`]: crate::traits::RandomAccessLabeling
/// [`LenderImpl`]: crate::traits::LenderImpl
macro_rules! impl_seq_from_ra {
    ([$($gen:tt)*] $ty:ty, $label:ty, |$this:ident| $num_nodes:expr) => {
        impl<$($gen)*> $crate::traits::SequentialLabeling for $ty {
            type Label = $label;
            type Lender<'__node>
                = $crate::traits::LenderImpl<'__node, Self>
            where
                Self: '__node;

            #[inline(always)]
            fn num_nodes(&self) -> usize {
                let $this = self;
                $num_nodes
            }

            #[inline(always)]
            fn get_num_arcs(&self) -> Option<u64> {
                Some($crate::traits::RandomAccessLabeling::num_arcs(self))
            }

            #[inline(always)]
            fn iter_from(&self, from: usize) -> Self::Lender<'_> {
                $crate::traits::LenderImpl {
                    labeling: self,
                    nodes: from..$crate::traits::SequentialLabeling::num_nodes(self),
                }
            }
        }
    };
}
pub(crate) use impl_seq_from_ra;

/// Panics if `u` or `v` is not a node of a graph with `num_nodes` nodes.
#[inline]
#[track_caller]
pub(crate) fn assert_nodes_exist(num_nodes: usize, u: usize, v: usize) {
    let max = u.max(v);
    if max >= num_nodes {
        panic!("Node {max} does not exist (the graph has {num_nodes} nodes)");
    }
}

/// Checks that a mutable graph built from labeled arcs is not equal to itself
/// if a label is a NaN, and that it is equal to itself otherwise.
#[cfg(test)]
#[allow(clippy::eq_op)] // comparing a graph with itself is the point
pub(crate) fn check_nan_labels<G: PartialEq + core::fmt::Debug>(
    from_arcs: impl Fn(Vec<((usize, usize), Option<f64>)>) -> G,
) {
    let mut arcs = vec![
        ((0, 1), Some(1.0)),
        ((0, 2), None),
        ((1, 2), Some(2.0)),
        ((2, 4), Some(f64::INFINITY)),
        ((3, 4), Some(f64::NEG_INFINITY)),
        ((1, 3), Some(f64::NAN)),
    ];
    let g = from_arcs(arcs.clone());
    assert_ne!(
        g, g,
        "The label contains a NaN which is not equal to itself so the graph must be not equal to itself"
    );

    arcs.pop();
    let g = from_arcs(arcs);
    assert_eq!(g, g, "Without NaN the graph should be equal to itself");
}
