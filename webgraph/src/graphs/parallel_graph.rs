/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use lender::*;
use std::num::NonZeroUsize;

/// A wrapper that overrides the number of partitions for
/// [`ParallelLabeling`].
///
/// Delegates all graph and labeling traits to the inner graph, but provides
/// its own [`ParallelLabeling`] implementation using
/// [`SplitLabeling::split_iter`] with the partition count stored in the
/// wrapper.
#[derive(Debug, Clone)]
pub struct ParallelGraph<G>(pub G, pub NonZeroUsize);

impl<G> ParallelGraph<G> {
    /// Creates a new [`ParallelGraph`] with the given inner graph and
    /// number of partitions.
    pub fn new(graph: G, num_partitions: NonZeroUsize) -> Self {
        Self(graph, num_partitions)
    }
}

impl<G: SequentialLabeling> SequentialLabeling for ParallelGraph<G> {
    type Label = G::Label;
    type Lender<'node>
        = G::Lender<'node>
    where
        Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        self.0.num_arcs_hint()
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        self.0.iter_from(from)
    }
}

impl<G: SequentialGraph> SequentialGraph for ParallelGraph<G> {}

impl<G: RandomAccessLabeling> RandomAccessLabeling for ParallelGraph<G> {
    type Labels<'succ>
        = G::Labels<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }

    #[inline(always)]
    fn labels(&self, node_id: usize) -> Self::Labels<'_> {
        self.0.labels(node_id)
    }

    #[inline(always)]
    fn outdegree(&self, node_id: usize) -> usize {
        self.0.outdegree(node_id)
    }
}

impl<G: RandomAccessGraph> RandomAccessGraph for ParallelGraph<G> {}

impl<G: SequentialLabeling + SplitLabeling> ParallelLabeling for ParallelGraph<G>
where
    for<'a> <G as SplitLabeling>::SplitLender<'a>: ExactSizeLender + FusedLender,
{
    type ParLender<'node>
        = <G as SplitLabeling>::SplitLender<'node>
    where
        Self: 'node;

    fn par_iters(&self) -> (Box<[Self::ParLender<'_>]>, Box<[usize]>) {
        let n = self.1.get();
        let step = self.0.num_nodes().div_ceil(n);
        let num_nodes = self.0.num_nodes();
        let boundaries: Box<[usize]> = (0..=n).map(|i| (i * step).min(num_nodes)).collect();
        let lenders: Box<[_]> = self.0.split_iter(n).into_iter().collect();
        (lenders, boundaries)
    }
}

/// Convenience implementation that makes it possible to iterate
/// over the graph using the [`for_`] macro
/// (see the [crate documentation]).
///
/// [crate documentation]: crate
impl<'b, G: SequentialLabeling> IntoLender for &'b ParallelGraph<G> {
    type Lender = <G as SequentialLabeling>::Lender<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}
