/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use lender::*;
use std::num::NonZeroUsize;
use sux::{
    traits::{IndexedSeq, Succ},
    utils::FairChunks,
};

/// A wrapper that splits a graph into arc-balanced partitions using
/// a degree cumulative function.
///
/// Delegates all graph and labeling traits to the inner graph, but provides
/// its own [`IntoParIters`] implementation using
/// [`SplitLabeling::split_iter_at`] with arc-balanced cutpoints computed
/// from the DCF via [`FairChunks`].
#[derive(Debug, Clone)]
pub struct ParallelDcfGraph<G, D>(pub G, pub D, pub NonZeroUsize);

impl<G, D> ParallelDcfGraph<G, D> {
    /// Creates a new [`ParallelDcfGraph`] with the given inner graph,
    /// degree cumulative function, and number of partitions.
    pub fn new(graph: G, dcf: D, num_partitions: NonZeroUsize) -> Self {
        Self(graph, dcf, num_partitions)
    }
}

impl<G: SequentialLabeling, D> SequentialLabeling for ParallelDcfGraph<G, D> {
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

impl<G: SequentialGraph, D> SequentialGraph for ParallelDcfGraph<G, D> {}

impl<G: RandomAccessLabeling, D> RandomAccessLabeling for ParallelDcfGraph<G, D> {
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

impl<G: RandomAccessGraph, D> RandomAccessGraph for ParallelDcfGraph<G, D> {}

impl<'a, G, D> IntoParIters for &'a ParallelDcfGraph<G, D>
where
    G: SequentialLabeling + SplitLabeling,
    D: for<'b> Succ<Input = u64, Output<'b> = u64> + IndexedSeq,
    for<'b> <G as SplitLabeling>::SplitLender<'b>: ExactSizeLender + FusedLender,
{
    type Label = G::Label;
    type ParLender = <G as SplitLabeling>::SplitLender<'a>;

    fn into_par_iters(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let n = self.2.get();
        let num_nodes = self.0.num_nodes();
        let total_arcs = self.1.get(num_nodes);
        let target = (total_arcs / n as u64).max(1);
        let cutpoints: Vec<usize> = std::iter::once(0)
            .chain(FairChunks::new(target, &self.1).map(|r| r.end))
            .collect();
        let boundaries: Box<[usize]> = cutpoints.iter().copied().collect();
        let lenders: Box<[_]> = self.0.split_iter_at(cutpoints).into_iter().collect();
        (lenders, boundaries)
    }
}

/// Convenience implementation that makes it possible to iterate
/// over the graph using the [`for_`] macro
/// (see the [crate documentation]).
///
/// [crate documentation]: crate
impl<'b, G: SequentialLabeling, D> IntoLender for &'b ParallelDcfGraph<G, D> {
    type Lender = <G as SequentialLabeling>::Lender<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}
