/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use lender::*;
use sux::{
    traits::{IndexedSeq, Succ},
    utils::FairChunks,
};

/// A wrapper that overrides the number of partitions for
/// [`IntoParLenders`].
///
/// Delegates all graph and labeling traits to the inner graph, but provides
/// its own [`IntoParLenders`] implementation using
/// [`SplitLabeling::split_iter`] with the partition count stored in the
/// wrapper.
#[derive(Debug, Clone)]
pub struct ParGraph<G>(pub G, usize);

impl<G> ParGraph<G> {
    /// Creates a new [`ParGraph`] with the given inner graph and
    /// number of partitions.
    pub fn new(graph: G, num_partitions: usize) -> Self {
        assert!(
            num_partitions > 0,
            "the number of partitions must be positive"
        );
        Self(graph, num_partitions)
    }
}

impl<G: SequentialLabeling> SequentialLabeling for ParGraph<G> {
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

impl<G: SequentialGraph> SequentialGraph for ParGraph<G> {}

impl<G: RandomAccessLabeling> RandomAccessLabeling for ParGraph<G> {
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

impl<G: RandomAccessGraph> RandomAccessGraph for ParGraph<G> {}

impl<'a, G: SequentialLabeling + SplitLabeling> IntoParLenders for &'a ParGraph<G>
where
    for<'b> <G as SplitLabeling>::SplitLender<'b>: ExactSizeLender + FusedLender,
{
    type ParLender = <G as SplitLabeling>::SplitLender<'a>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let n = self.1;
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
impl<'b, G: SequentialLabeling> IntoLender for &'b ParGraph<G> {
    type Lender = <G as SequentialLabeling>::Lender<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

/// A wrapper that splits a graph into arc-balanced partitions using
/// a degree cumulative function.
///
/// Delegates all graph and labeling traits to the inner graph, but provides
/// its own [`IntoParLenders`] implementation using
/// [`SplitLabeling::split_iter_at`] with arc-balanced cutpoints computed
/// from the DCF via [`FairChunks`].
#[derive(Debug, Clone)]
pub struct ParallelDcfGraph<G, D>(pub G, pub D, pub usize);

impl<G, D> ParallelDcfGraph<G, D> {
    /// Creates a new [`ParallelDcfGraph`] with the given inner graph,
    /// degree cumulative function, and number of partitions.
    pub fn new(graph: G, dcf: D, num_partitions: usize) -> Self {
        assert!(
            num_partitions > 0,
            "the number of partitions must be positive"
        );
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

impl<'a, G, D> IntoParLenders for &'a ParallelDcfGraph<G, D>
where
    G: SequentialLabeling + SplitLabeling,
    D: for<'b> Succ<Input = u64, Output<'b> = u64> + IndexedSeq,
    for<'b> <G as SplitLabeling>::SplitLender<'b>: ExactSizeLender + FusedLender,
{
    type ParLender = <G as SplitLabeling>::SplitLender<'a>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let n = self.2;
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
