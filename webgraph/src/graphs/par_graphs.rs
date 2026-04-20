/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Wrappers that alter the default result of
//! [`IntoParLenders::into_par_lenders`].
//!
//! A graph can have an intrinsic number of parallel lenders returned by
//! [`IntoParLenders::into_par_lenders`]: for example a [`ParSortedGraph`] has a
//! fixed number of lenders decided at construction time. In other cases there
//! is a default: for example, a [splittable graph] will return as many lenders
//! as the current number of Rayon threads.
//!
//! In some cases, however, it is desirable to alter this behavior: for
//! example, you might want to split the lenders [by the overall number of
//! successors they will return], rather than by the number of nodes.
//!
//! [splittable graph]: SplitLabeling
//! [by the overall number of successors they will return]: ParallelDcfGraph

use crate::prelude::*;
use lender::*;
use sux::{traits::SuccUnchecked, utils::FairChunks};

/// A wrapper that overrides the number of lenders returned by
/// [`IntoParLenders::into_par_lenders`] to a fixed number of lenders returning
/// the same number of nodes (except possibly the last one, which may be
/// smaller).
///
/// # Examples
///
/// ```rust
/// # use webgraph::prelude::*;
/// # use dsi_bitstream::prelude::BE;
/// # use tempfile::Builder;
/// # fn main() -> anyhow::Result<()> {
/// # let tempdir = Builder::new().prefix("test").tempdir()?;
/// # let basename = tempdir.path().join("basename");
/// // A VecGraph
/// let graph = VecGraph::from_arcs([(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)]);
///
/// // This is now a sorted graph ready to be compressed in parallel
/// // using exactly 2 lenders approximately of the same size, instead
/// // of the default number of lenders (the number of Rayon threads)
/// let sorted = ParSortedGraph::from(ParUniformGraph::new(graph, 2))?;
///
/// // This will compress the graph in parallel
/// BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ParUniformGraph<G>(pub G, usize);

impl<G> ParUniformGraph<G> {
    /// Creates a new [`ParGraph`] with the given inner graph and
    /// number of lenders.
    pub fn new(graph: G, num_lenders: usize) -> Self {
        assert!(num_lenders > 0, "the number of lenders must be positive");
        Self(graph, num_lenders)
    }
}

impl<G: SequentialLabeling> SequentialLabeling for ParUniformGraph<G> {
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

impl<G: SequentialGraph> SequentialGraph for ParUniformGraph<G> {}

impl<G: RandomAccessLabeling> RandomAccessLabeling for ParUniformGraph<G> {
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

impl<G: RandomAccessGraph> RandomAccessGraph for ParUniformGraph<G> {}

impl<'a, G: SequentialLabeling + SplitLabeling> IntoParLenders for &'a ParUniformGraph<G>
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
impl<'b, G: SequentialLabeling> IntoLender for &'b ParUniformGraph<G> {
    type Lender = <G as SequentialLabeling>::Lender<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

/// A wrapper that overrides the number of lenders returned by
/// [`IntoParLenders::into_par_lenders`] to a fixed number of lenders returning
/// approximately the same number of arcs (except possibly the last one, which
/// may be smaller).
///
/// The cutpoints are computed once at construction time from a degree
/// cumulative function (DCF), which is not retained afterwards.
/// A wrapper that overrides the number of lenders returned by
/// [`IntoParLenders::into_par_lenders`] to a fixed number of lenders returning
/// the same number of nodes (except possibly the last one, which may be
/// smaller).
///
/// # Examples
///
/// ```rust
/// # use webgraph::prelude::*;
/// # use dsi_bitstream::prelude::BE;
/// # use tempfile::Builder;
/// # fn main() -> anyhow::Result<()> {
/// # let tempdir = Builder::new().prefix("test").tempdir()?;
/// # let basename = tempdir.path().join("basename");
/// // A VecGraph
/// let graph = VecGraph::from_arcs([(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)]);
/// let dcf = graph.build_dcf();
/// let num_arcs = graph.num_arcs();
///
/// // This is now a sorted graph ready to be compressed in parallel
/// // using exactly 2 lenders returning approximately the same overall
/// // number of arcs, instead of lenders returning approximately the
/// // the number of nodes, as it happens with ParUniformGraph.
/// let sorted = ParSortedGraph::from(ParDcfGraph::new(graph, num_arcs, dcf, 2))?;
///
/// // This will compress the graph in parallel
/// BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ParDcfGraph<G> {
    graph: G,
    cutpoints: Vec<usize>,
}

impl<G> ParDcfGraph<G> {
    /// Creates a new [`ParDcfGraph`] with the given inner graph,
    /// degree cumulative function, and number of lenders.
    ///
    /// We require explicitly the number of arcs to support also
    /// sequential graphs for which the number is known.
    ///
    /// The cutpoints are computed immediately from the DCF using
    /// [`FairChunks`]; the DCF is not stored.
    pub fn new<D>(graph: G, num_arcs: u64, dcf: D, num_lenders: usize) -> Self
    where
        G: SequentialLabeling,
        D: for<'b> SuccUnchecked<Input = u64, Output<'b> = u64>,
    {
        assert!(num_lenders > 0, "the number of lenders must be positive");
        let num_nodes = graph.num_nodes();
        let target = num_arcs.div_ceil(num_lenders as u64);
        let cutpoints: Vec<usize> = std::iter::once(0)
            .chain(FairChunks::new_with(target, dcf, num_nodes, num_arcs).map(|r| r.end))
            .collect();
        Self { graph, cutpoints }
    }
}

impl<G: SequentialLabeling> SequentialLabeling for ParDcfGraph<G> {
    type Label = G::Label;
    type Lender<'node>
        = G::Lender<'node>
    where
        Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        self.graph.num_arcs_hint()
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        self.graph.iter_from(from)
    }
}

impl<G: SequentialGraph> SequentialGraph for ParDcfGraph<G> {}

impl<G: RandomAccessLabeling> RandomAccessLabeling for ParDcfGraph<G> {
    type Labels<'succ>
        = G::Labels<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.graph.num_arcs()
    }

    #[inline(always)]
    fn labels(&self, node_id: usize) -> Self::Labels<'_> {
        self.graph.labels(node_id)
    }

    #[inline(always)]
    fn outdegree(&self, node_id: usize) -> usize {
        self.graph.outdegree(node_id)
    }
}

impl<G: RandomAccessGraph> RandomAccessGraph for ParDcfGraph<G> {}

impl<'a, G> IntoParLenders for &'a ParDcfGraph<G>
where
    G: SequentialLabeling + SplitLabeling,
    for<'b> <G as SplitLabeling>::SplitLender<'b>: ExactSizeLender + FusedLender,
{
    type ParLender = <G as SplitLabeling>::SplitLender<'a>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let boundaries: Box<[usize]> = self.cutpoints.iter().copied().collect();
        let lenders: Box<[_]> = self
            .graph
            .split_iter_at(self.cutpoints.clone())
            .into_iter()
            .collect();
        (lenders, boundaries)
    }
}

/// Convenience implementation that makes it possible to iterate
/// over the graph using the [`for_`] macro
/// (see the [crate documentation]).
///
/// [crate documentation]: crate
impl<'b, G: SequentialLabeling> IntoLender for &'b ParDcfGraph<G> {
    type Lender = <G as SequentialLabeling>::Lender<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}
