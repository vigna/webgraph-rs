/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Wrappers that overrides the default result of
//! [`IntoParLenders::into_par_lenders`].
//!
//! A graph can have an intrinsic number of parallel lenders returned by
//! [`IntoParLenders::into_par_lenders`]: for example a [`ParSortedGraph`] has a
//! fixed number of lenders decided at construction time. In other cases there
//! is a default: for example, a [splittable graph] will return as many lenders
//! as the current number of Rayon threads.
//!
//! [`ParGraph`] lets you override this behavior by specifying either a fixed
//! number of uniform partitions or explicit cutpoints (e.g., computed from a
//! degree cumulative function).
//!
//! [splittable graph]: SplitLabeling

use crate::prelude::*;
use lender::*;
use sux::{traits::SuccUnchecked, utils::FairChunks};

/// How to partition the graph for parallel lenders.
#[derive(Debug, Clone)]
enum Splitting {
    /// Pre-computed cutpoints (e.g., from a DCF or CLI).
    Cutpoints(Box<[usize]>),
    /// Number of uniform partitions; cutpoints computed on the fly.
    Uniform(usize),
}

/// A wrapper that overrides the number and boundaries of lenders returned by
/// [`IntoParLenders::into_par_lenders`].
///
/// A `ParGraph` can be constructed in two ways:
///
/// - [`new`] splits nodes into a given number of approximately equal parts;
///
/// - [`with_cutpoints`] uses user-defined cutpoints;
///
/// - [`with_dcf`] uses a distributive cumulative function to compute
///   cutpoints providing approximately the same number of arcs.
///
/// # Examples
///
/// Uniform splitting into a fixed number of parts:
///
/// ```rust
/// # use webgraph::prelude::*;
/// # use dsi_bitstream::prelude::BE;
/// # use tempfile::Builder;
/// # fn main() -> anyhow::Result<()> {
/// # let tempdir = Builder::new().prefix("test").tempdir()?;
/// # let basename = tempdir.path().join("basename");
/// let graph = VecGraph::from_arcs([(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)]);
///
/// // Compress with exactly 2 lenders of approximately equal size
/// let sorted = ParSortedGraph::from_graph(ParGraph::new(graph, 2))?;
/// BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
/// # Ok(())
/// # }
/// ```
///
/// DCF-based splitting for arc-balanced partitions:
///
/// ```rust
/// # use webgraph::prelude::*;
/// # use dsi_bitstream::prelude::BE;
/// # use tempfile::Builder;
/// # fn main() -> anyhow::Result<()> {
/// # let tempdir = Builder::new().prefix("test").tempdir()?;
/// # let basename = tempdir.path().join("basename");
/// let graph = VecGraph::from_arcs([(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)]);
/// let dcf = graph.build_dcf();
/// let num_arcs = graph.num_arcs();
///
/// // Compress with 2 lenders returning approximately the same number of arcs
/// let sorted = ParSortedGraph::from_graph(ParGraph::with_dcf(graph, num_arcs, dcf, 2))?;
/// BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
/// # Ok(())
/// # }
/// ```
///
/// [`new`]: ParGraph::new
/// [`with_cutpoints`]: ParGraph::with_cutpoints
/// [`with_dcf`]: ParGraph::with_dcf
#[derive(Debug, Clone)]
pub struct ParGraph<G> {
    graph: G,
    splitting: Splitting,
}

impl<G> ParGraph<G> {
    /// Creates a new [`ParGraph`] that splits nodes into `num_lenders`
    /// approximately equal parts.
    pub fn new(graph: G, num_lenders: usize) -> Self {
        assert!(num_lenders > 0, "the number of lenders must be positive");
        Self {
            graph,
            splitting: Splitting::Uniform(num_lenders),
        }
    }

    /// Creates a new [`ParGraph`] with pre-computed cutpoints.
    ///
    /// The cutpoints must be a non-decreasing sequence starting at 0
    /// and ending at the number of nodes of the graph.
    pub fn with_cutpoints(graph: G, cutpoints: Vec<usize>) -> Self {
        Self {
            graph,
            splitting: Splitting::Cutpoints(cutpoints.into_boxed_slice()),
        }
    }

    /// Creates a new [`ParGraph`] with cutpoints computed from a degree
    /// cumulative function (DCF).
    ///
    /// We require explicitly the number of arcs to support also
    /// sequential graphs for which the number is known.
    ///
    /// The cutpoints are computed immediately from the DCF using
    /// [`FairChunks`]; the DCF is not stored.
    pub fn with_dcf<D>(graph: G, num_arcs: u64, dcf: D, num_lenders: usize) -> Self
    where
        G: SequentialLabeling,
        D: for<'b> SuccUnchecked<Input = u64, Output<'b> = u64>,
    {
        assert!(num_lenders > 0, "the number of lenders must be positive");
        let num_nodes = graph.num_nodes();
        let target = num_arcs.div_ceil(num_lenders as u64);
        let cutpoints: Box<[usize]> = std::iter::once(0)
            .chain(FairChunks::new_with(target, dcf, num_nodes, num_arcs).map(|r| r.end))
            .collect();
        Self {
            graph,
            splitting: Splitting::Cutpoints(cutpoints),
        }
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

impl<G: SequentialGraph> SequentialGraph for ParGraph<G> {}

impl<G: RandomAccessLabeling> RandomAccessLabeling for ParGraph<G> {
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

impl<G: RandomAccessGraph> RandomAccessGraph for ParGraph<G> {}

impl<G: SplitLabeling> SplitLabeling for ParGraph<G> {
    type SplitLender<'a>
        = G::SplitLender<'a>
    where
        Self: 'a;
    type IntoIterator<'a>
        = G::IntoIterator<'a>
    where
        Self: 'a;

    fn split_iter_at(&self, cutpoints: impl IntoIterator<Item = usize>) -> Self::IntoIterator<'_> {
        self.graph.split_iter_at(cutpoints)
    }
}

impl<'a, G> IntoParLenders for &'a ParGraph<G>
where
    G: SequentialLabeling + SplitLabeling,
    for<'b> <G as SplitLabeling>::SplitLender<'b>: ExactSizeLender + FusedLender,
{
    type ParLender = <G as SplitLabeling>::SplitLender<'a>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        match &self.splitting {
            Splitting::Cutpoints(cp) => {
                let lenders: Box<[_]> = self
                    .graph
                    .split_iter_at(cp.iter().copied())
                    .into_iter()
                    .collect();
                (lenders, cp.clone())
            }
            Splitting::Uniform(n) => {
                let n = *n;
                let num_nodes = self.graph.num_nodes();
                let step = num_nodes.div_ceil(n);
                let boundaries: Box<[usize]> = (0..=n).map(|i| (i * step).min(num_nodes)).collect();
                let lenders: Box<[_]> = self.graph.split_iter(n).into_iter().collect();
                (lenders, boundaries)
            }
        }
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
