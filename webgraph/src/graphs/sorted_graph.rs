/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! A graph built by sorting pairs of nodes.

use crate::graphs::arc_list_graph;
use crate::labels::proj::LeftIterator;
use crate::prelude::*;
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::par_sort_pairs::ParSortPairs;
use crate::utils::{MemoryUsage, SortedPairIter};
use anyhow::Result;
use lender::*;
use std::iter::Flatten;
use std::num::NonZeroUsize;

/// A graph representation built by sorting arc pairs.
///
/// Stores partition boundaries and sorted iterators for each partition.
/// The number of nodes is derived from the last boundary value.
///
/// A `SortedGraph` can be built from any [`SequentialGraph`] using
/// [`new`](SortedGraph::new) (sequential sort) or
/// [`par_new`](SortedGraph::par_new) (parallel sort from a
/// [`SplitLabeling`]). In both cases, the result implements
/// [`IntoParIters`], making it suitable for parallel compression via
/// [`BvCompConfig::par_comp`](crate::graphs::bvgraph::BvCompConfig::par_comp).
///
/// # Examples
///
/// ```ignore
/// // Sequential sort with defaults
/// let sorted = SortedGraph::new(PermutedGraph::new(&graph, &perm))?;
/// BvComp::with_basename("out").par_comp::<BE, _>(&sorted)?;
///
/// // Parallel sort with custom config
/// let sorted = SortedGraph::config()
///     .num_partitions(NonZeroUsize::new(8).unwrap())
///     .memory_usage(MemoryUsage::Percentage(0.5))
///     .par_sort(graph)?;
/// ```
pub struct SortedGraph<I> {
    boundaries: Box<[usize]>,
    iters: Box<[I]>,
}

type SeqIter<'a, I> = Flatten<std::iter::Cloned<std::slice::Iter<'a, I>>>;

#[allow(clippy::type_complexity)]
type MapFn = fn((usize, usize)) -> ((usize, usize), ());

/// Configuration for building a [`SortedGraph`].
///
/// Obtained via [`SortedGraph::config()`]. Use the setter methods to
/// customize partitioning and memory, then call [`sort`](Self::sort)
/// or [`par_sort`](Self::par_sort) to perform the sort.
pub struct SortedGraphConfig {
    num_partitions: NonZeroUsize,
    memory_usage: MemoryUsage,
}

impl SortedGraphConfig {
    /// Sets the number of output partitions.
    ///
    /// Defaults to [`rayon::current_num_threads`].
    pub const fn num_partitions(mut self, n: NonZeroUsize) -> Self {
        self.num_partitions = n;
        self
    }

    /// Sets the memory budget for in-memory sorting.
    ///
    /// Defaults to [`MemoryUsage::default`].
    pub const fn memory_usage(mut self, m: MemoryUsage) -> Self {
        self.memory_usage = m;
        self
    }

    /// Sorts arcs from a [`SequentialGraph`] sequentially, producing a
    /// partitioned [`SortedGraph`].
    ///
    /// The graph is iterated once; pairs are partitioned and sorted in a
    /// single pass using [`ParSortIters`] with one input iterator.
    pub fn sort<G: SequentialGraph>(self, graph: G) -> Result<SortedGraph<SortedPairIter>>
    where
        for<'a> <G as SequentialLabeling>::Lender<'a>: Send + Sync,
        for<'a, 'b> LenderIntoIter<'b, <G as SequentialLabeling>::Lender<'a>>: Send + Sync,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();

        let mut par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);

        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }

        // Single input iterator — sorting is sequential, output is partitioned
        let split = par_sort.sort([graph.iter().into_pairs()])?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }

    /// Sorts pairs from a sequential iterator, producing a partitioned
    /// [`SortedGraph`].
    ///
    /// The pairs are iterated once; they are partitioned and sorted in a
    /// single pass using [`ParSortIters`] with one input iterator.
    pub fn sort_pairs(
        self,
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + Send + Sync,
    ) -> Result<SortedGraph<SortedPairIter>> {
        let par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);
        let split = par_sort.sort([pairs])?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }

    /// Sorts pairs from a parallel iterator, producing a partitioned
    /// [`SortedGraph`].
    ///
    /// Uses [`ParSortPairs`] to sort in parallel.
    pub fn par_sort_pairs(
        self,
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = (usize, usize)>,
    ) -> Result<SortedGraph<SortedPairIter>> {
        let par_sort = ParSortPairs::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);
        let split = par_sort.sort(pairs)?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }

    /// Sorts arcs from a splittable [`SequentialGraph`] in parallel,
    /// producing a partitioned [`SortedGraph`].
    ///
    /// The graph is split via [`SplitLabeling`] and each split is
    /// sorted concurrently using [`ParSortIters`].
    pub fn par_sort<G>(self, graph: G) -> Result<SortedGraph<SortedPairIter>>
    where
        G: SequentialGraph
            + for<'g, 'a> SplitLabeling<
                SplitLender<'g>: NodeLabelsLender<
                    'a,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                >,
            >,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();

        let mut par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);

        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }

        let pairs: Vec<_> = graph
            .split_iter(rayon::current_num_threads())
            .into_iter()
            .map(|iter| iter.into_pairs())
            .collect();

        let split = par_sort.sort(pairs)?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }
}

impl SortedGraph<SortedPairIter> {
    /// Sorts arcs from a [`SequentialGraph`] sequentially with default
    /// settings, producing a partitioned [`SortedGraph`].
    ///
    /// Equivalent to `SortedGraph::config().sort(graph)`.
    pub fn new<G: SequentialGraph>(graph: G) -> Result<Self>
    where
        for<'a> <G as SequentialLabeling>::Lender<'a>: Send + Sync,
        for<'a, 'b> LenderIntoIter<'b, <G as SequentialLabeling>::Lender<'a>>: Send + Sync,
    {
        Self::config().sort(graph)
    }

    /// Sorts pairs from a sequential iterator with default settings,
    /// producing a partitioned [`SortedGraph`].
    ///
    /// Equivalent to `SortedGraph::config().sort_pairs(num_nodes, pairs)`.
    pub fn from_pairs(
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + Send + Sync,
    ) -> Result<Self> {
        Self::config().sort_pairs(num_nodes, pairs)
    }

    /// Sorts pairs from a parallel iterator with default settings,
    /// producing a partitioned [`SortedGraph`].
    ///
    /// Equivalent to `SortedGraph::config().par_sort_pairs(num_nodes, pairs)`.
    pub fn par_from_pairs(
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = (usize, usize)>,
    ) -> Result<Self> {
        Self::config().par_sort_pairs(num_nodes, pairs)
    }

    /// Sorts arcs from a splittable [`SequentialGraph`] in parallel with
    /// default settings, producing a partitioned [`SortedGraph`].
    ///
    /// Equivalent to `SortedGraph::config().par_sort(graph)`.
    pub fn par_new<G>(graph: G) -> Result<Self>
    where
        G: SequentialGraph
            + for<'g, 'a> SplitLabeling<
                SplitLender<'g>: NodeLabelsLender<
                    'a,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                >,
            >,
    {
        Self::config().par_sort(graph)
    }

    /// Returns a [`SortedGraphConfig`] with default settings for
    /// customization via chained setters.
    pub fn config() -> SortedGraphConfig {
        SortedGraphConfig {
            num_partitions: NonZeroUsize::new(rayon::current_num_threads())
                .expect("Number of Rayon threads should be non-zero"),
            memory_usage: MemoryUsage::default(),
        }
    }
}

impl<I> SortedGraph<I> {
    /// Creates a [`SortedGraph`] from pre-sorted partition boundaries and
    /// iterators.
    pub fn from_parts(boundaries: Box<[usize]>, iters: Box<[I]>) -> Self {
        SortedGraph { boundaries, iters }
    }

    /// Decomposes the [`SortedGraph`] into its partition boundaries and
    /// iterators.
    pub fn into_parts(self) -> (Box<[usize]>, Box<[I]>) {
        (self.boundaries, self.iters)
    }
}

// === SequentialLabeling ===

impl<I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> SequentialLabeling
    for SortedGraph<I>
{
    type Label = usize;
    type Lender<'node>
        = LeftIterator<arc_list_graph::NodeLabels<(), std::iter::Map<SeqIter<'node, I>, MapFn>>>
    where
        Self: 'node;

    #[inline]
    fn num_nodes(&self) -> usize {
        *self.boundaries.last().unwrap_or(&0)
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        let num_nodes = self.num_nodes();
        let map_fn: MapFn = |pair| (pair, ());
        let iter = self.iters.iter().cloned().flatten().map(map_fn);
        let mut lender = LeftIterator(arc_list_graph::NodeLabels::new(num_nodes, iter));
        lender.advance_by(from).unwrap();
        lender
    }
}

impl<I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> SequentialGraph for SortedGraph<I> {}

/// Creates lenders from an iterator of pair-iterators and their boundaries.
fn make_lenders<I: Iterator<Item = (usize, usize)> + Send + Sync>(
    iters: impl IntoIterator<Item = I>,
    boundaries: &[usize],
) -> Box<[LeftIterator<arc_list_graph::NodeLabels<(), std::iter::Map<I, MapFn>>>]> {
    let map_fn: MapFn = |pair| (pair, ());
    iters
        .into_iter()
        .enumerate()
        .map(|(i, iter)| {
            LeftIterator(
                arc_list_graph::NodeLabels::try_new_from(
                    boundaries[i + 1] - boundaries[i],
                    iter.map(map_fn),
                    boundaries[i],
                )
                .expect("Iterator should start from the expected first node"),
            )
        })
        .collect()
}

// === IntoParIters (owned — consumes iterators, no Clone needed) ===

impl<I: Iterator<Item = (usize, usize)> + Send + Sync> IntoParIters for SortedGraph<I> {
    type Label = usize;
    type ParLender = LeftIterator<arc_list_graph::NodeLabels<(), std::iter::Map<I, MapFn>>>;

    fn into_par_iters(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let lenders = make_lenders(self.iters.into_vec(), &self.boundaries);
        (lenders, self.boundaries)
    }
}

// === IntoParIters (reference — clones iterators) ===

impl<I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> IntoParIters for &SortedGraph<I> {
    type Label = usize;
    type ParLender = LeftIterator<arc_list_graph::NodeLabels<(), std::iter::Map<I, MapFn>>>;

    fn into_par_iters(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let lenders = make_lenders(self.iters.iter().cloned(), &self.boundaries);
        (lenders, self.boundaries.clone())
    }
}

// === IntoLender / Lending ===

impl<'a, I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> IntoLender
    for &'a SortedGraph<I>
{
    type Lender = <SortedGraph<I> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<'lend, I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> Lending<'lend>
    for &SortedGraph<I>
{
    type Lend = Lend<'lend, <SortedGraph<I> as SequentialLabeling>::Lender<'lend>>;
}
