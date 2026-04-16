/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! A labeled graph built by sorting labeled pairs of nodes.

use crate::graphs::arc_list_graph;
use crate::prelude::*;
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::sort_pairs::KMergeIters;
use crate::utils::{BatchCodec, CodecIter, MemoryUsage};
use anyhow::Result;
use lender::*;
use std::iter::Flatten;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// A labeled graph representation built by sorting labeled arc pairs.
///
/// Stores partition boundaries and sorted iterators for each partition.
/// The number of nodes is derived from the last boundary value.
///
/// A `SortedLabeledGraph` can be built from any labeled
/// [`SequentialLabeling`] using [`sort`](SortedLabeledGraphConfig::sort)
/// (sequential sort) or [`par_sort`](SortedLabeledGraphConfig::par_sort)
/// (parallel sort from a [`SplitLabeling`]). In both cases, the result
/// implements [`IntoParIters`].
///
/// The label serialization format is controlled by the [`BatchCodec`]
/// passed to the constructor.
pub struct SortedLabeledGraph<L, I> {
    boundaries: Box<[usize]>,
    iters: Box<[I]>,
    _phantom: PhantomData<L>,
}

type SeqIter<'a, I> = Flatten<std::iter::Cloned<std::slice::Iter<'a, I>>>;

/// Configuration for building a [`SortedLabeledGraph`].
///
/// Obtained via [`SortedLabeledGraph::config()`]. Use the setter methods
/// to customize partitioning and memory, then call
/// [`sort`](Self::sort) or [`par_sort`](Self::par_sort) to perform the sort.
pub struct SortedLabeledGraphConfig {
    num_partitions: NonZeroUsize,
    memory_usage: MemoryUsage,
}

impl SortedLabeledGraphConfig {
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

    /// Sorts labeled arcs from a [`SequentialLabeling`] sequentially,
    /// producing a partitioned [`SortedLabeledGraph`].
    ///
    /// The graph is iterated once; pairs are partitioned and sorted in a
    /// single pass using [`ParSortIters`] with one input iterator.
    pub fn sort<C, G>(
        self,
        graph: G,
        batch_codec: C,
    ) -> Result<SortedLabeledGraph<C::Label, KMergeIters<CodecIter<C>, C::Label>>>
    where
        C: BatchCodec,
        G: LabeledSequentialGraph<C::Label>,
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
        let split = par_sort.sort_labeled(batch_codec, [graph.iter().into_labeled_pairs()])?;
        Ok(SortedLabeledGraph {
            boundaries: split.boundaries,
            iters: split.iters,
            _phantom: PhantomData,
        })
    }

    /// Sorts labeled arcs from a splittable [`SequentialLabeling`] in
    /// parallel, producing a partitioned [`SortedLabeledGraph`].
    ///
    /// The graph is split via [`SplitLabeling`] and each split is sorted
    /// concurrently using [`ParSortIters`].
    pub fn par_sort<C, G>(
        self,
        graph: G,
        batch_codec: C,
    ) -> Result<SortedLabeledGraph<C::Label, KMergeIters<CodecIter<C>, C::Label>>>
    where
        C: BatchCodec,
        G: LabeledSequentialGraph<C::Label>
            + for<'a> SplitLabeling<
                SplitLender<'a>: for<'b> NodeLabelsLender<
                    'b,
                    Label: Pair<Left = usize, Right = C::Label> + Copy,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                > + Send
                    + Sync,
            >,
        CodecIter<C>: Clone + Send + Sync,
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
            .map(|iter| iter.into_labeled_pairs())
            .collect();

        let split = par_sort.sort_labeled(batch_codec, pairs)?;
        Ok(SortedLabeledGraph {
            boundaries: split.boundaries,
            iters: split.iters,
            _phantom: PhantomData,
        })
    }
}

impl<L, I> SortedLabeledGraph<L, I> {
    /// Creates a [`SortedLabeledGraph`] from pre-sorted partition
    /// boundaries and iterators.
    pub fn from_parts(boundaries: Box<[usize]>, iters: Box<[I]>) -> Self {
        SortedLabeledGraph {
            boundaries,
            iters,
            _phantom: PhantomData,
        }
    }

    /// Decomposes the [`SortedLabeledGraph`] into its partition boundaries
    /// and iterators.
    pub fn into_parts(self) -> (Box<[usize]>, Box<[I]>) {
        (self.boundaries, self.iters)
    }

    /// Returns a [`SortedLabeledGraphConfig`] with default settings for
    /// customization via chained setters.
    pub fn config() -> SortedLabeledGraphConfig {
        SortedLabeledGraphConfig {
            num_partitions: NonZeroUsize::new(rayon::current_num_threads())
                .expect("Number of Rayon threads should be non-zero"),
            memory_usage: MemoryUsage::default(),
        }
    }

    /// Sorts labeled arcs from a [`LabeledSequentialGraph`] sequentially
    /// with default settings, producing a partitioned
    /// [`SortedLabeledGraph`].
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().sort(graph, batch_codec)`.
    pub fn new<C, G>(graph: G, batch_codec: C) -> Result<SortedLabeledGraph<C::Label, KMergeIters<CodecIter<C>, C::Label>>>
    where
        C: BatchCodec,
        G: LabeledSequentialGraph<C::Label>,
        for<'a> <G as SequentialLabeling>::Lender<'a>: Send + Sync,
        for<'a, 'b> LenderIntoIter<'b, <G as SequentialLabeling>::Lender<'a>>: Send + Sync,
    {
        Self::config().sort(graph, batch_codec)
    }

    /// Sorts labeled arcs from a splittable [`LabeledSequentialGraph`] in
    /// parallel with default settings, producing a partitioned
    /// [`SortedLabeledGraph`].
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().par_sort(graph, batch_codec)`.
    pub fn par_new<C, G>(graph: G, batch_codec: C) -> Result<SortedLabeledGraph<C::Label, KMergeIters<CodecIter<C>, C::Label>>>
    where
        C: BatchCodec,
        G: LabeledSequentialGraph<C::Label>
            + for<'a> SplitLabeling<
                SplitLender<'a>: for<'b> NodeLabelsLender<
                    'b,
                    Label: Pair<Left = usize, Right = C::Label> + Copy,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                > + Send
                    + Sync,
            >,
        CodecIter<C>: Clone + Send + Sync,
    {
        Self::config().par_sort(graph, batch_codec)
    }
}

// === SequentialLabeling ===

impl<L: Clone + Copy + 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync>
    SequentialLabeling for SortedLabeledGraph<L, I>
{
    type Label = (usize, L);
    type Lender<'node>
        = arc_list_graph::NodeLabels<L, SeqIter<'node, I>>
    where
        Self: 'node;

    #[inline]
    fn num_nodes(&self) -> usize {
        *self.boundaries.last().unwrap_or(&0)
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        let num_nodes = self.num_nodes();
        let iter = self.iters.iter().cloned().flatten();
        let mut lender = arc_list_graph::NodeLabels::new(num_nodes, iter);
        lender.advance_by(from).unwrap();
        lender
    }
}

// === IntoParIters (owned — consumes iterators, no Clone needed) ===

impl<L: Clone + Copy + Send + Sync + 'static, I: Iterator<Item = ((usize, usize), L)> + Send + Sync>
    IntoParIters for SortedLabeledGraph<L, I>
{
    type Label = (usize, L);
    type ParLender = arc_list_graph::NodeLabels<L, I>;

    fn into_par_iters(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let boundaries = self.boundaries;
        let lenders: Box<[_]> = self
            .iters
            .into_vec()
            .into_iter()
            .enumerate()
            .map(|(i, iter)| {
                let start = boundaries[i];
                let end = boundaries[i + 1];
                let num_partition_nodes = end - start;
                arc_list_graph::NodeLabels::try_new_from(num_partition_nodes, iter, start)
                    .expect("Iterator should start from the expected first node")
            })
            .collect();
        (lenders, boundaries)
    }
}

// === IntoParIters (reference — clones iterators) ===

impl<
    L: Clone + Copy + Send + Sync + 'static,
    I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync,
> IntoParIters for &SortedLabeledGraph<L, I>
{
    type Label = (usize, L);
    type ParLender = arc_list_graph::NodeLabels<L, I>;

    fn into_par_iters(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let lenders: Box<[_]> = self
            .iters
            .iter()
            .enumerate()
            .map(|(i, iter)| {
                let start = self.boundaries[i];
                let end = self.boundaries[i + 1];
                let num_partition_nodes = end - start;
                arc_list_graph::NodeLabels::try_new_from(
                    num_partition_nodes,
                    iter.clone(),
                    start,
                )
                .expect("Iterator should start from the expected first node")
            })
            .collect();
        (lenders, self.boundaries.clone())
    }
}

// === IntoLender / Lending ===

impl<'a, L: Clone + Copy + 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync>
    IntoLender for &'a SortedLabeledGraph<L, I>
{
    type Lender = <SortedLabeledGraph<L, I> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<
    'lend,
    L: Clone + Copy + 'static,
    I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync,
> Lending<'lend> for &SortedLabeledGraph<L, I>
{
    type Lend = Lend<'lend, <SortedLabeledGraph<L, I> as SequentialLabeling>::Lender<'lend>>;
}
