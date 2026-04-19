/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Graphs and labelings built by sorting pairs of nodes.

use crate::graphs::arc_list_graph;
use crate::labels::proj::LeftIterator;
use crate::prelude::*;
use crate::traits::{BitDeserializer, BitSerializer};
use crate::utils::grouped_gaps;
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::par_sort_pairs::ParSortPairs;
use crate::utils::sort_pairs::KMergeIters;
use crate::utils::{BitReader, BitWriter, CodecIter, DefaultBatchCodec, MemoryUsage};
use anyhow::Result;
use dsi_bitstream::prelude::NE;
use lender::*;
use std::iter::Flatten;
use std::marker::PhantomData;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SortedLabeledGraph
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A labeled graph representation built by sorting labeled arc pairs.
///
/// Stores partition boundaries and sorted iterators for each partition.
/// The number of nodes is derived from the last boundary value.
///
/// A `SortedLabeledGraph` can be built from any labeled
/// [`SequentialLabeling`] using [`sort`](SortedLabeledGraphConfig::sort)
/// (sequential sort) or [`par_sort`](SortedLabeledGraphConfig::par_sort)
/// (parallel sort from a [`SplitLabeling`]). In both cases, the result
/// implements [`IntoParLenders`].
///
/// Labels are serialized and deserialized using a [`BitSerializer`] and
/// [`BitDeserializer`] pair passed to the constructor.
///
/// For the unlabeled case, use [`SortedGraph`], which is a transparent
/// wrapper around `SortedLabeledGraph<(), I>`.
pub struct ParSortedLabeledGraph<L, I> {
    boundaries: Box<[usize]>,
    iters: Box<[I]>,
    _phantom: PhantomData<L>,
}

type SeqIter<'a, I> = Flatten<std::iter::Cloned<std::slice::Iter<'a, I>>>;

impl<L, I> ParSortedLabeledGraph<L, I> {
    /// Creates a [`SortedLabeledGraph`] from pre-sorted partition
    /// boundaries and iterators.
    pub fn from_parts(boundaries: Box<[usize]>, iters: Box<[I]>) -> Self {
        ParSortedLabeledGraph {
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
        SortedLabeledGraphConfig::new()
    }

    /// Sorts labeled arcs from a [`LabeledSequentialGraph`] sequentially
    /// with default settings.
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().sort(graph, sd)`.
    pub fn from<SD, G>(
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
        G: LabeledSequentialGraph<SD::SerType>,
        for<'a> <G as SequentialLabeling>::Lender<'a>: Send + Sync,
        for<'a, 'b> LenderIntoIter<'b, <G as SequentialLabeling>::Lender<'a>>: Send + Sync,
    {
        SortedLabeledGraphConfig::new().sort(graph, sd)
    }

    /// Sorts labeled arcs from a splittable [`LabeledSequentialGraph`] in
    /// parallel with default settings.
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().par_sort(graph, sd)`.
    pub fn par_from<SD, G>(
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
        G: LabeledSequentialGraph<SD::SerType>
            + for<'a> SplitLabeling<
                SplitLender<'a>: for<'b> NodeLabelsLender<
                    'b,
                    Label: Pair<Left = usize, Right = SD::SerType> + Copy,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                > + Send
                                     + Sync,
            >,
    {
        SortedLabeledGraphConfig::new().par_sort(graph, sd)
    }

    /// Sorts labeled pairs from a sequential iterator with default
    /// settings.
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().sort_pairs(num_nodes, sd, pairs)`.
    pub fn from_pairs<SD>(
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = ((usize, usize), SD::SerType), IntoIter: Send + Sync>
        + Send
        + Sync,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
    {
        SortedLabeledGraphConfig::new().sort_pairs(num_nodes, sd, pairs)
    }

    /// Sorts labeled pairs from a parallel iterator with default
    /// settings.
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().par_sort_pairs(num_nodes, sd, pairs)`.
    pub fn par_from_pairs<SD>(
        num_nodes: usize,
        sd: SD,
        pairs: impl rayon::iter::ParallelIterator<Item = ((usize, usize), SD::SerType)>,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
    {
        SortedLabeledGraphConfig::new().par_sort_pairs(num_nodes, sd, pairs)
    }

    /// Sorts labeled arcs from a [`LabeledSequentialGraph`] sequentially
    /// with default settings.
    ///
    /// Unlike [`from`](Self::from), this method does not require `Send`
    /// or `Sync` on the graph's lenders or their items. The output is
    /// still partitioned for parallel compression.
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().sort_seq(graph, sd)`.
    pub fn from_seq<SD, G>(
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
        G: LabeledSequentialGraph<SD::SerType>,
    {
        SortedLabeledGraphConfig::new().sort_seq(graph, sd)
    }

    /// Sorts labeled pairs from a sequential iterator with default
    /// settings.
    ///
    /// Unlike [`from_pairs`](Self::from_pairs), this method does not
    /// require `Send` or `Sync` on the iterator. The output is still
    /// partitioned for parallel compression.
    ///
    /// Equivalent to
    /// `SortedLabeledGraph::config().sort_pairs_seq(num_nodes, sd, pairs)`.
    pub fn from_pairs_seq<SD>(
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = ((usize, usize), SD::SerType)>,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
    {
        SortedLabeledGraphConfig::new().sort_pairs_seq(num_nodes, sd, pairs)
    }
}

// === SequentialLabeling for SortedLabeledGraph ===

impl<L: Clone + Copy + 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync>
    SequentialLabeling for ParSortedLabeledGraph<L, I>
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

// === IntoParLenders for SortedLabeledGraph ===

/// Creates labeled lenders from an iterator of labeled-pair-iterators and
/// their boundaries.
fn make_labeled_lenders<
    L: Clone + Copy + 'static,
    I: Iterator<Item = ((usize, usize), L)> + Send + Sync,
>(
    iters: impl IntoIterator<Item = I>,
    boundaries: &[usize],
) -> Box<[arc_list_graph::NodeLabels<L, I>]> {
    iters
        .into_iter()
        .enumerate()
        .map(|(i, iter)| {
            arc_list_graph::NodeLabels::try_new_from(
                boundaries[i + 1] - boundaries[i],
                iter,
                boundaries[i],
            )
            .expect("Iterator should start from the expected first node")
        })
        .collect()
}

impl<L: Clone + Copy + Send + Sync + 'static, I: Iterator<Item = ((usize, usize), L)> + Send + Sync>
    IntoParLenders for ParSortedLabeledGraph<L, I>
{
    type ParLender = arc_list_graph::NodeLabels<L, I>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let lenders = make_labeled_lenders(self.iters.into_vec(), &self.boundaries);
        (lenders, self.boundaries)
    }
}

impl<
    L: Clone + Copy + Send + Sync + 'static,
    I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync,
> IntoParLenders for &ParSortedLabeledGraph<L, I>
{
    type ParLender = arc_list_graph::NodeLabels<L, I>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let lenders = make_labeled_lenders(self.iters.iter().cloned(), &self.boundaries);
        (lenders, self.boundaries.clone())
    }
}

// === IntoLender / Lending for SortedLabeledGraph ===

impl<'a, L: Clone + Copy + 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync>
    IntoLender for &'a ParSortedLabeledGraph<L, I>
{
    type Lender = <ParSortedLabeledGraph<L, I> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<
    'lend,
    L: Clone + Copy + 'static,
    I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync,
> Lending<'lend> for &ParSortedLabeledGraph<L, I>
{
    type Lend = Lend<'lend, <ParSortedLabeledGraph<L, I> as SequentialLabeling>::Lender<'lend>>;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SortedGraph — transparent wrapper around SortedLabeledGraph<(), I>
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A graph representation built by sorting arc pairs.
///
/// This is a transparent wrapper around
/// [`SortedLabeledGraph`]`<(), I>`, projecting away the unit label so
/// that [`Label`](SequentialLabeling::Label) is `usize`.
///
/// A `SortedGraph` can be built from any [`SequentialGraph`] using
/// [`from`](SortedGraph::from) (sequential sort) or
/// [`par_from`](SortedGraph::par_from) (parallel sort from a
/// [`SplitLabeling`]). In both cases, the result implements
/// [`IntoParLenders`], making it suitable for parallel compression via
/// [`BvCompConfig::par_comp`](crate::graphs::bvgraph::BvCompConfig::par_comp).
///
/// # Examples
///
/// ```ignore
/// // Sequential sort with defaults
/// let sorted = SortedGraph::from(PermutedGraph::new(&graph, &perm))?;
/// BvComp::with_basename("out").par_comp::<BE, _>(sorted)?;
///
/// // Parallel sort with custom config
/// let sorted = SortedGraph::config()
///     .num_partitions(8)
///     .memory_usage(MemoryUsage::Percentage(0.5))
///     .par_sort(graph)?;
/// ```
pub struct ParSortedGraph<I>(pub ParSortedLabeledGraph<(), I>);

/// The concrete iterator type for unlabeled sorted graphs.
///
/// This is `KMergeIters<CodecIter<DefaultBatchCodec>, ()>`, which
/// yields `((usize, usize), ())` pairs. The `()` label is projected
/// away by [`SortedGraph`]'s trait implementations.
pub type SortedPairIter = KMergeIters<CodecIter<DefaultBatchCodec>, ()>;

/// Internal codec type for labeled sorted graphs.
///
/// Users should not need to reference this type directly; the labeled
/// methods on [`SortedLabeledGraphConfig`] and [`SortedLabeledGraph`]
/// accept a single `SD` parameter implementing both [`BitSerializer`]
/// and [`BitDeserializer`].
pub(crate) type LabeledCodec<SD> = grouped_gaps::GroupedGapsCodec<
    NE,
    SD,
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::DELTA },
    false,
>;

/// The concrete iterator type for labeled sorted graphs.
///
/// This is the iterator returned by the terminal methods of
/// [`SortedLabeledGraphConfig`]. The type parameter `SD` implements both
/// [`BitSerializer`] and [`BitDeserializer`] for the label type.
/// Use [`BitSerDeser`](crate::traits::BitSerDeser) to combine
/// separate serializer and deserializer implementations.
pub type SortedLabeledIter<SD> = KMergeIters<
    grouped_gaps::GroupedGapsIter<
        NE,
        SD,
        { dsi_bitstream::dispatch::code_consts::GAMMA },
        { dsi_bitstream::dispatch::code_consts::GAMMA },
        { dsi_bitstream::dispatch::code_consts::DELTA },
    >,
    <SD as BitSerializer<NE, BitWriter<NE>>>::SerType,
>;

impl ParSortedGraph<SortedPairIter> {
    /// Creates a [`SortedGraph`] from pre-sorted partition boundaries and
    /// unlabeled pair iterators.
    ///
    /// The iterators yield `(usize, usize)` pairs; the `()` label is
    /// added internally.
    pub fn from_parts<J: Iterator<Item = (usize, usize)>>(
        boundaries: Box<[usize]>,
        iters: Box<[J]>,
    ) -> ParSortedGraph<std::iter::Map<J, fn((usize, usize)) -> ((usize, usize), ())>> {
        let labeled: Box<[_]> = iters
            .into_vec()
            .into_iter()
            .map(|iter| {
                let f: fn((usize, usize)) -> ((usize, usize), ()) = |pair| (pair, ());
                iter.map(f)
            })
            .collect();
        ParSortedGraph(ParSortedLabeledGraph::from_parts(boundaries, labeled))
    }
}

impl<I> ParSortedGraph<I> {
    /// Decomposes the [`SortedGraph`] into its partition boundaries and
    /// iterators.
    pub fn into_parts(self) -> (Box<[usize]>, Box<[I]>) {
        self.0.into_parts()
    }
}

impl ParSortedGraph<SortedPairIter> {
    /// Returns a [`SortedGraphConfig`] with default settings for
    /// customization via chained setters.
    pub fn config() -> SortedGraphConfig {
        SortedGraphConfig::new()
    }
}

impl ParSortedGraph<SortedPairIter> {
    /// Sorts arcs from a [`SequentialGraph`] sequentially with default
    /// settings.
    ///
    /// Equivalent to `SortedGraph::config().sort(graph)`.
    pub fn from<G: SequentialGraph>(graph: G) -> Result<Self>
    where
        for<'a> <G as SequentialLabeling>::Lender<'a>: Send + Sync,
        for<'a, 'b> LenderIntoIter<'b, <G as SequentialLabeling>::Lender<'a>>: Send + Sync,
    {
        SortedGraphConfig::new().sort(graph)
    }

    /// Sorts arcs from a graph implementing [`IntoParLenders`] in
    /// parallel with default settings.
    ///
    /// Equivalent to `SortedGraph::config().par_sort(graph)`.
    pub fn par_from<G>(graph: G) -> Result<Self>
    where
        G: SequentialGraph
            + IntoParLenders<
                ParLender: for<'a> NodeLabelsLender<
                    'a,
                    Label = usize,
                    IntoIterator: IntoIterator<IntoIter: Send>,
                >,
            >,
    {
        SortedGraphConfig::new().par_sort(graph)
    }

    /// Sorts pairs from a sequential iterator with default settings.
    ///
    /// Equivalent to `SortedGraph::config().sort_pairs(num_nodes, pairs)`.
    pub fn from_pairs(
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + Send + Sync,
    ) -> Result<Self> {
        SortedGraphConfig::new().sort_pairs(num_nodes, pairs)
    }

    /// Sorts pairs from a parallel iterator with default settings.
    ///
    /// Equivalent to `SortedGraph::config().par_sort_pairs(num_nodes, pairs)`.
    pub fn par_from_pairs(
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = (usize, usize)>,
    ) -> Result<Self> {
        SortedGraphConfig::new().par_sort_pairs(num_nodes, pairs)
    }

    /// Sorts arcs from a [`SequentialGraph`] sequentially with default
    /// settings.
    ///
    /// Unlike [`from`](Self::from), this method does not require `Send`
    /// or `Sync` on the graph's lenders or their items. The output is
    /// still partitioned for parallel compression.
    ///
    /// Equivalent to `SortedGraph::config().sort_seq(graph)`.
    pub fn from_seq<G: SequentialGraph>(graph: G) -> Result<Self> {
        SortedGraphConfig::new().sort_seq(graph)
    }

    /// Sorts pairs from a sequential iterator with default settings.
    ///
    /// Unlike [`from_pairs`](Self::from_pairs), this method does not
    /// require `Send` or `Sync` on the iterator. The output is still
    /// partitioned for parallel compression.
    ///
    /// Equivalent to `SortedGraph::config().sort_pairs_seq(num_nodes, pairs)`.
    pub fn from_pairs_seq(
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize)>,
    ) -> Result<Self> {
        SortedGraphConfig::new().sort_pairs_seq(num_nodes, pairs)
    }
}

// === SequentialLabeling for SortedGraph (projects away ()) ===

impl<I: Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync> SequentialLabeling
    for ParSortedGraph<I>
{
    type Label = usize;
    type Lender<'node>
        = LeftIterator<arc_list_graph::NodeLabels<(), SeqIter<'node, I>>>
    where
        Self: 'node;

    #[inline]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    #[inline]
    fn num_arcs_hint(&self) -> Option<u64> {
        self.0.num_arcs_hint()
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        LeftIterator(self.0.iter_from(from))
    }
}

impl<I: Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync> SequentialGraph
    for ParSortedGraph<I>
{
}

// === IntoParLenders for SortedGraph (wraps labeled lenders in LeftIterator) ===

impl<I: Iterator<Item = ((usize, usize), ())> + Send + Sync> IntoParLenders for ParSortedGraph<I> {
    type ParLender = LeftIterator<arc_list_graph::NodeLabels<(), I>>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let (lenders, boundaries) = self.0.into_par_lenders();
        let projected: Box<[_]> = lenders.into_vec().into_iter().map(LeftIterator).collect();
        (projected, boundaries)
    }
}

impl<I: Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync> IntoParLenders
    for &ParSortedGraph<I>
{
    type ParLender = LeftIterator<arc_list_graph::NodeLabels<(), I>>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let (lenders, boundaries) = (&self.0).into_par_lenders();
        let projected: Box<[_]> = lenders.into_vec().into_iter().map(LeftIterator).collect();
        (projected, boundaries)
    }
}

// === IntoLender / Lending for SortedGraph ===

impl<'a, I: Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync> IntoLender
    for &'a ParSortedGraph<I>
{
    type Lender = <ParSortedGraph<I> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<'lend, I: Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync> Lending<'lend>
    for &ParSortedGraph<I>
{
    type Lend = Lend<'lend, <ParSortedGraph<I> as SequentialLabeling>::Lender<'lend>>;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SortedGraphConfig
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Configuration for building a [`SortedGraph`].
///
/// This is a transparent wrapper around [`SortedLabeledGraphConfig`]
/// that forwards all methods with `SD = ()`, analogous to how
/// [`SortedGraph`] wraps [`SortedLabeledGraph`]`<(), I>`.
///
/// Obtained via [`SortedGraph::config()`]. Use the setter methods to
/// customize partitioning and memory, then call one of the terminal
/// methods to perform the sort.
pub struct SortedGraphConfig(pub SortedLabeledGraphConfig);

impl SortedGraphConfig {
    fn new() -> Self {
        SortedGraphConfig(SortedLabeledGraphConfig::new())
    }

    /// Sets the number of output partitions.
    ///
    /// Defaults to [`rayon::current_num_threads`].
    pub const fn num_partitions(self, n: usize) -> Self {
        SortedGraphConfig(self.0.num_partitions(n))
    }

    /// Sets the memory budget for in-memory sorting.
    ///
    /// Defaults to [`MemoryUsage::default`].
    pub const fn memory_usage(self, m: MemoryUsage) -> Self {
        SortedGraphConfig(self.0.memory_usage(m))
    }

    /// Sorts arcs from a [`SequentialGraph`] sequentially, producing a
    /// partitioned [`SortedGraph`].
    pub fn sort<G: SequentialGraph>(self, graph: G) -> Result<ParSortedGraph<SortedPairIter>>
    where
        for<'a> <G as SequentialLabeling>::Lender<'a>: Send + Sync,
        for<'a, 'b> LenderIntoIter<'b, <G as SequentialLabeling>::Lender<'a>>: Send + Sync,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();
        let mut par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.0.num_partitions)
            .memory_usage(self.0.memory_usage);
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }
        Ok(ParSortedGraph(
            par_sort
                .sort_labeled::<DefaultBatchCodec, _>(
                    DefaultBatchCodec::default(),
                    [graph.iter().into_pairs().map(|pair| (pair, ()))],
                )?
                .into(),
        ))
    }

    /// Sorts arcs from a graph implementing [`IntoParLenders`] in
    /// parallel, producing a partitioned [`SortedGraph`].
    pub fn par_sort<G>(self, graph: G) -> Result<ParSortedGraph<SortedPairIter>>
    where
        G: SequentialGraph
            + IntoParLenders<
                ParLender: for<'a> NodeLabelsLender<
                    'a,
                    Label = usize,
                    IntoIterator: IntoIterator<IntoIter: Send>,
                >,
            >,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();
        let mut par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.0.num_partitions)
            .memory_usage(self.0.memory_usage);
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }
        let (lenders, _boundaries) = graph.into_par_lenders();
        let pairs: Vec<_> = lenders
            .into_vec()
            .into_iter()
            .map(|lender| lender.into_pairs().map(|pair| (pair, ())))
            .collect();
        Ok(ParSortedGraph(
            par_sort
                .sort_labeled::<DefaultBatchCodec, _>(DefaultBatchCodec::default(), pairs)?
                .into(),
        ))
    }

    /// Sorts unlabeled pairs from a sequential iterator, producing a
    /// partitioned [`SortedGraph`].
    pub fn sort_pairs(
        self,
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + Send + Sync,
    ) -> Result<ParSortedGraph<SortedPairIter>> {
        Ok(ParSortedGraph(self.0.sort_pairs(
            num_nodes,
            (),
            pairs.into_iter().map(|pair| (pair, ())),
        )?))
    }

    /// Sorts unlabeled pairs from a parallel iterator, producing a
    /// partitioned [`SortedGraph`].
    pub fn par_sort_pairs(
        self,
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = (usize, usize)>,
    ) -> Result<ParSortedGraph<SortedPairIter>> {
        Ok(ParSortedGraph(self.0.par_sort_pairs(
            num_nodes,
            (),
            rayon::iter::ParallelIterator::map(pairs, |pair| (pair, ())),
        )?))
    }

    /// Sorts arcs from a [`SequentialGraph`] sequentially, producing a
    /// partitioned [`SortedGraph`].
    ///
    /// Unlike [`sort`](Self::sort), this method does not require `Send`
    /// or `Sync` on the graph's lenders or their items. The output is
    /// still partitioned for parallel compression.
    pub fn sort_seq<G: SequentialGraph>(self, graph: G) -> Result<ParSortedGraph<SortedPairIter>> {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();
        let mut par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.0.num_partitions)
            .memory_usage(self.0.memory_usage);
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }
        Ok(ParSortedGraph(
            par_sort
                .sort_labeled_seq::<DefaultBatchCodec, _>(
                    DefaultBatchCodec::default(),
                    graph.iter().into_pairs().map(|pair| (pair, ())),
                )?
                .into(),
        ))
    }

    /// Sorts unlabeled pairs from a sequential iterator, producing a
    /// partitioned [`SortedGraph`].
    ///
    /// Unlike [`sort_pairs`](Self::sort_pairs), this method does not
    /// require `Send` or `Sync` on the iterator. The output is still
    /// partitioned for parallel compression.
    pub fn sort_pairs_seq(
        self,
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize)>,
    ) -> Result<ParSortedGraph<SortedPairIter>> {
        Ok(ParSortedGraph(self.0.sort_pairs_seq(
            num_nodes,
            (),
            pairs.into_iter().map(|pair| (pair, ())),
        )?))
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SortedLabeledGraphConfig
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Configuration for building a [`SortedLabeledGraph`].
///
/// Obtained via [`SortedLabeledGraph::config()`]. Use the setter methods
/// to customize partitioning and memory, then call one of the terminal
/// methods to perform the sort.
pub struct SortedLabeledGraphConfig {
    num_partitions: usize,
    memory_usage: MemoryUsage,
}

impl Default for SortedLabeledGraphConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedLabeledGraphConfig {
    /// Creates a new [`SortedLabeledGraphConfig`] with default settings.
    pub fn new() -> Self {
        SortedLabeledGraphConfig {
            num_partitions: rayon::current_num_threads(),
            memory_usage: MemoryUsage::default(),
        }
    }

    /// Sets the number of output partitions.
    ///
    /// Defaults to [`rayon::current_num_threads`].
    pub const fn num_partitions(mut self, n: usize) -> Self {
        assert!(n > 0, "the number of partitions must be positive");
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

    /// Sorts labeled arcs from a [`LabeledSequentialGraph`] sequentially,
    /// producing a partitioned [`SortedLabeledGraph`].
    pub fn sort<SD, G>(
        self,
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
        G: LabeledSequentialGraph<SD::SerType>,
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
        let codec = LabeledCodec::new(sd);
        Ok(par_sort
            .sort_labeled(codec, [graph.iter().into_labeled_pairs()])?
            .into())
    }

    /// Sorts labeled arcs from a splittable [`LabeledSequentialGraph`] in
    /// parallel, producing a partitioned [`SortedLabeledGraph`].
    pub fn par_sort<SD, G>(
        self,
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
        G: LabeledSequentialGraph<SD::SerType>
            + for<'a> SplitLabeling<
                SplitLender<'a>: for<'b> NodeLabelsLender<
                    'b,
                    Label: Pair<Left = usize, Right = SD::SerType> + Copy,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                > + Send
                                     + Sync,
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
            .map(|iter| iter.into_labeled_pairs())
            .collect();
        let codec = LabeledCodec::new(sd);
        Ok(par_sort.sort_labeled(codec, pairs)?.into())
    }

    /// Sorts labeled pairs from a sequential iterator, producing a
    /// partitioned [`SortedLabeledGraph`].
    pub fn sort_pairs<SD>(
        self,
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = ((usize, usize), SD::SerType), IntoIter: Send + Sync>
        + Send
        + Sync,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
    {
        let par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);
        let codec = LabeledCodec::new(sd);
        Ok(par_sort.sort_labeled(codec, [pairs])?.into())
    }

    /// Sorts labeled pairs from a parallel iterator, producing a
    /// partitioned [`SortedLabeledGraph`].
    pub fn par_sort_pairs<SD>(
        self,
        num_nodes: usize,
        sd: SD,
        pairs: impl rayon::iter::ParallelIterator<Item = ((usize, usize), SD::SerType)>,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
    {
        let codec = LabeledCodec::new(sd);
        let par_sort = ParSortPairs::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);
        Ok(par_sort.sort_labeled(&codec, pairs)?.into())
    }

    /// Sorts labeled arcs from a [`LabeledSequentialGraph`] sequentially,
    /// producing a partitioned [`SortedLabeledGraph`].
    ///
    /// Unlike [`sort`](Self::sort), this method does not require `Send`
    /// or `Sync` on the graph's lenders or their items. The output is
    /// still partitioned for parallel compression.
    pub fn sort_seq<SD, G>(
        self,
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
        G: LabeledSequentialGraph<SD::SerType>,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();
        let mut par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }
        let codec = LabeledCodec::new(sd);
        Ok(par_sort
            .sort_labeled_seq(codec, graph.iter().into_labeled_pairs())?
            .into())
    }

    /// Sorts labeled pairs from a sequential iterator, producing a
    /// partitioned [`SortedLabeledGraph`].
    ///
    /// Unlike [`sort_pairs`](Self::sort_pairs), this method does not
    /// require `Send` or `Sync` on the iterator. The output is still
    /// partitioned for parallel compression.
    pub fn sort_pairs_seq<SD>(
        self,
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = ((usize, usize), SD::SerType)>,
    ) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
    where
        SD: BitSerializer<NE, BitWriter<NE>>
            + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
            + Send
            + Sync
            + Clone,
        SD::SerType: Copy + Send + Sync + 'static,
    {
        let par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(self.num_partitions)
            .memory_usage(self.memory_usage);
        let codec = LabeledCodec::new(sd);
        Ok(par_sort.sort_labeled_seq(codec, pairs)?.into())
    }
}
