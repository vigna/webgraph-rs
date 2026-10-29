/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Graphs built by sorting, providing efficient [`IntoParLenders`]
//! implementations.
//!
//! The graphs in this module provide declarative interfaces to the sorting
//! machinery in the [`par_sort_iters`] and [`par_sort_pairs`] modules. Given a
//! (labelled) graph, possibly provided just as an iterator on (labelled) pairs,
//! construction methods return a sorted version of the graph, with sorted
//! lenders and iterators and an efficient [`IntoParLenders`]
//! implementation—hence the `ParSorted` prefix.
//!
//! The resulting graphs are only sequential, and are usually used directly for
//! compression or other transformations.
//!
//! By default, repeated arcs are kept. Optionally, you can [deduplicate]
//! them or, in the labeled case, [merge their labels] using a
//! [`MergeLabels`] (e.g., to count repeated arcs, or to collect their
//! labels). Both happen while sorting, so the temporary files written by the
//! sort contain each arc at most once per batch.
//!
//! # Examples
//!
//! Here we turn a list of arcs into a parallel sorted graph:
//!
//! ```rust
//! # use webgraph::prelude::*;
//! # use dsi_bitstream::prelude::BE;
//! # use tempfile::Builder;
//! # fn main() -> anyhow::Result<()> {
//! # if cfg!(miri) { return Ok(()); }
//! # let tempdir = Builder::new().prefix("test").tempdir()?;
//! # let basename = tempdir.path().join("basename");
//! // Bunch of arcs
//! let arcs = [(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)];
//!
//! // This is now a sorted graph ready to be compressed in parallel
//! let sorted = ParSortedGraph::from_pairs(6, arcs)?;
//!
//! // This will compress the graph in parallel
//! BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
//! # Ok(())
//! # }
//! ```
//!
//! Note that we passed the sorted graph by value, which is efficient as the
//! underlying iterators will not be cloned. If you need to reuse the graph,
//! pass a reference instead; the iterators will be cloned as needed.
//!
//! The level of parallelism is controlled by the current number of Rayon
//! threads, so you can easily customize it by installing a custom thread pool:
//!
//! ```rust
//! # use webgraph::prelude::*;
//! # use dsi_bitstream::prelude::BE;
//! # use rayon::ThreadPoolBuilder;
//! # use tempfile::Builder;
//! # fn main() -> anyhow::Result<()> {
//! # if cfg!(miri) { return Ok(()); }
//! # let tempdir = Builder::new().prefix("test").tempdir()?;
//! # let basename = tempdir.path().join("basename");
//! // Bunch of arcs
//! let arcs = [(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)];
//!
//! // Custom thread pool with 4 threads
//! let pool = ThreadPoolBuilder::new().num_threads(4).build()?;
//!
//! pool.install(|| -> anyhow::Result<()> {
//!     let sorted = ParSortedGraph::from_pairs(6, arcs)?;
//!     BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
//!     Ok(())
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! Note that you must create the sorted graph inside the pool, as the
//! number of threads is retrieved at creation time.
//!
//! Using the pool we are in fact controlling two parameters at the same time:
//! the level of parallelism in the sorting process, and the number of lenders
//! returned by [`IntoParLenders::into_par_lenders`] on the resulting
//! [`ParSortedGraph`]. This is usually what you want, because you are going to
//! be using exactly the same number of thread to compress.
//!
//! However, you can customize the number of lenders independently of the number
//! of threads using a [configuration] obtained via
//! [`ParSortedGraph::config()`]:
//!
//! ```rust
//! # use webgraph::prelude::*;
//! # use dsi_bitstream::prelude::BE;
//! # use rayon::ThreadPoolBuilder;
//! # use tempfile::Builder;
//! # fn main() -> anyhow::Result<()> {
//! # if cfg!(miri) { return Ok(()); }
//! # let tempdir = Builder::new().prefix("test").tempdir()?;
//! # let basename = tempdir.path().join("basename");
//! // Bunch of arcs
//! let arcs = [(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)];
//!
//! // Custom thread pool with 4 threads
//! let pool = ThreadPoolBuilder::new().num_threads(4).build()?;
//!
//! pool.install(|| -> anyhow::Result<()> {
//!     let sorted = ParSortedGraph::config()
//!         .num_lenders(8)
//!         .sort_pairs(6, arcs)?;
//!     BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
//!     Ok(())
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! You can also sort a graph, which is useful, for example, to permute a
//! graph:
//!
//! ```rust
//! # use webgraph::prelude::*;
//! # use dsi_bitstream::prelude::BE;
//! # use tempfile::Builder;
//! # fn main() -> anyhow::Result<()> {
//! # if cfg!(miri) { return Ok(()); }
//! # let tempdir = Builder::new().prefix("test").tempdir()?;
//! # let basename = tempdir.path().join("basename");
//! // A VecGraph
//! let graph = VecGraph::from_arcs([(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)]);
//! let perm = [2, 0, 1, 5, 4, 3];
//! let perm_graph = PermutedGraph::new(&graph, &perm);
//! let sorted = ParSortedGraph::from_graph(perm_graph)?;
//! BvComp::with_basename(basename).par_comp::<BE, _>(sorted)?;
//! # Ok(())
//! # }
//! ```
//!
//! For labeled graph you need to specify how to serialize and deserialize the
//! labels, as they will be stored together with the arcs. In this example we
//! build a graph labeled on `i8` and we use the [`FixedWidth`] bit
//! serializer/deserializer:
//!
//! ```rust
//! # use webgraph::prelude::*;
//! # use webgraph::graphs::vec_graph::LabeledVecGraph;
//! # use webgraph::traits::bit_serde::FixedWidth;
//! # use dsi_bitstream::prelude::BE;
//! # use tempfile::Builder;
//! # fn main() -> anyhow::Result<()> {
//! # if cfg!(miri) { return Ok(()); }
//! # let tempdir = Builder::new().prefix("test").tempdir()?;
//! # let basename = tempdir.path().join("basename");
//! // A LabeledVecGraph
//! let graph = LabeledVecGraph::from_arcs([((5, 3), -2), ((1, 0), -1), ((5, 0), 100), ((1, 2), -20), ((3, 4), 127)]);
//! let sorted = ParSortedLabeledGraph::from_graph(graph, <FixedWidth<i8>>::new())?;
//! # Ok(())
//! # }
//! ```
//!
//! Finally, if you already have multiple iterators on pairs, you can sort them
//! in parallel using [`par_from_pair_iters`]. For example, if you want
//! to transpose a graph:
//!
//! ```rust
//! # use webgraph::prelude::*;
//! # use dsi_bitstream::prelude::BE;
//! # use tempfile::Builder;
//! # fn main() -> anyhow::Result<()> {
//! # if cfg!(miri) { return Ok(()); }
//! # let tempdir = Builder::new().prefix("test").tempdir()?;
//! # let basename = tempdir.path().join("basename");
//! // A VecGraph
//! let graph = VecGraph::from_arcs([(5, 3), (1, 0), (5, 0), (1, 2), (3, 4)]);
//! let num_nodes = graph.num_nodes();
//!
//! // Split into parallel lenders, turn them into pairs
//! // and transpose them
//! let rev_iters = graph.into_par_lenders().0.into_iter()
//!     .map(|lender| lender.into_pairs().map(|(x, y)| (y, x)));
//!
//! let transposed = ParSortedGraph::par_from_pair_iters(num_nodes, rev_iters)?;
//! BvComp::with_basename(basename).par_comp::<BE, _>(transposed)?;
//! # Ok(())
//! # }
//! ```
//!
//! [`par_from_pair_iters`]: ParSortedGraph::par_from_pair_iters
//! [configuration]: ParSortedGraph::config
//! [deduplicate]: ParSortedGraphConf::dedup
//! [merge their labels]: ParSortedLabeledGraphConf::merge_labels
use crate::graphs::{arc_list_graph, impl_into_lender};
use crate::labels::proj::LeftIterator;
use crate::prelude::*;
use crate::traits::BitSerializer;
use crate::utils::grouped_gaps;
use crate::utils::kmerge_iters::KMergeIters;
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::par_sort_pairs::ParSortPairs;
use crate::utils::{
    AddPolicy, BitWriter, CodecIter, Dedup, DefaultBatchCodec, MemoryUsage, MergePolicy, NoMerge,
    WithMerge,
};
use anyhow::Result;
use dsi_bitstream::prelude::NE;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use lender::*;
use std::iter::Flatten;
use std::sync::Arc;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ParSortedLabeledGraph
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A sorted labeled graph that can be processed in parallel.
///
/// A [`ParSortedLabeledGraph`] can be build in six ways:
///
/// - [`from`] takes a labelled sequential graph;
///
/// - [`par_from`] takes a labeled graph implementing [`IntoParLenders`];
///
/// - [`from_pairs`] takes an iterator on labeled pairs;
///
/// - [`par_from_pairs`] takes a Rayon parallel iterator on labeled pairs.
///
/// - [`from_try_pairs`] takes an iterator on a [`Result`] of labeled pairs;
///
/// - [`par_from_try_pairs`] takes a Rayon parallel iterator on a [`Result`]
///   of labeled pairs;
///
/// - [`par_from_pair_iters`] takes multiple iterators on pairs; useful for
///   direct pair manipulation such as transposition.
///
/// The `try_pairs` methods are useful in scenarios where the pairs come, for
/// example, from a file.
///
/// Labels are serialized and deserialized using a [`BitSerializer`] and
/// [`BitDeserializer`] pair passed to the constructor.
///
/// These method use default values: use a [configuration] for
/// turning.
///
/// Unless [deduplication] or [label merging] is enabled in the
/// configuration, repeated arcs are kept, each with its label. Since
/// compressors reject repeated successors, a graph with repeated arcs cannot
/// be compressed.
///
/// For the unlabeled case, use [`ParSortedGraph`].
///
/// # Examples
///
/// See the [module documentation].
///
/// [`from`]: ParSortedLabeledGraph::from_graph
/// [`par_from`]: ParSortedLabeledGraph::par_from_graph
/// [`from_pairs`]: ParSortedLabeledGraph::from_pairs
/// [`par_from_pairs`]: ParSortedLabeledGraph::par_from_pairs
/// [`from_try_pairs`]: ParSortedLabeledGraph::from_try_pairs
/// [`par_from_try_pairs`]: ParSortedLabeledGraph::par_from_try_pairs
/// [`par_from_pair_iters`]: ParSortedLabeledGraph::par_from_pair_iters
/// [configuration]: ParSortedLabeledGraph::config
/// [deduplication]: ParSortedLabeledGraphConf::dedup
/// [label merging]: ParSortedLabeledGraphConf::merge_labels
/// [module documentation]: crate::graphs::par_sorted_graph
pub struct ParSortedLabeledGraph<I> {
    boundaries: Box<[usize]>,
    iters: Box<[I]>,
}

type SeqIter<'a, I> = Flatten<std::iter::Cloned<std::slice::Iter<'a, I>>>;

/// Internal codec type for labeled sorted graphs.
///
/// Users should not need to reference this type directly; the labeled
/// methods on [`ParSortedLabeledGraphConf`] and [`ParSortedLabeledGraph`]
/// accept a single `SD` parameter implementing both [`BitSerializer`]
/// and [`BitDeserializer`].
pub(crate) type LabeledCodec<SD, P, L> = grouped_gaps::GroupedGapsCodec<
    NE,
    SD,
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::DELTA },
    P,
    L,
>;

/// The concrete iterator type for labeled sorted graphs.
///
/// This is the iterator returned by the terminal methods of
/// [`ParSortedLabeledGraphConf`]. The type parameter `SD` implements both
/// [`BitSerializer`] and [`BitDeserializer`] for the label type. Use
/// [`BitSerDeser`] to combine separate serializer and deserializer
/// implementations.
pub type SortedLabeledIter<SD, P = NoMerge> = KMergeIters<
    grouped_gaps::GroupedGapsIter<
        NE,
        SD,
        { dsi_bitstream::dispatch::code_consts::GAMMA },
        { dsi_bitstream::dispatch::code_consts::GAMMA },
        { dsi_bitstream::dispatch::code_consts::DELTA },
    >,
    <SD as BitSerializer<NE, BitWriter<NE>>>::SerType,
    P,
    Arc<tempfile::TempDir>,
>;

impl<I> ParSortedLabeledGraph<I> {
    /// Creates a [`ParSortedLabeledGraph`] from pre-sorted partition
    /// boundaries and iterators.
    ///
    /// # Panics
    ///
    /// Panics if the boundaries do not contain exactly one element more than
    /// the iterators, do not start at 0, or are not non-decreasing.
    pub fn from_parts(boundaries: Box<[usize]>, iters: Box<[I]>) -> Self {
        assert_eq!(
            boundaries.len(),
            iters.len() + 1,
            "there must be exactly one boundary more than the iterators"
        );
        assert_eq!(boundaries[0], 0, "boundaries must start at 0");
        assert!(
            boundaries.windows(2).all(|w| w[0] <= w[1]),
            "boundaries must be non-decreasing"
        );
        ParSortedLabeledGraph { boundaries, iters }
    }

    /// Decomposes the [`ParSortedLabeledGraph`] into its partition boundaries
    /// and iterators.
    pub fn into_parts(self) -> (Box<[usize]>, Box<[I]>) {
        (self.boundaries, self.iters)
    }
}

impl ParSortedLabeledGraph<()> {
    /// Returns a [`ParSortedLabeledGraphConf`] with default settings for
    /// customization via chained setters.
    pub fn config() -> ParSortedLabeledGraphConf {
        ParSortedLabeledGraphConf::default()
    }

    /// Sorts labeled arcs from a [`LabeledSequentialGraph`] with
    /// default settings.
    ///
    /// Equivalent to [`ParSortedLabeledGraph::config().sort_graph(graph, sd)`].
    ///
    /// [`ParSortedLabeledGraph::config().sort_graph(graph, sd)`]: ParSortedLabeledGraphConf::sort_graph
    pub fn from_graph<SD, G>(
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
    where
        SD: LabelSerDeser,
        SD::SerType: Copy,
        G: LabeledSequentialGraph<SD::SerType>,
    {
        ParSortedLabeledGraphConf::default().sort_graph(graph, sd)
    }

    /// Sorts labeled arcs from a graph implementing [`IntoParLenders`] in
    /// parallel with default settings.
    ///
    /// Equivalent to
    /// [`ParSortedLabeledGraph::config().par_sort_graph(graph, sd)`].
    ///
    /// [`ParSortedLabeledGraph::config().par_sort_graph(graph, sd)`]: ParSortedLabeledGraphConf::par_sort_graph
    pub fn par_from_graph<SD, G>(
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
    where
        SD: LabelSerDeser,
        SD::SerType: Copy,
        G: LabeledSequentialGraph<SD::SerType>
            + IntoParLenders<
                ParLender: for<'a> NodeLabelsLender<
                    'a,
                    Label: Pair<Left = usize, Right = SD::SerType> + Copy,
                    IntoIterator: IntoIterator<IntoIter: Send>,
                >,
            >,
    {
        ParSortedLabeledGraphConf::default().par_sort_graph(graph, sd)
    }

    /// Sorts labeled pairs from an iterator with default settings.
    ///
    /// Equivalent to
    /// [`ParSortedLabeledGraph::config().sort_pairs(num_nodes, sd, pairs)`].
    ///
    /// [`ParSortedLabeledGraph::config().sort_pairs(num_nodes, sd, pairs)`]: ParSortedLabeledGraphConf::sort_pairs
    pub fn from_pairs<SD>(
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = ((usize, usize), SD::SerType)>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
    where
        SD: LabelSerDeser,
        SD::SerType: Copy,
    {
        ParSortedLabeledGraphConf::default().sort_pairs(num_nodes, sd, pairs)
    }

    /// Sorts labeled pairs from a parallel iterator with default
    /// settings.
    ///
    /// Equivalent to
    /// [`ParSortedLabeledGraph::config().par_sort_pairs(num_nodes, sd, pairs)`].
    ///
    /// [`ParSortedLabeledGraph::config().par_sort_pairs(num_nodes, sd, pairs)`]: ParSortedLabeledGraphConf::par_sort_pairs
    pub fn par_from_pairs<SD>(
        num_nodes: usize,
        sd: SD,
        pairs: impl rayon::iter::ParallelIterator<Item = ((usize, usize), SD::SerType)>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
    where
        SD: LabelSerDeser,
        SD::SerType: Copy,
    {
        ParSortedLabeledGraphConf::default().par_sort_pairs(num_nodes, sd, pairs)
    }

    /// Sorts labeled pairs from multiple iterators in parallel with default
    /// settings.
    ///
    /// Equivalent to
    /// [`ParSortedLabeledGraph::config().par_sort_pair_iters(num_nodes, sd, iters)`].
    ///
    /// [`ParSortedLabeledGraph::config().par_sort_pair_iters(num_nodes, sd, iters)`]: ParSortedLabeledGraphConf::par_sort_pair_iters
    pub fn par_from_pair_iters<SD, I>(
        num_nodes: usize,
        sd: SD,
        iters: impl IntoIterator<Item = I>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
    where
        SD: LabelSerDeser,
        SD::SerType: Copy,
        I: Iterator<Item = ((usize, usize), SD::SerType)> + Send,
    {
        ParSortedLabeledGraphConf::default().par_sort_pair_iters(num_nodes, sd, iters)
    }

    /// Sorts labeled pairs from a fallible iterator with default
    /// settings.
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    ///
    /// Equivalent to
    /// [`ParSortedLabeledGraph::config().sort_try_pairs(num_nodes, sd, pairs)`].
    ///
    /// [`ParSortedLabeledGraph::config().sort_try_pairs(num_nodes, sd, pairs)`]: ParSortedLabeledGraphConf::sort_try_pairs
    pub fn from_try_pairs<SD, E: Into<anyhow::Error>>(
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = Result<((usize, usize), SD::SerType), E>>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
    where
        SD: LabelSerDeser,
        SD::SerType: Copy,
    {
        ParSortedLabeledGraphConf::default().sort_try_pairs(num_nodes, sd, pairs)
    }

    /// Sorts labeled pairs from a fallible parallel iterator with default
    /// settings.
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    ///
    /// Equivalent to
    /// [`ParSortedLabeledGraph::config().par_sort_try_pairs(num_nodes, sd, pairs)`].
    ///
    /// [`ParSortedLabeledGraph::config().par_sort_try_pairs(num_nodes, sd, pairs)`]: ParSortedLabeledGraphConf::par_sort_try_pairs
    pub fn par_from_try_pairs<SD, E: Into<anyhow::Error> + Send>(
        num_nodes: usize,
        sd: SD,
        pairs: impl rayon::iter::ParallelIterator<Item = Result<((usize, usize), SD::SerType), E>>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
    where
        SD: LabelSerDeser,
        SD::SerType: Copy,
    {
        ParSortedLabeledGraphConf::default().par_sort_try_pairs(num_nodes, sd, pairs)
    }
}

// === SequentialLabeling for SortedLabeledGraph ===

impl<L: 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync> SequentialLabeling
    for ParSortedLabeledGraph<I>
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
        // An out-of-range `from` exhausts the lender, yielding an empty
        // iteration like the range-based implementations.
        let _ = lender.advance_by(from);
        lender
    }
}

// === IntoParLenders for SortedLabeledGraph ===

/// Creates labeled lenders from an iterator of labeled-pair-iterators and
/// their boundaries.
fn make_labeled_lenders<L: 'static, I: Iterator<Item = ((usize, usize), L)> + Send + Sync>(
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

impl<L: Send + Sync + 'static, I: Iterator<Item = ((usize, usize), L)> + Send + Sync> IntoParLenders
    for ParSortedLabeledGraph<I>
{
    type ParLender = arc_list_graph::NodeLabels<L, I>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let lenders = make_labeled_lenders(self.iters.into_vec(), &self.boundaries);
        (lenders, self.boundaries)
    }
}

impl<L: Send + Sync + 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync>
    IntoParLenders for &ParSortedLabeledGraph<I>
{
    type ParLender = arc_list_graph::NodeLabels<L, I>;

    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
        let lenders = make_labeled_lenders(self.iters.iter().cloned(), &self.boundaries);
        (lenders, self.boundaries.clone())
    }
}

// === IntoLender / Lending for SortedLabeledGraph ===

impl_into_lender!(
    [L: 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync]
    ParSortedLabeledGraph<I>
);

impl<'lend, L: 'static, I: Iterator<Item = ((usize, usize), L)> + Clone + Send + Sync>
    Lending<'lend> for &ParSortedLabeledGraph<I>
{
    type Lend = Lend<'lend, <ParSortedLabeledGraph<I> as SequentialLabeling>::Lender<'lend>>;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ParSortedGraph — transparent wrapper around SortedLabeledGraph<(), I>
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A sorted graph that can be processed in parallel.
///
/// A [`ParSortedGraph`] can be build in six ways:
///
/// - [`from`] takes a sequential graph;
///
/// - [`par_from`] takes a graph implementing [`IntoParLenders`];
///
/// - [`from_pairs`] takes an iterator on pairs;
///
/// - [`par_from_pairs`] takes a Rayon parallel iterator on pairs.
///
/// - [`from_try_pairs`] takes an iterator on a [`Result`] of labeled pairs;
///
/// - [`par_from_try_pairs`] takes a Rayon parallel iterator on a [`Result`]
///   of labeled pairs.
///
/// - [`par_from_pair_iters`] takes multiple iterators on pairs; useful for
///   direct pair manipulation such as transposition.
///
/// The `try_pairs` two methods are useful in scenarios where the pairs come,
/// for example, from a file.
///
/// These method use default values: use a [configuration] for
/// turning .
///
/// For the labeled case, use [`ParSortedLabeledGraph`].
///
/// # Examples
///
/// See the [module documentation].
///
/// [`from`]: ParSortedGraph::from_graph
/// [`par_from`]: ParSortedGraph::par_from_graph
/// [`from_pairs`]: ParSortedGraph::from_pairs
/// [`par_from_pairs`]: ParSortedGraph::par_from_pairs
/// [`from_try_pairs`]: ParSortedGraph::from_try_pairs
/// [`par_from_try_pairs`]: ParSortedGraph::par_from_try_pairs
/// [`par_from_pair_iters`]: ParSortedGraph::par_from_pair_iters
/// [configuration]: ParSortedGraph::config
/// [module documentation]: crate::graphs::par_sorted_graph
pub struct ParSortedGraph<I>(pub ParSortedLabeledGraph<I>);

/// The concrete iterator type for unlabeled sorted graphs.
///
/// Yields `((usize, usize), ())` pairs. The `()` label is projected
/// away by [`ParSortedGraph`]'s trait implementations.
///
/// With the [`Dedup`] [merge policy], duplicate pairs are suppressed.
///
/// [merge policy]: crate::utils::merge_policy
pub type SortedPairIter<P = NoMerge> =
    KMergeIters<CodecIter<DefaultBatchCodec<P>>, (), P, Arc<tempfile::TempDir>>;

impl ParSortedGraph<SortedPairIter> {
    /// Creates a [`ParSortedGraph`] from pre-sorted partition boundaries and
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
    /// Decomposes the [`ParSortedGraph`] into its partition boundaries and
    /// iterators.
    pub fn into_parts(self) -> (Box<[usize]>, Box<[I]>) {
        self.0.into_parts()
    }
}

impl ParSortedGraph<()> {
    /// Returns a [`ParSortedGraphConf`] with default settings for
    /// customization via chained setters.
    pub fn config() -> ParSortedGraphConf {
        ParSortedGraphConf::default()
    }
}

impl ParSortedGraph<SortedPairIter> {
    /// Sorts arcs from a [`SequentialGraph`] with default settings.
    ///
    /// Equivalent to [`ParSortedGraph::config().sort_graph(graph)`].
    ///
    /// [`ParSortedGraph::config().sort_graph(graph)`]: ParSortedGraphConf::sort_graph
    pub fn from_graph<G: SequentialGraph>(graph: G) -> Result<Self> {
        ParSortedGraphConf::default().sort_graph(graph)
    }

    /// Sorts arcs from a graph implementing [`IntoParLenders`] in
    /// parallel with default settings.
    ///
    /// Equivalent to [`ParSortedGraph::config().par_sort_graph(graph)`].
    ///
    /// [`ParSortedGraph::config().par_sort_graph(graph)`]: ParSortedGraphConf::par_sort_graph
    pub fn par_from_graph<G>(graph: G) -> Result<Self>
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
        ParSortedGraphConf::default().par_sort_graph(graph)
    }

    /// Sorts pairs from an iterator with default settings.
    ///
    /// Equivalent to [`ParSortedGraph::config().sort_pairs(num_nodes, pairs)`].
    ///
    /// [`ParSortedGraph::config().sort_pairs(num_nodes, pairs)`]: ParSortedGraphConf::sort_pairs
    pub fn from_pairs(
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize)>,
    ) -> Result<Self> {
        ParSortedGraphConf::default().sort_pairs(num_nodes, pairs)
    }

    /// Sorts pairs from a parallel iterator with default settings.
    ///
    /// Equivalent to [`ParSortedGraph::config().par_sort_pairs(num_nodes, pairs)`].
    ///
    /// [`ParSortedGraph::config().par_sort_pairs(num_nodes, pairs)`]: ParSortedGraphConf::par_sort_pairs
    pub fn par_from_pairs(
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = (usize, usize)>,
    ) -> Result<Self> {
        ParSortedGraphConf::default().par_sort_pairs(num_nodes, pairs)
    }

    /// Sorts pairs from multiple iterators in parallel with default settings.
    ///
    /// Equivalent to [`ParSortedGraph::config().par_sort_pair_iters(num_nodes, iters)`].
    ///
    /// [`ParSortedGraph::config().par_sort_pair_iters(num_nodes, iters)`]: ParSortedGraphConf::par_sort_pair_iters
    pub fn par_from_pair_iters<I>(
        num_nodes: usize,
        iters: impl IntoIterator<Item = I>,
    ) -> Result<Self>
    where
        I: Iterator<Item = (usize, usize)> + Send,
    {
        ParSortedGraphConf::default().par_sort_pair_iters(num_nodes, iters)
    }

    /// Sorts pairs from a fallible iterator with default settings.
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    ///
    /// Equivalent to [`ParSortedGraph::config().sort_try_pairs(num_nodes, pairs)`].
    ///
    /// [`ParSortedGraph::config().sort_try_pairs(num_nodes, pairs)`]: ParSortedGraphConf::sort_try_pairs
    pub fn from_try_pairs<E: Into<anyhow::Error>>(
        num_nodes: usize,
        pairs: impl IntoIterator<Item = Result<(usize, usize), E>>,
    ) -> Result<Self> {
        ParSortedGraphConf::default().sort_try_pairs(num_nodes, pairs)
    }

    /// Sorts pairs from a fallible parallel iterator with default
    /// settings.
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    ///
    /// Equivalent to [`ParSortedGraph::config().par_sort_try_pairs(num_nodes, pairs)`].
    ///
    /// [`ParSortedGraph::config().par_sort_try_pairs(num_nodes, pairs)`]: ParSortedGraphConf::par_sort_try_pairs
    pub fn par_from_try_pairs<E: Into<anyhow::Error> + Send>(
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = Result<(usize, usize), E>>,
    ) -> Result<Self> {
        ParSortedGraphConf::default().par_sort_try_pairs(num_nodes, pairs)
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
    fn get_num_arcs(&self) -> Option<u64> {
        self.0.get_num_arcs()
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

impl_into_lender!([I: Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync] ParSortedGraph<I>);

impl<'lend, I: Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync> Lending<'lend>
    for &ParSortedGraph<I>
{
    type Lend = Lend<'lend, <ParSortedGraph<I> as SequentialLabeling>::Lender<'lend>>;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ParSortedGraphConf
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Configuration for building a [`ParSortedGraph`].
///
/// Obtained via [`ParSortedGraph::config()`]. Use the setter methods to
/// customize partitioning and memory, then call one of the terminal
/// methods to perform the sort.
///
/// You can alternatively build an instance using the [`Default`] trait
/// implementation.
///
/// # Deduplication
///
/// By default, duplicate arcs are preserved. Call [`.dedup()`] to enable
/// deduplication.
///
/// [`.dedup()`]: Self::dedup
pub struct ParSortedGraphConf<PL = Option<ProgressLogger>, P = NoMerge>(
    pub(crate) ParSortedLabeledGraphConf<PL, P>,
);

impl Default for ParSortedGraphConf {
    fn default() -> Self {
        ParSortedGraphConf(ParSortedLabeledGraphConf::default())
    }
}

impl<PL, P> ParSortedGraphConf<PL, P> {
    /// Enables deduplication of arcs during sorting.
    pub fn dedup(self) -> ParSortedGraphConf<PL, Dedup> {
        ParSortedGraphConf(self.0.dedup())
    }

    /// Sets the number of lenders that will be returned by [`IntoParLenders`].
    ///
    /// Defaults to [`rayon::current_num_threads`].
    pub fn num_lenders(self, n: usize) -> Self {
        ParSortedGraphConf(self.0.num_lenders(n))
    }

    /// Sets the memory budget for in-memory sorting.
    ///
    /// Defaults to [`MemoryUsage::default`].
    pub fn memory_usage(self, m: MemoryUsage) -> Self {
        ParSortedGraphConf(self.0.memory_usage(m))
    }

    /// Sets the progress logger.
    ///
    /// Only the [`item_name`] is set by the sort methods; all other properties
    /// should be configured by the caller.
    ///
    /// [`item_name`]: ProgressLog::item_name
    pub fn progress_logger<PL2: ProgressLog>(self, pl: PL2) -> ParSortedGraphConf<PL2, P> {
        ParSortedGraphConf(self.0.progress_logger(pl))
    }
}

impl<PL: ProgressLog, P: AddPolicy<(), State = ()> + MergePolicy<(), Output = ()>>
    ParSortedGraphConf<PL, P>
{
    /// Sorts arcs from a [`SequentialGraph`], returning a [`ParSortedGraph`].
    pub fn sort_graph<G: SequentialGraph>(
        self,
        graph: G,
    ) -> Result<ParSortedGraph<SortedPairIter<P>>> {
        let num_nodes = graph.num_nodes();
        self.sort_pairs(num_nodes, graph.iter().into_pairs())
    }

    /// Sorts arcs from a graph implementing [`IntoParLenders`] in
    /// parallel, producing a partitioned [`ParSortedGraph`].
    pub fn par_sort_graph<G>(self, graph: G) -> Result<ParSortedGraph<SortedPairIter<P>>>
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
        let (lenders, _boundaries) = graph.into_par_lenders();
        let iters = lenders
            .into_vec()
            .into_iter()
            .map(|lender| lender.into_pairs());
        self.par_sort_pair_iters(num_nodes, iters)
    }

    /// Sorts unlabeled pairs from an iterator, producing a partitioned
    /// [`ParSortedGraph`].
    pub fn sort_pairs(
        self,
        num_nodes: usize,
        pairs: impl IntoIterator<Item = (usize, usize)>,
    ) -> Result<ParSortedGraph<SortedPairIter<P>>> {
        Ok(ParSortedGraph(self.0.sort_pairs(
            num_nodes,
            (),
            pairs.into_iter().map(|pair| (pair, ())),
        )?))
    }

    /// Sorts unlabeled pairs from multiple iterators in parallel,
    /// producing a partitioned [`ParSortedGraph`].
    pub fn par_sort_pair_iters<I>(
        self,
        num_nodes: usize,
        iters: impl IntoIterator<Item = I>,
    ) -> Result<ParSortedGraph<SortedPairIter<P>>>
    where
        I: Iterator<Item = (usize, usize)> + Send,
    {
        Ok(ParSortedGraph(self.0.par_sort_pair_iters(
            num_nodes,
            (),
            iters.into_iter().map(|iter| iter.map(|pair| (pair, ()))),
        )?))
    }

    /// Sorts unlabeled pairs from a parallel iterator, producing a
    /// partitioned [`ParSortedGraph`].
    pub fn par_sort_pairs(
        self,
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = (usize, usize)>,
    ) -> Result<ParSortedGraph<SortedPairIter<P>>> {
        Ok(ParSortedGraph(self.0.par_sort_pairs(
            num_nodes,
            (),
            rayon::iter::ParallelIterator::map(pairs, |pair| (pair, ())),
        )?))
    }

    /// Sorts unlabeled pairs from a fallible iterator, producing a
    /// partitioned [`ParSortedGraph`].
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    pub fn sort_try_pairs<E: Into<anyhow::Error>>(
        self,
        num_nodes: usize,
        pairs: impl IntoIterator<Item = Result<(usize, usize), E>>,
    ) -> Result<ParSortedGraph<SortedPairIter<P>>> {
        Ok(ParSortedGraph(self.0.sort_try_pairs(
            num_nodes,
            (),
            pairs.into_iter().map(|r| r.map(|pair| (pair, ()))),
        )?))
    }

    /// Sorts unlabeled pairs from a fallible parallel iterator, producing a
    /// partitioned [`ParSortedGraph`].
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    pub fn par_sort_try_pairs<E: Into<anyhow::Error> + Send>(
        self,
        num_nodes: usize,
        pairs: impl rayon::iter::ParallelIterator<Item = Result<(usize, usize), E>>,
    ) -> Result<ParSortedGraph<SortedPairIter<P>>> {
        Ok(ParSortedGraph(self.0.par_sort_try_pairs(
            num_nodes,
            (),
            rayon::iter::ParallelIterator::map(pairs, |r| r.map(|pair| (pair, ()))),
        )?))
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ParSortedLabeledGraphConf
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Configuration for building a [`ParSortedLabeledGraph`].
///
/// Obtained via [`ParSortedLabeledGraph::config()`]. Use the setter methods
/// to customize partitioning and memory, then call one of the terminal
/// methods to perform the sort.
///
/// You can alternatively build an instance using the [`Default`] trait
/// implementation.
///
/// # Repeated arcs
///
/// By default, repeated arcs are preserved. Call [`.dedup()`] to keep only
/// one of them, or [`.merge_labels()`] to merge their labels.
///
/// [`.dedup()`]: Self::dedup
/// [`.merge_labels()`]: Self::merge_labels
pub struct ParSortedLabeledGraphConf<PL = Option<ProgressLogger>, P = NoMerge> {
    num_partitions: usize,
    memory_usage: Option<MemoryUsage>,
    pl: PL,
    policy: P,
}

impl Default for ParSortedLabeledGraphConf {
    /// Creates a [`ParSortedLabeledGraphConf`] using the [Rayon current number
    /// of threads] and [`MemoryUsage::default`] for the defaults.
    ///
    /// [Rayon current number of threads]: rayon::current_num_threads
    /// [`MemoryUsage::default`]: MemoryUsage::default
    fn default() -> Self {
        ParSortedLabeledGraphConf {
            num_partitions: rayon::current_num_threads(),
            memory_usage: None,
            pl: None,
            policy: NoMerge,
        }
    }
}

impl<PL, P> ParSortedLabeledGraphConf<PL, P> {
    /// Enables deduplication of arcs during sorting.
    ///
    /// Of the labels of a repeated arc, an unspecified one is kept. To
    /// combine the labels of repeated arcs instead, use [`merge_labels`].
    ///
    /// This method replaces the effect of a previous call to
    /// [`merge_labels`].
    ///
    /// [`merge_labels`]: Self::merge_labels
    pub fn dedup(self) -> ParSortedLabeledGraphConf<PL, Dedup> {
        ParSortedLabeledGraphConf {
            num_partitions: self.num_partitions,
            memory_usage: self.memory_usage,
            pl: self.pl,
            policy: Dedup,
        }
    }

    /// Merges the labels of repeated arcs during sorting using the given
    /// [`MergeLabels`].
    ///
    /// The sorted graph contains each arc once, labeled by the
    /// [output](MergeLabels::Output) of the merger. Merging happens both
    /// when batches of pairs are written to disk, so that each batch
    /// contains each arc once, labeled by a state of the merger, and when
    /// the batches are merged; the [final step] of the merger is applied
    /// exactly once to each arc, repeated or not. See [`MergeLabels`] for
    /// the details.
    ///
    /// The merger must implement [`AddLabel`] for the type of the labels of
    /// the pairs to sort; this requirement is checked by the sorting
    /// methods. The serializer and deserializer passed to the sorting
    /// methods handle the states of the merger.
    ///
    /// Neither states nor outputs need implement [`Clone`]. However, if
    /// states do not implement [`Clone`], the sorted graph can only be
    /// consumed by value, for example by passing it (rather than a reference
    /// to it) to [`par_comp_labeled`]: iterating on a sorted graph by
    /// reference clones its iterators, and thus their states.
    ///
    /// The merger must also implement [`Clone`], [`Send`], and [`Sync`], as
    /// it is shared by the sorting threads and cloned by the iterators of
    /// the sorted graph:
    ///
    /// ```compile_fail
    /// # use webgraph::graphs::par_sorted_graph::ParSortedLabeledGraph;
    /// # use webgraph::traits::MergeLabels;
    /// // Does not implement Clone
    /// struct Sum;
    ///
    /// impl MergeLabels for Sum {
    ///     type State = u64;
    ///     type Output = u64;
    ///
    ///     fn merge(&self, _arc: (usize, usize), state: &mut u64, other: u64) {
    ///         *state += other;
    ///     }
    ///
    ///     fn finish(&self, _arc: (usize, usize), state: u64) -> u64 {
    ///         state
    ///     }
    /// }
    ///
    /// let _conf = ParSortedLabeledGraph::config().merge_labels(Sum);
    /// ```
    ///
    /// This method replaces the effect of a previous call to [`dedup`].
    ///
    /// # Examples
    ///
    /// Counting the occurrences of each arc, and compressing the result:
    ///
    /// ```rust
    /// # if cfg!(miri) { return Ok::<(), Box<dyn std::error::Error>>(()); }
    /// # use dsi_bitstream::prelude::*;
    /// # use lender::*;
    /// # use webgraph::prelude::*;
    /// # use webgraph::graphs::par_sorted_graph::ParSortedLabeledGraph;
    /// # use webgraph::labels::bitstream::BitStreamStoreLabelsConf;
    /// # use webgraph::traits::{CountRepeats, FixedWidth};
    /// // Labels are ignored by CountRepeats
    /// let pairs = [((0, 1), 'a'), ((1, 2), 'b'), ((0, 1), 'c'), ((0, 1), 'd')];
    /// let graph = ParSortedLabeledGraph::config()
    ///     .merge_labels(CountRepeats)
    ///     .sort_pairs(3, FixedWidth::<u64>::new(), pairs)?;
    ///
    /// // The graph has no repeated arcs, and its labels are u64 counts
    /// let mut lender = graph.iter();
    /// let (node, succ) = lender.next().unwrap();
    /// assert_eq!((node, succ.into_iter().collect::<Vec<_>>()), (0, vec![(1, 3)]));
    ///
    /// let dir = tempfile::tempdir()?;
    /// BvComp::with_basename(dir.path().join("graph")).par_comp_labeled::<BE, _, _>(
    ///     &graph,
    ///     BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u64>::new()),
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// [final step]: MergeLabels::finish
    /// [`dedup`]: Self::dedup
    /// [`par_comp_labeled`]: crate::graphs::bvgraph::BvCompConf::par_comp_labeled
    pub fn merge_labels<M: MergeLabels + Clone + Send + Sync>(
        self,
        merger: M,
    ) -> ParSortedLabeledGraphConf<PL, WithMerge<M>> {
        ParSortedLabeledGraphConf {
            num_partitions: self.num_partitions,
            memory_usage: self.memory_usage,
            pl: self.pl,
            policy: WithMerge(merger),
        }
    }

    /// Sets the number of lenders that will be returned by [`IntoParLenders`].
    ///
    /// Defaults to [`rayon::current_num_threads`].
    pub fn num_lenders(self, n: usize) -> Self {
        assert!(n > 0, "the number of lenders must be positive");
        Self {
            num_partitions: n,
            ..self
        }
    }

    /// Sets the memory budget for in-memory sorting.
    ///
    /// Defaults to [`MemoryUsage::default`].
    pub fn memory_usage(self, m: MemoryUsage) -> Self {
        Self {
            memory_usage: Some(m),
            ..self
        }
    }

    /// Sets the progress logger.
    ///
    /// Only the [`item_name`] is set by the sort methods; all other properties
    /// (e.g., expected updates, display options) should be configured by the
    /// caller.
    ///
    /// [`item_name`]: ProgressLog::item_name
    pub fn progress_logger<PL2: ProgressLog>(self, pl: PL2) -> ParSortedLabeledGraphConf<PL2, P> {
        ParSortedLabeledGraphConf {
            num_partitions: self.num_partitions,
            memory_usage: self.memory_usage,
            pl,
            policy: self.policy,
        }
    }
}

impl<PL: ProgressLog, P> ParSortedLabeledGraphConf<PL, P> {
    /// Consumes this configuration and returns a configured [`ParSortIters`]
    /// together with the progress logger and the merge policy.
    fn into_par_sort_iters(self, num_nodes: usize) -> Result<(ParSortIters, PL, P)> {
        let mut psi = ParSortIters::new(num_nodes)?.num_partitions(self.num_partitions);
        if let Some(m) = self.memory_usage {
            psi = psi.memory_usage(m);
        }
        Ok((psi, self.pl, self.policy))
    }

    /// Consumes this configuration and returns a configured [`ParSortPairs`]
    /// together with the progress logger and the merge policy.
    fn into_par_sort_pairs(self, num_nodes: usize) -> Result<(ParSortPairs, PL, P)> {
        let mut psp = ParSortPairs::new(num_nodes)?.num_partitions(self.num_partitions);
        if let Some(m) = self.memory_usage {
            psp = psp.memory_usage(m);
        }
        Ok((psp, self.pl, self.policy))
    }

    /// Sorts labeled arcs from a [`LabeledSequentialGraph`], producing a
    /// partitioned [`ParSortedLabeledGraph`].
    pub fn sort_graph<SD, L, G>(
        self,
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD, P>>>
    where
        SD: LabelSerDeser,
        L: Copy + Send + Sync + 'static,
        P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
        G: LabeledSequentialGraph<L>,
    {
        let num_nodes = graph.num_nodes();
        let (par_sort, mut pl, policy) = self.into_par_sort_iters(num_nodes)?;
        let codec = LabeledCodec::<SD, P, L>::with_policy(sd, policy);
        Ok(par_sort
            .sort_labeled_seq(codec, graph.iter().into_labeled_pairs(), &mut pl)?
            .into())
    }

    /// Sorts labeled arcs from a graph implementing [`IntoParLenders`] in
    /// parallel, producing a partitioned [`ParSortedLabeledGraph`].
    pub fn par_sort_graph<SD, L, G>(
        self,
        graph: G,
        sd: SD,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD, P>>>
    where
        SD: LabelSerDeser,
        L: Copy + Send + Sync + 'static,
        P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
        G: LabeledSequentialGraph<L>
            + IntoParLenders<
                ParLender: for<'a> NodeLabelsLender<
                    'a,
                    Label: Pair<Left = usize, Right = L> + Copy,
                    IntoIterator: IntoIterator<IntoIter: Send>,
                >,
            >,
    {
        let num_nodes = graph.num_nodes();
        let (lenders, _boundaries) = graph.into_par_lenders();
        let iters = lenders
            .into_vec()
            .into_iter()
            .map(|lender| lender.into_labeled_pairs());
        self.par_sort_pair_iters(num_nodes, sd, iters)
    }

    /// Sorts labeled pairs from an iterator, producing a partitioned
    /// [`ParSortedLabeledGraph`].
    pub fn sort_pairs<SD, L>(
        self,
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = ((usize, usize), L)>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD, P>>>
    where
        SD: LabelSerDeser,
        L: Copy + Send + Sync + 'static,
        P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
    {
        let (par_sort, mut pl, policy) = self.into_par_sort_iters(num_nodes)?;
        let codec = LabeledCodec::<SD, P, L>::with_policy(sd, policy);
        Ok(par_sort.sort_labeled_seq(codec, pairs, &mut pl)?.into())
    }

    /// Sorts labeled pairs from multiple iterators in parallel,
    /// producing a partitioned [`ParSortedLabeledGraph`].
    pub fn par_sort_pair_iters<SD, L, I>(
        self,
        num_nodes: usize,
        sd: SD,
        iters: impl IntoIterator<Item = I>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD, P>>>
    where
        SD: LabelSerDeser,
        L: Copy + Send + Sync + 'static,
        P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
        I: Iterator<Item = ((usize, usize), L)> + Send,
    {
        let (par_sort, mut pl, policy) = self.into_par_sort_iters(num_nodes)?;
        let codec = LabeledCodec::<SD, P, L>::with_policy(sd, policy);
        let iters: Vec<_> = iters.into_iter().collect();
        Ok(par_sort.sort_labeled(codec, iters, &mut pl)?.into())
    }

    /// Sorts labeled pairs from a parallel iterator, producing a
    /// partitioned [`ParSortedLabeledGraph`].
    pub fn par_sort_pairs<SD, L>(
        self,
        num_nodes: usize,
        sd: SD,
        pairs: impl rayon::iter::ParallelIterator<Item = ((usize, usize), L)>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD, P>>>
    where
        SD: LabelSerDeser,
        L: Copy + Send + Sync + 'static,
        P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
    {
        let (par_sort, mut pl, policy) = self.into_par_sort_pairs(num_nodes)?;
        let codec = LabeledCodec::<SD, P, L>::with_policy(sd, policy);
        Ok(par_sort.sort_labeled(&codec, pairs, &mut pl)?.into())
    }

    /// Sorts labeled pairs from a fallible iterator, producing a
    /// partitioned [`ParSortedLabeledGraph`].
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    pub fn sort_try_pairs<SD, L, E: Into<anyhow::Error>>(
        self,
        num_nodes: usize,
        sd: SD,
        pairs: impl IntoIterator<Item = Result<((usize, usize), L), E>>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD, P>>>
    where
        SD: LabelSerDeser,
        L: Copy + Send + Sync + 'static,
        P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
    {
        let (par_sort, mut pl, policy) = self.into_par_sort_iters(num_nodes)?;
        let codec = LabeledCodec::<SD, P, L>::with_policy(sd, policy);
        Ok(par_sort.try_sort_labeled_seq(codec, pairs, &mut pl)?.into())
    }

    /// Sorts labeled pairs from a fallible parallel iterator, producing a
    /// partitioned [`ParSortedLabeledGraph`].
    ///
    /// Note that the `try_` infix refers to the fallibility of the
    /// pairs returned by the input iterators; all methods in this module
    /// are fallible as they write batches on disk.
    pub fn par_sort_try_pairs<SD, L, E: Into<anyhow::Error> + Send>(
        self,
        num_nodes: usize,
        sd: SD,
        pairs: impl rayon::iter::ParallelIterator<Item = Result<((usize, usize), L), E>>,
    ) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD, P>>>
    where
        SD: LabelSerDeser,
        L: Copy + Send + Sync + 'static,
        P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
    {
        let (par_sort, mut pl, policy) = self.into_par_sort_pairs(num_nodes)?;
        let codec = LabeledCodec::<SD, P, L>::with_policy(sd, policy);
        Ok(par_sort.try_sort_labeled(&codec, pairs, &mut pl)?.into())
    }
}
