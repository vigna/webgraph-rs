/*
 * SPDX-FileCopyrightText: 2025-2026 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Facilities to sort in parallel externally (labeled) pairs of nodes
//! returned by a sequence of iterators, returning [partitioned sorted iterators of
//! (labeled) pairs of nodes](SplitIters).
//!
//! The algorithm implemented in this module is a derivation of
//! [`ParSortPairs`]. It circumvents the bottleneck of merging sorted batches
//! and then partitioning them for parallel compression by building an already
//! partitioned result. Each thread sorts one of the input iterators but
//! partitions the inputs it is sorting in a [settable number of
//! partitions]. Then, we build the result iterators by merging the first
//! partition from each thread, then the second partition from each thread, and
//! so on. At that point the iterators can be used directly for parallel
//! compression, without ever building a globally merged list of pairs. Merging
//! happens in parallel in each returned iterator.
//!
//! Parallelism is controlled via the current Rayon thread pool. Please
//! [install] a custom pool if you want to customize the parallelism. By
//! default the number of partitions is equal to the number of threads, as one
//! expects to use the same level of parallelism for sorting and for
//! compression, but there might be situations in which it might be beneficial
//! to have a different number of partitions and threads.
//!
//! The typical use of [`ParSortIters`] is to sort (labeled) pairs of nodes
//! representing a (labeled) graph; the resulting [`SplitIters`] structure can
//! be wrapped in a [`ParSortedGraph`] (or [`ParSortedLabeledGraph`]) and then
//! compressed using, for example, [`BvCompConf::par_comp`].
//!
//! For example, when transposing or permuting a [splittable] graph one obtains
//! such a sequence of iterators.
//!
//! If your pairs are emitted by a single parallel iterator, consider using
//! [`ParSortPairs`] instead.
//!
//! [`ParSortPairs`]: crate::utils::par_sort_pairs::ParSortPairs
//! [settable number of partitions]: ParSortIters::num_partitions
//! [install]: rayon::ThreadPool::install
//! [`BvCompConf::par_comp`]: crate::graphs::bvgraph::BvCompConf::par_comp
//! [splittable]: crate::traits::SplitLabeling
//! [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
//! [`ParSortedLabeledGraph`]: crate::graphs::par_sorted_graph::ParSortedLabeledGraph

use std::sync::Arc;

use anyhow::Result;
use dsi_progress_logger::ProgressLog;
use rayon::prelude::*;
use tempfile::TempDir;

use super::MemoryUsage;
use super::kmerge_iters::KMergeIters;
use super::par_sort_common::{Sorter, impl_sorter_conf, merge_partitions, strip_labels};
use crate::utils::{BatchCodec, CodecIter, DefaultBatchCodec};
use crate::utils::{SortedPairIter, SplitIters};

/// Takes a sequence of iterators of (labeled) pairs as input, and turns them
/// into a [`SplitIters`] structure which can be wrapped in a
/// [`ParSortedGraph`] for compression with
/// [`BvCompConf::par_comp`].
///
/// Note that batches will be memory-mapped. If you encounter OS-level errors
/// using this class (e.g., `ENOMEM: Out of memory` under Linux), please review
/// the limitations of your OS regarding memory-mapping (e.g.,
/// `/proc/sys/vm/max_map_count` under Linux).
///
/// See the [module documentation] for more details.
///
/// # Examples
///
/// In this example we transpose a graph in parallel by splitting it, exchanging
/// the source and destination of each arc, sorting the resulting pairs in
/// parallel using [`ParSortIters`], wrapping the result in a [`ParSortedGraph`],
/// and then compressing it using [`BvCompConf::par_comp`]:
///
/// ```
/// # if cfg!(miri) { return Ok::<(), Box<dyn std::error::Error>>(()); }
/// # use dsi_bitstream::traits::BE;
/// # use dsi_progress_logger::no_logging;
/// # use rayon::prelude::*;
/// # use webgraph::prelude::*;
/// # use webgraph::graphs::bvgraph::{BvComp, CompFlags};
/// # use webgraph::traits::{SequentialLabeling, SplitLabeling};
/// # use webgraph::utils::par_sort_iters::ParSortIters;
/// # use webgraph::graphs::par_sorted_graph::ParSortedGraph;
/// // Build a small VecGraph
/// let g = VecGraph::from_arcs([
///     (0, 4),
///     (1, 0),
///     (1, 3),
///     (2, 1),
///     (3, 2),
/// ]);
///
/// let num_nodes = g.num_nodes();
/// let num_partitions = 2;
///
/// // Split the graph into lenders and convert each to pairs
/// let pairs: Vec<_> = g
///     .split_iter(num_partitions)
///     .into_iter()
///     .map(|lender| lender.into_pairs().map(|(src, dst)| (dst, src)))
///     .collect();
///
/// // Sort the pairs using ParSortIters
/// let pair_sorter = ParSortIters::new(num_nodes)?
///     .num_partitions(num_partitions);
///
/// let sorted = pair_sorter.sort(pairs, no_logging![])?;
///
/// // Wrap in ParSortedGraph and compress in parallel
/// let sorted_graph = ParSortedGraph::from_parts(sorted.boundaries, sorted.iters);
/// let bvcomp_out_dir = tempfile::tempdir()?;
///
/// BvComp::with_basename(bvcomp_out_dir.path().join("graph")).
///     par_comp::<BE, _>(sorted_graph)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// [`BvCompConf::par_comp`]: crate::graphs::bvgraph::BvCompConf::par_comp
/// [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
/// [module documentation]: self
pub struct ParSortIters<const DEDUP: bool = false> {
    num_nodes: usize,
    num_partitions: usize,
    memory_usage: Option<MemoryUsage>,
}

impl<const DEDUP: bool> ParSortIters<DEDUP> {
    /// Sorts the output of the provided sequence of iterators, returning a
    /// [`SplitIters`] structure.
    ///
    /// When `DEDUP` is `true`, [`DefaultBatchCodec<true>`] is used to also
    /// eliminate duplicates during batch serialization.
    pub fn sort(
        &self,
        pairs: impl IntoIterator<
            Item: IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + Send + Sync,
            IntoIter: ExactSizeIterator + Send + Sync,
        >,
        pl: &mut impl ProgressLog,
    ) -> Result<SplitIters<SortedPairIter<DEDUP>>> {
        Ok(strip_labels(
            self.sort_labeled::<DefaultBatchCodec<DEDUP>, _>(
                <DefaultBatchCodec<DEDUP>>::default(),
                pairs
                    .into_iter()
                    .map(|iter| iter.into_iter().map(|pair| (pair, ()))),
                pl,
            )?,
        ))
    }

    /// See [`try_sort_seq`].
    ///
    /// This is a convenience method for iterators that cannot fail.
    ///
    /// [`try_sort_seq`]: ParSortIters::try_sort_seq
    pub fn sort_seq(
        &self,
        pairs: impl IntoIterator<Item = (usize, usize)>,
        pl: &mut impl ProgressLog,
    ) -> Result<SplitIters<SortedPairIter<DEDUP>>> {
        self.try_sort_seq::<std::convert::Infallible>(pairs.into_iter().map(Ok), pl)
    }

    /// Sorts the output of the provided iterator sequentially, returning a
    /// [`SplitIters`] structure.
    ///
    /// Unlike [`sort`], this method processes the input on the current
    /// thread and does not require `Send` or `Sync` on the iterator.
    /// The output is still partitioned for parallel compression.
    ///
    /// [`sort`]: ParSortIters::sort
    pub fn try_sort_seq<E: Into<anyhow::Error>>(
        &self,
        pairs: impl IntoIterator<Item = Result<(usize, usize), E>>,
        pl: &mut impl ProgressLog,
    ) -> Result<SplitIters<SortedPairIter<DEDUP>>> {
        Ok(strip_labels(
            self.try_sort_labeled_seq::<DefaultBatchCodec<DEDUP>, E, _>(
                <DefaultBatchCodec<DEDUP>>::default(),
                pairs.into_iter().map(|r| r.map(|pair| (pair, ()))),
                pl,
            )?,
        ))
    }
}

impl_sorter_conf!(ParSortIters);

impl<const DEDUP: bool> ParSortIters<DEDUP> {
    /// Sorts the output of the provided sequence of iterators of (labeled)
    /// pairs, returning a [`SplitIters`] structure.
    ///
    /// This method accepts as type parameter a [`BitSerializer`] and a
    /// [`BitDeserializer`] that are used to serialize and deserialize the
    /// labels.
    ///
    /// The bit deserializer must be [`Clone`] because we need one for each
    /// `BatchIterator`, and there are possible scenarios in which the
    /// deserializer might be stateful.
    ///
    /// [`BitSerializer`]: crate::traits::BitSerializer
    /// [`BitDeserializer`]: crate::traits::BitDeserializer
    pub fn sort_labeled<
        C: BatchCodec,
        P: IntoIterator<
                Item: IntoIterator<Item = ((usize, usize), C::Label), IntoIter: Send> + Send,
                IntoIter: ExactSizeIterator + Send,
            >,
    >(
        &self,
        batch_codec: C,
        pairs: P,
        pl: &mut impl ProgressLog,
    ) -> Result<SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP, Arc<TempDir>>>> {
        let sorter = Sorter::new(
            &batch_codec,
            self.num_nodes,
            self.num_partitions,
            self.memory_usage,
            Some(rayon::current_num_threads()),
        )?;
        let mut pl = pl.concurrent();
        sorter.start(&mut pl);

        // Each iterator is sorted by a thread in its own partitioned batches;
        // then, we merge the {partition_id -> [iterators]} maps of each thread
        let partitions = pairs
            .into_iter()
            .enumerate()
            .par_bridge()
            .map_init(
                || pl.clone(),
                |pl, (block_id, pairs)| {
                    let mut buffers = sorter.buffers(block_id);
                    for pair in pairs {
                        sorter.push(&mut buffers, pair, pl)?;
                    }
                    sorter.finish(&mut buffers, pl)
                },
            )
            .collect::<Result<Vec<_>>>()?
            .into_par_iter()
            .reduce(|| sorter.no_batches(), merge_partitions);
        pl.done();

        Ok(sorter.into_split_iters(partitions))
    }

    /// See [`try_sort_labeled_seq`].
    ///
    /// This is a convenience method for iterators that cannot fail.
    ///
    /// [`try_sort_labeled_seq`]: ParSortIters::try_sort_labeled_seq
    pub fn sort_labeled_seq<C: BatchCodec, P: IntoIterator<Item = ((usize, usize), C::Label)>>(
        &self,
        batch_codec: C,
        pairs: P,
        pl: &mut impl ProgressLog,
    ) -> Result<SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP, Arc<TempDir>>>> {
        self.try_sort_labeled_seq::<C, std::convert::Infallible, _>(
            batch_codec,
            pairs.into_iter().map(Ok),
            pl,
        )
    }

    /// Sorts the output of the provided iterator of (labeled) pairs
    /// sequentially, returning a [`SplitIters`] structure.
    ///
    /// Unlike [`sort_labeled`], this method processes the input on the
    /// current thread and does not require `Send` or `Sync` on the iterator
    /// or its items. The output is still partitioned, so the resulting
    /// [`SplitIters`] can be wrapped in a [`ParSortedGraph`] and compressed
    /// in parallel via
    /// [`BvCompConf::par_comp`].
    ///
    /// [`sort_labeled`]: ParSortIters::sort_labeled
    /// [`BvCompConf::par_comp`]: crate::graphs::bvgraph::BvCompConf::par_comp
    /// [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
    pub fn try_sort_labeled_seq<
        C: BatchCodec,
        E: Into<anyhow::Error>,
        P: IntoIterator<Item = Result<((usize, usize), C::Label), E>>,
    >(
        &self,
        batch_codec: C,
        pairs: P,
        pl: &mut impl ProgressLog,
    ) -> Result<SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP, Arc<TempDir>>>> {
        let sorter = Sorter::new(
            &batch_codec,
            self.num_nodes,
            self.num_partitions,
            self.memory_usage,
            None,
        )?;
        sorter.start(pl);

        let mut buffers = sorter.buffers(0);
        for pair in pairs {
            sorter.push(&mut buffers, pair.map_err(Into::into)?, pl)?;
        }
        let partitions = sorter.finish(&mut buffers, pl)?;
        pl.done();

        Ok(sorter.into_split_iters(partitions))
    }
}
