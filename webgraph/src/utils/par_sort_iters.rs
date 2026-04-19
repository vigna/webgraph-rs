/*
 * SPDX-FileCopyrightText: 2025-2026 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
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
//! compressed using, for example, [`BvCompConfig::par_comp`].
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
//! [`BvCompConfig::par_comp`]: crate::graphs::bvgraph::BvCompConfig::par_comp
//! [splittable]: crate::traits::SplitLabeling
//! [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
//! [`ParSortedLabeledGraph`]: crate::graphs::par_sorted_graph::ParSortedLabeledGraph

use anyhow::{Context, Result, ensure};
use dsi_progress_logger::prelude::*;
use dsi_progress_logger::{ProgressLog, concurrent_progress_logger};
use rayon::prelude::*;

use super::MemoryUsage;
use super::sort_pairs::KMergeIters;
use crate::utils::{BatchCodec, CodecIter, DefaultBatchCodec};
use crate::utils::{SortedPairIter, SplitIters};

/// Takes a sequence of iterators of (labeled) pairs as input, and turns them
/// into a [`SplitIters`] structure which can be wrapped in a
/// [`ParSortedGraph`] for compression with
/// [`BvCompConfig::par_comp`].
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
/// and then compressing it using [`BvCompConfig::par_comp`]:
///
/// [`BvCompConfig::par_comp`]: crate::graphs::bvgraph::BvCompConfig::par_comp
/// [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
/// [module documentation]: self
///
/// ```
/// # use dsi_bitstream::traits::BE;
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
/// let sorted = pair_sorter.sort(pairs)?;
///
/// // Wrap in ParSortedGraph and compress in parallel
/// let sorted_graph = ParSortedGraph::from_parts(sorted.boundaries, sorted.iters);
/// let bvcomp_out_dir = tempfile::tempdir()?;
///
/// BvComp::with_basename(bvcomp_out_dir.path().join("graph")).
///     par_comp::<BE, _>(sorted_graph)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ParSortIters<const DEDUP: bool = false> {
    num_nodes: usize,
    expected_num_pairs: Option<usize>,
    num_partitions: usize,
    memory_usage: MemoryUsage,
}

impl<const DEDUP: bool> ParSortIters<DEDUP> {
    /// This is a convenience method for iterators that cannot fail.
    /// See [`try_sort`].
    ///
    /// [`try_sort`]: ParSortIters::try_sort
    pub fn sort(
        &self,
        pairs: impl IntoIterator<
            Item: IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + Send + Sync,
            IntoIter: ExactSizeIterator + Send + Sync,
        >,
    ) -> Result<SplitIters<SortedPairIter<DEDUP>>> {
        self.try_sort::<std::convert::Infallible>(pairs)
    }

    /// Sorts the output of the provided sequence of iterators, returning a
    /// [`SplitIters`] structure.
    ///
    /// When `DEDUP` is `true`, [`DefaultBatchCodec<true>`] is used to also
    /// eliminate duplicates during batch serialization.
    pub fn try_sort<E: Into<anyhow::Error>>(
        &self,
        pairs: impl IntoIterator<
            Item: IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + Send + Sync,
            IntoIter: ExactSizeIterator + Send + Sync,
        >,
    ) -> Result<SplitIters<SortedPairIter<DEDUP>>> {
        let split = <ParSortIters<DEDUP>>::try_sort_labeled::<DefaultBatchCodec<DEDUP>, E, _>(
            self,
            <DefaultBatchCodec<DEDUP>>::default(),
            pairs
                .into_iter()
                .map(|iter| iter.into_iter().map(|pair| (pair, ()))),
        )?;

        let strip: fn(((usize, usize), ())) -> (usize, usize) = |(pair, _)| pair;
        let iters_without_labels: Vec<_> = split
            .iters
            .into_vec()
            .into_iter()
            .map(|iter| iter.map(strip))
            .collect();

        Ok(SplitIters::new(
            split.boundaries,
            iters_without_labels.into_boxed_slice(),
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
    ) -> Result<SplitIters<SortedPairIter<DEDUP>>> {
        self.try_sort_seq::<std::convert::Infallible>(pairs.into_iter().map(Ok))
    }

    /// Sorts the output of the provided iterator sequentially, returning a
    /// [`SplitIters`] structure.
    ///
    /// Unlike [`try_sort`], this method processes the input on the current
    /// thread and does not require `Send` or `Sync` on the iterator.
    /// The output is still partitioned for parallel compression.
    ///
    /// [`try_sort`]: ParSortIters::try_sort
    pub fn try_sort_seq<E: Into<anyhow::Error>>(
        &self,
        pairs: impl IntoIterator<Item = Result<(usize, usize), E>>,
    ) -> Result<SplitIters<SortedPairIter<DEDUP>>> {
        let split = <ParSortIters<DEDUP>>::try_sort_labeled_seq::<DefaultBatchCodec<DEDUP>, E, _>(
            self,
            <DefaultBatchCodec<DEDUP>>::default(),
            pairs.into_iter().map(|r| r.map(|pair| (pair, ()))),
        )?;

        let strip: fn(((usize, usize), ())) -> (usize, usize) = |(pair, _)| pair;
        let iters_without_labels: Vec<_> = split
            .iters
            .into_vec()
            .into_iter()
            .map(|iter| iter.map(strip))
            .collect();

        Ok(SplitIters::new(
            split.boundaries,
            iters_without_labels.into_boxed_slice(),
        ))
    }
}

impl<const DEDUP: bool> ParSortIters<DEDUP> {
    /// Creates a new [`ParSortIters`] instance.
    fn create(num_nodes: usize) -> Result<Self> {
        Ok(Self {
            num_nodes,
            expected_num_pairs: None,
            num_partitions: rayon::current_num_threads(),
            memory_usage: MemoryUsage::default(),
        })
    }
}

impl ParSortIters {
    /// Creates a new [`ParSortIters`] instance.
    ///
    /// The methods [`num_partitions`] (which sets the number of iterators in
    /// the resulting [`SplitIters`]), [`memory_usage`], and
    /// [`expected_num_pairs`] can be used to customize the instance.
    ///
    /// This method will return an error if [`rayon::current_num_threads`]
    /// returns zero.
    ///
    /// [`num_partitions`]: ParSortIters::num_partitions
    /// [`memory_usage`]: ParSortIters::memory_usage
    /// [`expected_num_pairs`]: ParSortIters::expected_num_pairs
    pub fn new(num_nodes: usize) -> Result<Self> {
        Self::create(num_nodes)
    }

    /// Creates a new [`ParSortIters`] instance with deduplication enabled.
    ///
    /// When enabled, each partition iterator in the resulting [`SplitIters`]
    /// will skip consecutive elements sharing the same pair of nodes, keeping
    /// only the first occurrence.
    ///
    /// See [`new`] for details.
    ///
    /// [`new`]: ParSortIters::new
    pub fn new_dedup(num_nodes: usize) -> Result<ParSortIters<true>> {
        ParSortIters::create(num_nodes)
    }
}

impl<const DEDUP: bool> ParSortIters<DEDUP> {
    /// Approximate number of pairs to be sorted.
    ///
    /// Used only for progress reporting.
    pub const fn expected_num_pairs(self, expected_num_pairs: usize) -> Self {
        Self {
            expected_num_pairs: Some(expected_num_pairs),
            ..self
        }
    }

    /// How many partitions to split the nodes into.
    ///
    /// This is the number of iterators in the resulting [`SplitIters`].
    ///
    /// Defaults to [`rayon::current_num_threads`].
    pub const fn num_partitions(self, num_partitions: usize) -> Self {
        assert!(num_partitions > 0, "num_partitions must be positive");
        Self {
            num_partitions,
            ..self
        }
    }

    /// How much memory to use for in-memory sorts.
    ///
    /// Larger values yield faster merges (by reducing logarithmically the
    /// number of batches to merge) but consume linearly more memory. We suggest
    /// to set this parameter as large as possible, depending on the available
    /// memory. The default is the default of [`MemoryUsage`].
    pub const fn memory_usage(self, memory_usage: MemoryUsage) -> Self {
        Self {
            memory_usage,
            ..self
        }
    }

    /// See [`try_sort_labeled`].
    ///
    /// This is a convenience method for iterators that cannot fail.
    ///
    /// [`try_sort_labeled`]: ParSortIters::try_sort_labeled
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
    ) -> Result<SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP>>> {
        self.try_sort_labeled::<C, std::convert::Infallible, P>(batch_codec, pairs)
    }

    /// Sorts the output of the provided sequence of iterators of (labeled)
    /// pairs, returning a [`SplitIters`] structure.
    ///
    /// This method accepts as type parameter a [`BitSerializer`] and a
    /// [`BitDeserializer`] that are used to serialize and deserialize the
    /// labels.
    ///
    /// [`BitSerializer`]: crate::traits::BitSerializer
    /// [`BitDeserializer`]: crate::traits::BitDeserializer
    ///
    /// The bit deserializer must be [`Clone`] because we need one for each
    /// `BatchIterator`, and there are possible scenarios in which the
    /// deserializer might be stateful.
    pub fn try_sort_labeled<
        C: BatchCodec,
        E: Into<anyhow::Error>,
        P: IntoIterator<
                Item: IntoIterator<Item = ((usize, usize), C::Label), IntoIter: Send> + Send,
                IntoIter: ExactSizeIterator + Send,
            >,
    >(
        &self,
        batch_codec: C,
        pairs: P,
    ) -> Result<SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP>>> {
        let unsorted_pairs = pairs;

        let num_partitions = self.num_partitions;
        let num_buffers = rayon::current_num_threads() * num_partitions;
        let batch_size = self
            .memory_usage
            .batch_size::<((usize, usize), C::Label)>()
            .div_ceil(num_buffers);
        let num_nodes_per_partition = self.num_nodes.div_ceil(num_partitions);

        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "pair",
            local_speed = true,
            expected_updates = self.expected_num_pairs,
        );
        pl.start("Reading and sorting pairs");
        let total_memory =
            batch_size * num_buffers * std::mem::size_of::<((usize, usize), C::Label)>();
        pl.info(format_args!(
            "Threads: {}; partitions: {}; batch size: {}; memory: {}B",
            rayon::current_num_threads(),
            num_partitions,
            batch_size,
            super::humanize(total_memory as f64),
        ));

        let presort_tmp_dir =
            tempfile::tempdir().context("Could not create temporary directory")?;

        let presort_tmp_dir = &presort_tmp_dir;

        let partitioned_presorted_pairs = unsorted_pairs
            .into_iter()
            .enumerate()
            .par_bridge()
            .map_init(
                || pl.clone(),
                |pl, (block_id, pair)| {
                    let mut unsorted_buffers = (0..num_partitions)
                        .map(|_| Vec::with_capacity(batch_size))
                        .collect::<Vec<_>>();
                    let mut sorted_pairs =
                        (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

                    for ((src, dst), label) in pair {
                        ensure!(
                            src < self.num_nodes,
                            "Source node {src} is out of bounds (num_nodes = {})",
                            self.num_nodes
                        );
                        let partition_id = src / num_nodes_per_partition;

                        let sorted_pairs = &mut sorted_pairs[partition_id];
                        let buf = &mut unsorted_buffers[partition_id];
                        if buf.len() >= buf.capacity() {
                            let buf_len = buf.len();
                            super::par_sort_pairs::flush_buffer(
                                presort_tmp_dir.path(),
                                &batch_codec,
                                block_id,
                                partition_id,
                                sorted_pairs,
                                buf,
                            )
                            .context("Could not flush buffer")?;
                            assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                            pl.update_with_count(buf_len);
                        }

                        buf.push(((src, dst), label));
                    }

                    for (partition_id, (pairs, mut buf)) in
                        sorted_pairs.iter_mut().zip(unsorted_buffers).enumerate()
                    {
                        let buf_len = buf.len();
                        super::par_sort_pairs::flush_buffer(
                            presort_tmp_dir.path(),
                            &batch_codec,
                            block_id,
                            partition_id,
                            pairs,
                            &mut buf,
                        )
                        .context("Could not flush buffer at the end")?;
                        assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                        pl.update_with_count(buf_len);
                    }

                    Ok(sorted_pairs)
                },
            )
            .collect::<Result<Vec<_>>>()?;

        // At this point, the iterator could be collected into {worker_id ->
        // {partition_id -> [iterators]}}, i.e., Vec<Vec<Vec<BatchIterator>>>>.
        //
        // Let's merge the {partition_id -> [iterators]} maps of each worker
        let partitioned_presorted_pairs = partitioned_presorted_pairs.into_par_iter().reduce(
            || (0..num_partitions).map(|_| Vec::new()).collect(),
            |mut pair_partitions1: Vec<Vec<CodecIter<C>>>,
             pair_partitions2: Vec<Vec<CodecIter<C>>>|
             -> Vec<Vec<CodecIter<C>>> {
                assert_eq!(pair_partitions1.len(), num_partitions);
                assert_eq!(pair_partitions2.len(), num_partitions);
                for (partition1, partition2) in pair_partitions1.iter_mut().zip(pair_partitions2) {
                    partition1.extend(partition2);
                }
                pair_partitions1
            },
        );
        // At this point, the iterator was turned into {partition_id ->
        // [iterators]}, i.e., Vec<Vec<BatchIterator>>>.
        pl.done();

        // Build boundaries array: [0, nodes_per_partition,
        // 2*nodes_per_partition, ..., num_nodes]
        let boundaries: Vec<usize> = (0..=num_partitions)
            .map(|i| (i * num_nodes_per_partition).min(self.num_nodes))
            .collect();

        // Build iterators array
        let iters: Vec<KMergeIters<CodecIter<C>, C::Label, DEDUP>> = partitioned_presorted_pairs
            .into_iter()
            .map(|partition| {
                // 'partition' contains N iterators that are not sorted with
                // respect to each other. We merge them and turn them into a
                // single sorted iterator.
                KMergeIters::new(partition)
            })
            .collect();

        Ok(SplitIters::new(
            boundaries.into_boxed_slice(),
            iters.into_boxed_slice(),
        ))
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
    ) -> Result<SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP>>> {
        self.try_sort_labeled_seq::<C, std::convert::Infallible, _>(
            batch_codec,
            pairs.into_iter().map(Ok),
        )
    }

    /// Sorts the output of the provided iterator of (labeled) pairs
    /// sequentially, returning a [`SplitIters`] structure.
    ///
    /// Unlike [`try_sort_labeled`], this method processes the input on the
    /// current thread and does not require `Send` or `Sync` on the iterator
    /// or its items. The output is still partitioned, so the resulting
    /// [`SplitIters`] can be wrapped in a [`ParSortedGraph`] and compressed
    /// in parallel via
    /// [`BvCompConfig::par_comp`](crate::graphs::bvgraph::BvCompConfig::par_comp).
    ///
    /// [`try_sort_labeled`]: ParSortIters::try_sort_labeled
    /// [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
    pub fn try_sort_labeled_seq<
        C: BatchCodec,
        E: Into<anyhow::Error>,
        P: IntoIterator<Item = Result<((usize, usize), C::Label), E>>,
    >(
        &self,
        batch_codec: C,
        pairs: P,
    ) -> Result<SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP>>> {
        let num_partitions = self.num_partitions;
        let batch_size = self
            .memory_usage
            .batch_size::<((usize, usize), C::Label)>()
            .div_ceil(num_partitions);
        let num_nodes_per_partition = self.num_nodes.div_ceil(num_partitions);

        let mut pl = progress_logger![
            display_memory = true,
            item_name = "pair",
            expected_updates = self.expected_num_pairs,
        ];
        pl.start("Reading and sorting pairs (sequential)");
        let total_memory =
            batch_size * num_partitions * std::mem::size_of::<((usize, usize), C::Label)>();
        pl.info(format_args!(
            "Partitions: {}; batch size: {}; memory: {}B",
            num_partitions,
            batch_size,
            super::humanize(total_memory as f64),
        ));

        let presort_tmp_dir =
            tempfile::tempdir().context("Could not create temporary directory")?;

        let mut unsorted_buffers: Vec<_> = (0..num_partitions)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut sorted_pairs: Vec<Vec<CodecIter<C>>> =
            (0..num_partitions).map(|_| Vec::new()).collect();

        for pair in pairs {
            let ((src, dst), label) = pair.map_err(Into::into)?;
            ensure!(
                src < self.num_nodes,
                "Source node {src} is out of bounds (num_nodes = {})",
                self.num_nodes
            );
            let partition_id = src / num_nodes_per_partition;

            let buf = &mut unsorted_buffers[partition_id];
            if buf.len() >= buf.capacity() {
                let buf_len = buf.len();
                super::par_sort_pairs::flush_buffer(
                    presort_tmp_dir.path(),
                    &batch_codec,
                    0,
                    partition_id,
                    &mut sorted_pairs[partition_id],
                    buf,
                )
                .context("Could not flush buffer")?;
                assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                pl.update_with_count(buf_len);
            }

            buf.push(((src, dst), label));
        }

        // Flush remaining buffers
        for (partition_id, mut buf) in unsorted_buffers.into_iter().enumerate() {
            let buf_len = buf.len();
            super::par_sort_pairs::flush_buffer(
                presort_tmp_dir.path(),
                &batch_codec,
                0,
                partition_id,
                &mut sorted_pairs[partition_id],
                &mut buf,
            )
            .context("Could not flush buffer at the end")?;
            assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
            pl.update_with_count(buf_len);
        }

        pl.done();

        // Build boundaries array
        let boundaries: Vec<usize> = (0..=num_partitions)
            .map(|i| (i * num_nodes_per_partition).min(self.num_nodes))
            .collect();

        // Build iterators array
        let iters: Vec<KMergeIters<CodecIter<C>, C::Label, DEDUP>> =
            sorted_pairs.into_iter().map(KMergeIters::new).collect();

        Ok(SplitIters::new(
            boundaries.into_boxed_slice(),
            iters.into_boxed_slice(),
        ))
    }
}
