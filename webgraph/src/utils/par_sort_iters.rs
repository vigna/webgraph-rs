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

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use dsi_progress_logger::ProgressLog;
use rayon::prelude::*;
use tempfile::TempDir;

use super::MemoryUsage;
use super::kmerge_iters::KMergeIters;
use crate::traits::split::uniform_cutpoints;
use crate::utils::{BatchCodec, CodecIter, DefaultBatchCodec};
use crate::utils::{SortedPairIter, SplitIters};

/// Implements the constructors and the setters of a sorter.
///
/// The sorter must be a structure with a `DEDUP` const parameter and fields
/// `num_nodes`, `num_partitions`, and `memory_usage`.
macro_rules! impl_sorter_conf {
    ($ty:ident) => {
        impl<const DEDUP: bool> $ty<DEDUP> {
            #[doc = concat!("Creates a new [`", stringify!($ty), "`] instance.")]
            pub(crate) fn create(num_nodes: usize) -> ::anyhow::Result<Self> {
                Ok(Self {
                    num_nodes,
                    num_partitions: rayon::current_num_threads(),
                    memory_usage: None,
                })
            }
        }

        impl $ty {
            #[doc = concat!("Creates a new [`", stringify!($ty), "`] instance.")]
            ///
            /// The methods [`num_partitions`] (which sets the number of iterators in
            /// the resulting [`SplitIters`]) and [`memory_usage`] can be used to
            /// customize the instance.
            ///
            #[doc = concat!("[`num_partitions`]: ", stringify!($ty), "::num_partitions")]
            #[doc = concat!("[`memory_usage`]: ", stringify!($ty), "::memory_usage")]
            pub fn new(num_nodes: usize) -> ::anyhow::Result<Self> {
                Self::create(num_nodes)
            }

            #[doc = concat!("Creates a new [`", stringify!($ty), "`] instance with deduplication enabled.")]
            ///
            /// When enabled, each partition iterator in the resulting [`SplitIters`]
            /// will skip consecutive elements sharing the same pair of nodes, keeping
            /// only the first occurrence.
            ///
            /// See [`new`] for details.
            ///
            #[doc = concat!("[`new`]: ", stringify!($ty), "::new")]
            pub fn new_dedup(num_nodes: usize) -> ::anyhow::Result<$ty<true>> {
                $ty::create(num_nodes)
            }
        }

        impl<const DEDUP: bool> $ty<DEDUP> {
            /// How many partitions to split the nodes into.
            ///
            /// This is the number of iterators in the resulting [`SplitIters`].
            ///
            /// Defaults to [`rayon::current_num_threads`].
            ///
            /// # Panics
            ///
            /// This method will panic if `num_partitions` is zero.
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
                    memory_usage: Some(memory_usage),
                    ..self
                }
            }
        }
    };
}

pub(crate) use impl_sorter_conf;

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

/// The buffers of unsorted pairs and the sorted batches of a sorting thread,
/// one for each partition.
pub(crate) struct PartitionBuffers<C: BatchCodec> {
    /// An identifier for the thread, used in the names of the batch files.
    id: usize,
    pub(super) sorted: Vec<Vec<CodecIter<C>>>,
    pub(super) unsorted: Vec<Vec<((usize, usize), C::Label)>>,
}

impl<C: BatchCodec> Default for PartitionBuffers<C> {
    fn default() -> Self {
        Self {
            id: usize::MAX,
            sorted: Vec::new(),
            unsorted: Vec::new(),
        }
    }
}

impl<C: BatchCodec> PartitionBuffers<C> {
    /// Drops all buffers and batches.
    pub(super) fn clear(&mut self) {
        self.sorted.clear();
        self.unsorted.clear();
    }
}

/// The parameters of a sort, and the resources shared by its threads.
///
/// This structure implements the sorting logic of both [`ParSortIters`] and
/// [`ParSortPairs`].
///
/// Pairs are partitioned by source in [uniform] partitions, and each
/// partition of each thread accumulates pairs in a buffer that is sorted and
/// written to disk as a batch when full.
///
/// [uniform]: uniform_cutpoints
/// [`ParSortPairs`]: crate::utils::par_sort_pairs::ParSortPairs
pub(crate) struct Sorter<'a, C: BatchCodec> {
    batch_codec: &'a C,
    num_nodes: usize,
    num_partitions: usize,
    nodes_per_partition: usize,
    /// The number of sorting threads, or `None` for a sequential sort.
    num_threads: Option<usize>,
    /// The number of pairs in a batch.
    batch_size: usize,
    tmp_dir: Arc<TempDir>,
}

impl<'a, C: BatchCodec> Sorter<'a, C> {
    /// Creates a new sorter using `num_threads` threads, or the current thread
    /// if `num_threads` is `None`.
    pub(super) fn new(
        batch_codec: &'a C,
        num_nodes: usize,
        num_partitions: usize,
        memory_usage: Option<MemoryUsage>,
        num_threads: Option<usize>,
    ) -> Result<Self> {
        // max(1): a zero batch size (BatchSize(0), or a MemorySize smaller
        // than one element) would allocate zero-capacity buffers, whose
        // effective batch size would then be whatever Vec's growth policy
        // picks on the first push.
        let batch_size = memory_usage
            .unwrap_or_default()
            .batch_size::<((usize, usize), C::Label)>()
            .div_ceil(num_threads.unwrap_or(1) * num_partitions)
            .max(1);
        Ok(Self {
            batch_codec,
            num_nodes,
            num_partitions,
            nodes_per_partition: num_nodes.div_ceil(num_partitions),
            num_threads,
            batch_size,
            tmp_dir: Arc::new(tempfile::tempdir().context("Could not create temporary directory")?),
        })
    }

    /// Starts the progress logger, logging the parameters of the sort.
    pub(super) fn start(&self, pl: &mut impl ProgressLog) {
        let num_buffers = self.num_threads.unwrap_or(1) * self.num_partitions;
        let memory = super::humanize(
            (self.batch_size * num_buffers * std::mem::size_of::<((usize, usize), C::Label)>())
                as f64,
        );
        pl.item_name("pair");
        match self.num_threads {
            Some(num_threads) => {
                pl.start("Reading and sorting pairs...");
                pl.info(format_args!(
                    "Threads: {}; partitions: {}; batch size: {}; memory: {}B",
                    num_threads, self.num_partitions, self.batch_size, memory,
                ));
            }
            None => {
                pl.start("Reading and sorting pairs (sequential)...");
                pl.info(format_args!(
                    "Partitions: {}; batch size: {}; memory: {}B",
                    self.num_partitions, self.batch_size, memory,
                ));
            }
        }
    }

    /// Returns new, empty buffers for a sorting thread.
    ///
    /// The identifier `id` must be unique among the threads of the sort.
    pub(super) fn buffers(&self, id: usize) -> PartitionBuffers<C> {
        PartitionBuffers {
            id,
            sorted: (0..self.num_partitions).map(|_| Vec::new()).collect(),
            unsorted: (0..self.num_partitions)
                .map(|_| Vec::with_capacity(self.batch_size))
                .collect(),
        }
    }

    /// Adds a labeled pair to the buffer of its partition, first writing the
    /// buffer to disk as a sorted batch if it is full.
    pub(super) fn push(
        &self,
        buffers: &mut PartitionBuffers<C>,
        ((src, dst), label): ((usize, usize), C::Label),
        pl: &mut impl ProgressLog,
    ) -> Result<()> {
        ensure!(
            src < self.num_nodes,
            "Source node {src} is out of bounds (num_nodes = {})",
            self.num_nodes
        );
        ensure!(
            dst < self.num_nodes,
            "Destination node {dst} is out of bounds (num_nodes = {})",
            self.num_nodes
        );
        let partition_id = src / self.nodes_per_partition;
        let buf = &mut buffers.unsorted[partition_id];
        if buf.len() >= buf.capacity() {
            self.flush(
                buffers.id,
                partition_id,
                &mut buffers.sorted[partition_id],
                buf,
                pl,
            )
            .context("Could not flush buffer")?;
        }
        buf.push(((src, dst), label));
        Ok(())
    }

    /// Writes all buffers to disk, and returns the sorted batches of each
    /// partition, leaving `buffers` empty.
    pub(super) fn finish(
        &self,
        buffers: &mut PartitionBuffers<C>,
        pl: &mut impl ProgressLog,
    ) -> Result<Vec<Vec<CodecIter<C>>>> {
        let mut sorted = std::mem::take(&mut buffers.sorted);
        let unsorted = std::mem::take(&mut buffers.unsorted);
        for (partition_id, (batches, mut buf)) in sorted.iter_mut().zip(unsorted).enumerate() {
            self.flush(buffers.id, partition_id, batches, &mut buf, pl)
                .context("Could not flush buffer at the end")?;
        }
        Ok(sorted)
    }

    /// Returns empty lists of sorted batches, one for each partition.
    ///
    /// This is the identity element of [`merge_partitions`].
    pub(super) fn no_batches(&self) -> Vec<Vec<CodecIter<C>>> {
        (0..self.num_partitions).map(|_| Vec::new()).collect()
    }

    /// Returns a [`SplitIters`] merging the sorted batches of each partition.
    pub(super) fn into_split_iters<const DEDUP: bool>(
        self,
        partitions: Vec<Vec<CodecIter<C>>>,
    ) -> SplitIters<KMergeIters<CodecIter<C>, C::Label, DEDUP, Arc<TempDir>>> {
        let iters = partitions
            .into_iter()
            // The batches of a partition are not sorted with respect to each
            // other, so we merge them into a single sorted iterator
            .map(|batches| KMergeIters::with_anchor(batches, Arc::clone(&self.tmp_dir)))
            .collect();
        SplitIters::new(
            uniform_cutpoints(self.num_nodes, self.num_partitions).collect(),
            iters,
        )
    }

    /// Sorts the content of a buffer and writes it to disk as a batch,
    /// appending it to the sorted batches of its partition and leaving the
    /// buffer empty.
    fn flush(
        &self,
        id: usize,
        partition_id: usize,
        sorted: &mut Vec<CodecIter<C>>,
        buf: &mut Vec<((usize, usize), C::Label)>,
        pl: &mut impl ProgressLog,
    ) -> Result<()> {
        // Skip empty buffers: the end-of-sort flush covers every partition of
        // every thread, and sparse inputs would otherwise create and mmap an
        // empty batch file for each of them.
        if buf.is_empty() {
            return Ok(());
        }

        let path: &Path = &self
            .tmp_dir
            .path()
            .join(format!("sorted_batch_{id}_{partition_id}_{}", sorted.len()));

        // Safety check. It's not foolproof (TOCTOU) but should catch most
        // programming errors.
        ensure!(
            !path.exists(),
            "Can't create temporary file {}, it already exists",
            path.display()
        );

        self.batch_codec
            .encode_batch(path, buf)
            .with_context(|| format!("Could not write sorted batch to {}", path.display()))?;
        sorted.push(
            self.batch_codec
                .decode_batch(path)
                .with_context(|| format!("Could not read sorted batch from {}", path.display()))?
                .into_iter(),
        );
        pl.update_with_count(buf.len());
        buf.clear();
        Ok(())
    }
}

/// Appends the sorted batches of each partition of `other` to those of
/// `partitions`.
pub(super) fn merge_partitions<I>(mut partitions: Vec<Vec<I>>, other: Vec<Vec<I>>) -> Vec<Vec<I>> {
    assert_eq!(partitions.len(), other.len());
    for (batches, other_batches) in partitions.iter_mut().zip(other) {
        batches.extend(other_batches);
    }
    partitions
}

/// Strips the `()` labels from the result of sorting unlabeled pairs.
pub(super) fn strip_labels<const DEDUP: bool>(
    split: SplitIters<KMergeIters<CodecIter<DefaultBatchCodec<DEDUP>>, (), DEDUP, Arc<TempDir>>>,
) -> SplitIters<SortedPairIter<DEDUP>> {
    let strip: fn(((usize, usize), ())) -> (usize, usize) = |(pair, _)| pair;
    SplitIters::new(
        split.boundaries,
        split
            .iters
            .into_iter()
            .map(|iter| iter.map(strip))
            .collect(),
    )
}
