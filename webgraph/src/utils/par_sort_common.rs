/*
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Machinery shared by [`ParSortPairs`] and [`ParSortIters`].
//!
//! [`ParSortPairs`]: super::par_sort_pairs::ParSortPairs
//! [`ParSortIters`]: super::par_sort_iters::ParSortIters

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use dsi_progress_logger::ProgressLog;
use tempfile::TempDir;

use super::kmerge_iters::KMergeIters;
use super::{BatchCodec, CodecIter, DefaultBatchCodec, MemoryUsage, SortedPairIter, SplitIters};
use crate::traits::split::uniform_cutpoints;

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
/// Pairs are partitioned by source in [uniform] partitions, and each
/// partition of each thread accumulates pairs in a buffer that is sorted and
/// written to disk as a batch when full.
///
/// [uniform]: uniform_cutpoints
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
