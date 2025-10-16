/*
 * SPDX-FileCopyrightText: 2025 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(clippy::type_complexity)]

//! Facilities to sort in parallel externally pairs of nodes with an associated
//! label returned by a [`ParallelIterator`], returning a
//! [`SplitIters`] structure.
//!
//! The typical use of [`ParSortPairs`] is to sort pairs of nodes with an
//! associated label representing a graph; the resulting
//! [`SplitIters`] structure can be then used to build
//! a compressed representation of the graph using, e.g.,
//! [`BvComp::parallel_iter`](crate::graphs::bvgraph::BvComp::parallel_iter).

use std::cell::RefCell;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{ensure, Context, Result};
use dsi_bitstream::traits::NE;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;
use rayon::Yield;
use rdst::RadixSort;
use thread_local::ThreadLocal;

use super::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, Triple};
use super::MemoryUsage;
use crate::traits::{BitDeserializer, BitSerializer};
use crate::utils::SplitIters;

/// Takes a parallel iterator of (labelled) pairs as input, and turns them into
/// a [`SplitIters`] structure which is suitable for
/// [`BvComp::parallel_iter`](crate::graphs::bvgraph::BvComp::parallel_iter).
///
/// Note that batches will be memory-mapped. If you encounter OS-level errors
/// using this class (e.g., `ENOMEM: Out of memory` under Linux), please review
/// the limitations of your OS regarding memory-mapping (e.g.,
/// `/proc/sys/vm/max_map_count` under Linux).
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use dsi_bitstream::traits::BigEndian;
/// use lender::Lender;
/// use rayon::prelude::*;
/// use webgraph::traits::SequentialLabeling;
/// use webgraph::graphs::bvgraph::{BvComp, CompFlags};
/// use webgraph::graphs::arc_list_graph;
/// use webgraph::utils::par_sort_pairs::ParSortPairs;
///
/// let num_partitions = 2;
/// let num_nodes: usize = 5;
/// let unsorted_pairs = vec![(1, 3), (3, 2), (2, 1), (1, 0), (0, 4)];
///
/// let pair_sorter = ParSortPairs::new(num_nodes)?
///     .expected_num_pairs(unsorted_pairs.len())
///     .num_partitions(NonZeroUsize::new(num_partitions).unwrap());
///
/// let split_iters = pair_sorter.sort(
///     unsorted_pairs.par_iter().copied()
/// )?;
///
/// assert_eq!(split_iters.boundaries.len(), num_partitions + 1);
/// assert_eq!(split_iters.boundaries[0], 0);
/// assert_eq!(split_iters.boundaries[2], num_nodes);
///
/// let collected: Vec<_> = split_iters.iters
///     .into_vec()
///     .into_iter()
///     .map(|iter| iter.into_iter().collect::<Vec<_>>())
///     .collect();
///
/// assert_eq!(
///     collected,
///     vec![
///         vec![(0, 4), (1, 0), (1, 3), (2, 1)], // nodes 0, 1, and 2 are in partition 0
///         vec![(3, 2)], // nodes 3 and 4 are in partition 1
///     ],
/// );
///
/// let bvcomp_tmp_dir = tempfile::tempdir()?;
/// let bvcomp_out_dir = tempfile::tempdir()?;
///
/// // Convert pairs to labeled form and compress
/// let split_iters = pair_sorter.sort(
///     unsorted_pairs.par_iter().copied()
/// )?;
///
/// use webgraph::utils::SplitIters;
/// let split_labeled = SplitIters::new(
///     split_iters.boundaries.clone(),
///     split_iters.iters
///         .into_vec()
///         .into_iter()
///         .map(|iter| iter.into_iter().map(|(src, dst)| (src, dst, ())))
///         .collect::<Vec<_>>()
///         .into_boxed_slice()
/// );
///
/// // Convert to (node, lender) pairs using From trait
/// let pairs: Vec<_> = split_labeled.into();
///
/// // Use with parallel_iter
/// BvComp::parallel_iter::<BigEndian, _>(
///     &bvcomp_out_dir.path().join("graph"),
///     pairs.into_iter()
///         .map(|(node, lender)| (node, webgraph::prelude::LeftIterator(lender))),
///     num_nodes,
///     CompFlags::default(),
///     &rayon::ThreadPoolBuilder::default().build()?,
///     bvcomp_tmp_dir.path(),
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ParSortPairs<L = ()> {
    num_nodes: usize,
    expected_num_pairs: Option<usize>,
    num_partitions: NonZeroUsize,
    memory_usage: MemoryUsage,
    marker: PhantomData<L>,
}

impl ParSortPairs<()> {
    /// See [`try_sort`](ParSortPairs::try_sort).
    pub fn sort(
        &self,
        pairs: impl ParallelIterator<Item = (usize, usize)>,
    ) -> Result<SplitIters<impl IntoIterator<Item = (usize, usize), IntoIter: Clone + Send + Sync>>>
    {
        self.try_sort::<std::convert::Infallible>(pairs.map(Ok))
    }

    /// Sorts the output of the provided parallel iterator,
    /// returning a [`SplitIters`] structure.
    pub fn try_sort<E: Into<anyhow::Error>>(
        &self,
        pairs: impl ParallelIterator<Item = Result<(usize, usize), E>>,
    ) -> Result<SplitIters<impl IntoIterator<Item = (usize, usize), IntoIter: Clone + Send + Sync>>>
    {
        let split = self.try_sort_labeled(
            &(),
            (),
            pairs.map(|pair| -> Result<_> {
                let (src, dst) = pair.map_err(Into::into)?;
                Ok((src, dst, ()))
            }),
        )?;

        let iters_without_labels: Vec<_> = split
            .iters
            .into_vec()
            .into_iter()
            .map(|into_iter| into_iter.into_iter().map(|(src, dst, ())| (src, dst)))
            .collect();

        Ok(SplitIters::new(
            split.boundaries,
            iters_without_labels.into_boxed_slice(),
        ))
    }
}

impl<L> ParSortPairs<L> {
    pub fn new(num_nodes: usize) -> Result<Self> {
        Ok(Self {
            num_nodes,
            expected_num_pairs: None,
            num_partitions: NonZeroUsize::new(num_cpus::get()).context("zero CPUs")?,
            memory_usage: MemoryUsage::default(),
            marker: PhantomData,
        })
    }

    /// Approximate number of pairs to be sorted.
    ///
    /// Used only for progress reporting.
    pub fn expected_num_pairs(self, expected_num_pairs: usize) -> Self {
        Self {
            expected_num_pairs: Some(expected_num_pairs),
            ..self
        }
    }

    /// How many partitions to split the nodes into.
    ///
    /// Defaults to `num_cpus::get()`.
    pub fn num_partitions(self, num_partitions: NonZeroUsize) -> Self {
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
    /// memory.
    pub fn memory_usage(self, memory_usage: MemoryUsage) -> Self {
        Self {
            memory_usage,
            ..self
        }
    }

    /// See [`try_sort_labeled`](ParSortPairs::try_sort_labeled).
    ///
    /// This is a convenience method for parallel iterators that cannot fail.
    pub fn sort_labeled<S, D>(
        &self,
        serializer: &S,
        deserializer: D,
        pairs: impl ParallelIterator<Item = (usize, usize, L)>,
    ) -> Result<
        SplitIters<
            impl IntoIterator<
                Item = (
                    usize,
                    usize,
                    <D as BitDeserializer<NE, BitReader>>::DeserType,
                ),
                IntoIter: Clone + Send + Sync,
            >,
        >,
    >
    where
        L: Copy + Send + Sync,
        S: Sync + BitSerializer<NE, BitWriter, SerType = L>,
        D: Clone + Send + Sync + BitDeserializer<NE, BitReader, DeserType: Copy + Send + Sync>,
    {
        self.try_sort_labeled::<S, D, std::convert::Infallible>(
            serializer,
            deserializer,
            pairs.map(Ok),
        )
    }

    /// Sorts the output of the provided parallel iterator,
    /// returning a [`SplitIters`] structure.
    ///
    /// This  method accept as type parameter a [`BitSerializer`] and a
    /// [`BitDeserializer`] that are used to serialize and deserialize the labels.
    ///
    /// The bit deserializer must be [`Clone`] because we need one for each
    /// [`BatchIterator`], and there are possible scenarios in which the
    /// deserializer might be stateful.
    pub fn try_sort_labeled<S, D, E: Into<anyhow::Error>>(
        &self,
        serializer: &S,
        deserializer: D,
        pairs: impl ParallelIterator<Item = Result<(usize, usize, L), E>>,
    ) -> Result<
        SplitIters<
            impl IntoIterator<
                Item = (
                    usize,
                    usize,
                    <D as BitDeserializer<NE, BitReader>>::DeserType,
                ),
                IntoIter: Clone + Send + Sync,
            >,
        >,
    >
    where
        L: Copy + Send + Sync,
        S: Sync + BitSerializer<NE, BitWriter, SerType = L>,
        D: Clone + Send + Sync + BitDeserializer<NE, BitReader, DeserType: Copy + Send + Sync>,
    {
        let unsorted_pairs = pairs;

        let num_partitions = self.num_partitions.into();
        let num_buffers = rayon::current_num_threads() * num_partitions;
        let batch_size = self
            .memory_usage
            .batch_size::<Triple<L>>()
            .div_ceil(num_buffers);
        let num_nodes_per_partition = self.num_nodes.div_ceil(num_partitions);

        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "pair",
            local_speed = true,
            expected_updates = self.expected_num_pairs,
        );
        pl.start("Reading and sorting pairs");

        let worker_id = AtomicUsize::new(0);
        let presort_tmp_dir =
            tempfile::tempdir().context("Could not create temporary directory")?;

        let sorter_thread_states = ThreadLocal::<RefCell<SorterThreadState<L, D>>>::new();

        // iterators in partitioned_presorted_pairs[partition_id] contain all pairs (src, dst, label)
        // where num_nodes_per_partition*partition_id <= src < num_nodes_per_partition*(partition_id+1)
        unsorted_pairs.try_for_each_init(
            // Rayon calls this initializer on every sequential iterator inside the parallel
            // iterator. Depending on how the parallel iterator was constructed (and if
            // IndexedParallelIterator::with_min_len was not used) this can result in lots of:
            // * tiny iterators, and we don't want to create as many tiny BatchIterators because that's
            //   extremely inefficient.
            // * unsorted_buffers arrays with batch_size as capacity, but are mostly empty and sit
            //   in memory until we flush them
            // Thus, we use ThreadLocal to have one SorterThreadState per thread, which is reused
            // across multiple sequential iterators.
            || {
                (
                    pl.clone(),
                    loop {
                        if let Ok(state) = sorter_thread_states
                        .get_or(|| {
                            RefCell::new(SorterThreadState {
                                worker_id: worker_id.fetch_add(1, Ordering::Relaxed),
                                unsorted_buffers: (0..num_partitions)
                                    .map(|_| Vec::with_capacity(batch_size))
                                    .collect(),
                                sorted_pairs: (0..num_partitions).map(|_| Vec::new()).collect(),
                            })
                        })
                        .try_borrow_mut() {
                            // usually succeeds in the first attempt
                            break state;
                        }
                        // This thread is already borrowing its state higher in the call stack,
                        // but rayon is calling us again because of work stealing.
                        // But we cannot work right now (without allocating a new state, that is)
                        // so we yield back to rayon so it can resume the task that is already
                        // running in this thread.
                        match rayon::yield_now() {
                            None => panic!("rayon::yield_now() claims we are not running in a thread pool"),
                            Some(Yield::Idle) => panic!("Thread state is already borrowed, but there are no other tasks running"),
                            Some(Yield::Executed) => (),
                        }
                    }
                )
            },
            |(pl, thread_state), pair| -> Result<_> {
                let (src, dst, label) = pair.map_err(Into::into)?;
                ensure!(
                    src < self.num_nodes,
                    "Expected {}, but got {src}",
                    self.num_nodes
                );
                let partition_id = src / num_nodes_per_partition;
                let SorterThreadState {
                    worker_id,
                    ref mut sorted_pairs,
                    ref mut unsorted_buffers,
                } = &mut **thread_state;

                let sorted_pairs = &mut sorted_pairs[partition_id];
                let buf = &mut unsorted_buffers[partition_id];
                if buf.len() >= buf.capacity() {
                    let buf_len = buf.len();
                    flush_buffer(
                        presort_tmp_dir.path(),
                        serializer,
                        deserializer.clone(),
                        *worker_id,
                        partition_id,
                        sorted_pairs,
                        buf,
                    )
                    .context("Could not flush buffer")?;
                    assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                    pl.update_with_count(buf_len);
                }

                buf.push(Triple {
                    pair: [src, dst],
                    label,
                });
                Ok(())
            },
        )?;

        // flush remaining buffers
        let partitioned_presorted_pairs: Vec<Vec<BatchIterator<D>>> = sorter_thread_states
        .into_iter()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map_with(pl.clone(), |pl, thread_state: RefCell<SorterThreadState<L, D>>| {
            let thread_state = thread_state.into_inner();
            let mut partitioned_sorted_pairs = Vec::with_capacity(num_partitions);
            assert_eq!(thread_state.sorted_pairs.len(), num_partitions);
            assert_eq!(thread_state.unsorted_buffers.len(), num_partitions);
            for (partition_id, (mut sorted_pairs, mut buf)) in thread_state.sorted_pairs.into_iter().zip(thread_state.unsorted_buffers.into_iter()).enumerate() {
                let buf_len = buf.len();
                flush_buffer(presort_tmp_dir.path(), serializer, deserializer.clone(), thread_state.worker_id, partition_id, &mut sorted_pairs, &mut buf).context("Could not flush buffer at the end")?;
                assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                pl.update_with_count(buf_len);

                partitioned_sorted_pairs.push(sorted_pairs);
            }
            Ok(partitioned_sorted_pairs)
        })
        // At this point, the iterator could be collected into
        // {worker_id -> {partition_id -> [iterators]}}
        // ie. Vec<Vec<Vec<BatchIterator>>>>.
        //
        // Let's merge the {partition_id -> [iterators]} maps of each worker
        .try_reduce(
            || (0..num_partitions).map(|_| Vec::new()).collect(),
            |mut pair_partitions1: Vec<Vec<BatchIterator<D>>>, pair_partitions2: Vec<Vec<BatchIterator<D>>>| -> Result<Vec<Vec<BatchIterator<D>>>> {
            assert_eq!(pair_partitions1.len(), num_partitions);
            assert_eq!(pair_partitions2.len(), num_partitions);
            for (partition1, partition2) in pair_partitions1.iter_mut().zip(pair_partitions2.into_iter()) {
                partition1.extend(partition2.into_iter());
            }
            Ok(pair_partitions1)
        })?
        // At this point, the iterator was turned into
        // {partition_id -> [iterators]}
        // ie. Vec<Vec<BatchIterator>>>.
        ;
        pl.done();

        // Build boundaries array: [0, nodes_per_partition, 2*nodes_per_partition, ..., num_nodes]
        let boundaries: Vec<usize> = (0..=num_partitions)
            .map(|i| (i * num_nodes_per_partition).min(self.num_nodes))
            .collect();

        // Build iterators array
        let iters: Vec<_> = partitioned_presorted_pairs
            .into_iter()
            .map(|partition| {
                // 'partition' contains N iterators that are not sorted with respect to each other.
                // We merge them and turn them into a single sorted iterator.
                KMergeIters::new(partition)
            })
            .collect();

        Ok(SplitIters::new(
            boundaries.into_boxed_slice(),
            iters.into_boxed_slice(),
        ))
    }
}

struct SorterThreadState<L: Copy, D: BitDeserializer<NE, BitReader>> {
    worker_id: usize,
    sorted_pairs: Vec<Vec<BatchIterator<D>>>,
    unsorted_buffers: Vec<Vec<Triple<L>>>,
}

pub(crate) fn flush_buffer<
    L: Copy + Send + Sync,
    S: BitSerializer<NE, BitWriter, SerType = L>,
    D: BitDeserializer<NE, BitReader>,
>(
    tmp_dir: &Path,
    serializer: &S,
    deserializer: D,
    worker_id: usize,
    partition_id: usize,
    sorted_pairs: &mut Vec<BatchIterator<D>>,
    buf: &mut Vec<Triple<L>>,
) -> Result<()> {
    buf.radix_sort_unstable();

    let path = tmp_dir.join(format!(
        "sorted_batch_{worker_id}_{partition_id}_{}",
        sorted_pairs.len()
    ));

    // Safety check. It's not foolproof (TOCTOU) but should catch most programming errors.
    ensure!(
        !path.exists(),
        "Can't create temporary file {}, it already exists",
        path.display()
    );
    sorted_pairs.push(
        BatchIterator::new_from_vec_sorted_labeled(&path, buf, serializer, deserializer)
            .with_context(|| format!("Could not write sorted batch to {}", path.display()))?,
    );
    buf.clear();
    Ok(())
}
