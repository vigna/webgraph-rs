/*
 * SPDX-FileCopyrightText: 2025 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(clippy::type_complexity)]

//! Facilities to sort in parallel externally pairs of nodes with an associated
//! label returned by a [`ParallelIterator`].

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Mutex;

use anyhow::{ensure, Context, Result};
use dsi_bitstream::traits::NE;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;
use rdst::RadixSort;

use super::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, Triple};
use super::MemoryUsage;
use crate::traits::{BitDeserializer, BitSerializer};

/// Takes a parallel iterator of pairs as input, and returns them into a vector
/// of sorted iterators (which can be flattened into a single iterator),
/// suitable for
/// [`BvComp::parallel_iter`](crate::graphs::bvgraph::BvComp::parallel_iter).
///
/// Note that batches will be memory-mapped. If you encounter OS-level errors
/// using this class (e.g., `ENOMEM: Out of memory` under Linux), please review
/// the limitations of your OS regarding memory-mapping (e.g.,
/// `/proc/sys/vm/max_map_count` under Linux).
///
/// ```ignore TODO
/// use std::num::NonZeroUsize;
///
/// use dsi_bitstream::traits::BigEndian;
/// use lender::Lender;
/// use rayon::prelude::*;
/// use webgraph::traits::SequentialLabeling;
/// use webgraph::graphs::bvgraph::{BvComp, CompFlags};
/// use webgraph::graphs::arc_list_graph::Iter;
/// use webgraph::utils::par_sort_graph::ParSortGraph;
///
/// let num_partitions = 2;
/// let num_nodes: usize = 5;
/// let num_nodes_per_partition = num_nodes.div_ceil(num_partitions);
/// let unsorted_pairs = vec![(1, 3), (3, 2), (2, 1), (1, 0), (0, 4)];
///
/// let pair_sorter = ParSortGraph::new(num_nodes)?
///     .expected_num_pairs(unsorted_pairs.len())
///     .num_partitions(NonZeroUsize::new(num_partitions).unwrap());
///
/// assert_eq!(
///     pair_sorter.sort(
///         unsorted_pairs.par_iter().copied()
///     )?
///         .into_iter()
///         .map(|partition| partition.into_iter().collect::<Vec<_>>())
///         .collect::<Vec<_>>(),
///     vec![
///         vec![(0, 4), (1, 0), (1, 3), (2, 1)], // nodes 0, 1, and 2 are in partition 0
///         vec![(3, 2)], // nodes 3 and 4 are in partition 1
///     ],
/// );
///
/// let bvcomp_tmp_dir = tempfile::tempdir()?;
/// let bvcomp_out_dir = tempfile::tempdir()?;
///
/// BvComp::parallel_iter::<BigEndian, _>(
///     &bvcomp_out_dir.path().join("graph"),
///     pair_sorter.sort(
///         unsorted_pairs.par_iter().copied()
///     )?
///         .into_iter()
///         .into_iter()
///         .enumerate()
///         .map(|(partition_id, partition)| {
///             webgraph::prelude::LeftIterator(Iter::<(), _>::try_new_from(
///                 num_nodes_per_partition,
///                 partition.into_iter().map(|(src, dst)| (src, dst, ())),
///                 partition_id*num_nodes_per_partition,
///             ).unwrap())
///         }),
///     num_nodes,
///     CompFlags::default(),
///     &rayon::ThreadPoolBuilder::default().build()?,
///     bvcomp_tmp_dir.path(),
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ParSortGraph<L = ()> {
    num_nodes: usize,
    expected_num_pairs: Option<usize>,
    num_partitions: NonZeroUsize,
    memory_usage: MemoryUsage,
    marker: PhantomData<L>,
}

impl ParSortGraph<()> {
    /// See [`try_sort`](ParSortGraph::try_sort).
    pub fn sort(
        &self,
        pairs: impl IntoIterator<
            Item: IntoIterator<Item = (usize, usize), IntoIter: Send> + Send,
            IntoIter: ExactSizeIterator,
        >,
    ) -> Result<Vec<(usize, impl IntoIterator<Item = (usize, usize), IntoIter: Send + Sync>)>> {
        self.try_sort::<std::convert::Infallible>(pairs)
    }

    /// Sorts the output of the provided parallel iterator,
    /// returning a vector of sorted iterators, one per partition.
    pub fn try_sort<E: Into<anyhow::Error>>(
        &self,
        pairs: impl IntoIterator<
            Item: IntoIterator<Item = (usize, usize), IntoIter: Send> + Send,
            IntoIter: ExactSizeIterator,
        >,
    ) -> Result<Vec<(usize, impl IntoIterator<Item = (usize, usize), IntoIter: Send + Sync>)>> {
        Ok(<ParSortGraph<()>>::try_sort_labeled::<(), (), E>(
            self,
            &(),
            (),
            pairs
                .into_iter()
                .map(|iter| iter.into_iter().map(|(src, dst)| (src, dst, ()))),
        )?
        .into_iter()
        .map(|(start_node, iter)| {
            (
                start_node,
                iter.into_iter().map(|(src, dst, ())| (src, dst))
            )
        })
        .collect())
    }
}

impl<L> ParSortGraph<L> {
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

    /// See [`try_sort_labeled`](ParSortGraph::try_sort_labeled).
    ///
    /// This is a convenience method for parallel iterators that cannot fail.
    pub fn sort_labeled<S, D>(
        &self,
        serializer: &S,
        deserializer: D,
        pairs: impl IntoIterator<
            Item: IntoIterator<Item = (usize, usize, L), IntoIter: Send> + Send,
            IntoIter: ExactSizeIterator,
        >,
    ) -> Result<
        Vec<
            (
                usize,
                impl IntoIterator<
                    Item = (
                        usize,
                        usize,
                        <D as BitDeserializer<NE, BitReader>>::DeserType,
                    ),
                    IntoIter: Send + Sync,
                >,
            )
        >,
    >
    where
        L: Copy + Send + Sync,
        S: Sync + BitSerializer<NE, BitWriter, SerType = L>,
        D: Clone + Send + Sync + BitDeserializer<NE, BitReader, DeserType: Copy + Send + Sync>,
    {
        self.try_sort_labeled::<S, D, std::convert::Infallible>(serializer, deserializer, pairs)
    }

    /// Sorts the output of the provided parallel iterator,
    /// returning a vector of sorted iterators, one per partition.
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
        pairs: impl IntoIterator<
            Item: IntoIterator<Item = (usize, usize, L), IntoIter: Send> + Send,
            IntoIter: ExactSizeIterator,
        >,
    ) -> Result<
        Vec<
            (
                usize,
                impl IntoIterator<
                    Item = (
                        usize,
                        usize,
                        <D as BitDeserializer<NE, BitReader>>::DeserType,
                    ),
                    IntoIter: Send + Sync,
                >,
            )
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
        pl.info(format_args!("Per-processor batch size: {}", batch_size));

        let presort_tmp_dir =
            tempfile::tempdir().context("Could not create temporary directory")?;

        let unsorted_pairs = unsorted_pairs.into_iter();
        let num_blocks = unsorted_pairs.len();

        let partitioned_presorted_pairs = Mutex::new(vec![Vec::new(); num_blocks]);

        std::thread::scope(|s| {
            let partitioned_presorted_pairs = &partitioned_presorted_pairs;
            let presort_tmp_dir = &presort_tmp_dir;
            for (block_id, pair) in unsorted_pairs.enumerate() {
                let deserializer = deserializer.clone();
                let mut pl = pl.clone();
                s.spawn(move || {
                    let mut unsorted_buffers = (0..num_partitions)
                        .map(|_| Vec::with_capacity(batch_size))
                        .collect::<Vec<_>>();
                    let mut sorted_pairs =
                        (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

                    for (src, dst, label) in pair {
                        /* ensure!(
                            src < self.num_nodes,
                            "Expected {}, but got {src}",
                            self.num_nodes
                        ); */
                        let partition_id = src / num_nodes_per_partition;

                        let sorted_pairs = &mut sorted_pairs[partition_id];
                        let buf = &mut unsorted_buffers[partition_id];
                        if buf.len() >= buf.capacity() {
                            let buf_len = buf.len();
                            flush_buffer(
                                presort_tmp_dir.path(),
                                serializer,
                                deserializer.clone(),
                                block_id,
                                partition_id,
                                sorted_pairs,
                                buf,
                            )
                            .context("Could not flush buffer")
                            .unwrap();
                            assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                            pl.update_with_count(buf_len);
                        }

                        buf.push(Triple {
                            pair: [src, dst],
                            label,
                        });
                    }

                    for (partition_id, (mut pairs, mut buf)) in sorted_pairs
                        .iter_mut()
                        .zip(unsorted_buffers.into_iter())
                        .enumerate()
                    {
                        let buf_len = buf.len();
                        flush_buffer(
                            presort_tmp_dir.path(),
                            serializer,
                            deserializer.clone(),
                            block_id,
                            partition_id,
                            &mut pairs,
                            &mut buf,
                        )
                        .context("Could not flush buffer at the end")
                        .unwrap();
                        assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                        pl.update_with_count(buf_len);
                    }

                    // TODO: ugly
                    partitioned_presorted_pairs.lock().unwrap()[block_id] = sorted_pairs;
                });
            }
        });

        // At this point, the iterator could be collected into
        // {worker_id -> {partition_id -> [iterators]}}
        // ie. Vec<Vec<Vec<BatchIterator>>>>.
        //
        // Let's merge the {partition_id -> [iterators]} maps of each worker
        let partitioned_presorted_pairs = partitioned_presorted_pairs
            .into_inner()
            .unwrap()
            .into_par_iter()
            .reduce(
                || (0..num_partitions).map(|_| Vec::new()).collect(),
                |mut pair_partitions1: Vec<Vec<BatchIterator<D>>>,
                 pair_partitions2: Vec<Vec<BatchIterator<D>>>|
                 -> Vec<Vec<BatchIterator<D>>> {
                    assert_eq!(pair_partitions1.len(), num_partitions);
                    assert_eq!(pair_partitions2.len(), num_partitions);
                    for (partition1, partition2) in pair_partitions1
                        .iter_mut()
                        .zip(pair_partitions2.into_iter())
                    {
                        partition1.extend(partition2.into_iter());
                    }
                    pair_partitions1
                },
            );
        // At this point, the iterator was turned into
        // {partition_id -> [iterators]}
        // ie. Vec<Vec<BatchIterator>>>.
        pl.done();

        Ok(partitioned_presorted_pairs
            .into_iter()
            .enumerate()
            .map(|(partition_id, partition)| {
                // 'partition' contains N iterators that are not sorted with respect to each other.
                // We merge them and turn them into a single sorted iterator.
                (partition_id * num_nodes_per_partition, KMergeIters::new(partition))
            })
            .collect())
    }
}

fn flush_buffer<
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
