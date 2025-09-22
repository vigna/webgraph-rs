/*
 * SPDX-FileCopyrightText: 2025 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::{ensure, Context, Result};
use dsi_bitstream::traits::NE;
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use rayon::prelude::*;
use rdst::RadixSort;

use super::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, Triple};
use crate::traits::{BitDeserializer, BitSerializer};

/// Takes a parallel iterator of pairs as input, and returns them into a vector of sorted iterators
/// (which can be flattened into a single iterator), suitable for
/// [`BvComp::parallel_iter`](webgraph::graphs::bvgraph::BvComp::parallel_iter).
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use dsi_bitstream::traits::LittleEndian;
/// use lender::Lender;
/// use rayon::prelude::*;
/// use webgraph::traits::SequentialLabeling;
/// use webgraph::graphs::bvgraph::{BvComp, CompFlags};
/// use webgraph::graphs::arc_list_graph::ArcListGraph;
/// use webgraph::utils::par_sort_pairs::ParSortPairs;
///
/// let num_partitions = 2;
/// let num_nodes: usize = 5;
/// let num_nodes_per_partition = num_nodes.div_ceil(num_partitions);
/// let unsorted_pairs = vec![(1, 3), (3, 2), (2, 1), (1, 0), (0, 4)];
///
/// let pair_sorter = ParSortPairs::new(num_nodes)
///     .unwrap()
///     .expected_num_pairs(unsorted_pairs.len())
///     .num_partitions(NonZeroUsize::new(num_partitions).unwrap());
///
/// assert_eq!(
///     pair_sorter.par_sort_pairs(
///         unsorted_pairs.par_iter().copied()
///     )
///         .unwrap()
///         .into_iter()
///         .map(|partition| partition.into_iter().collect::<Vec<_>>())
///         .collect::<Vec<_>>(),
///     vec![
///         vec![(0, 4), (1, 0), (1, 3), (2, 1)], // nodes 0, 1, and 2 are in partition 0
///         vec![(3, 2)], // nodes 3 and 4 are in partition 1
///     ],
/// );
///
/// let bvcomp_tmp_dir = tempfile::tempdir().unwrap();
/// let bvcomp_out_dir = tempfile::tempdir().unwrap();
///
/// BvComp::parallel_iter::<LittleEndian, _>(
///     &bvcomp_out_dir.path().join("graph"),
///     pair_sorter.par_sort_pairs(
///         unsorted_pairs.par_iter().copied()
///     )
///         .unwrap()
///         .into_iter()
///         .into_iter()
///         .enumerate()
///         .map(|(partition_id, partition)| {
///             ArcListGraph::new(
///                 num_nodes,
///                 partition.into_iter(),
///             )
///             .iter_from(partition_id * num_nodes_per_partition)
///             .take(num_nodes_per_partition)
///         }),
///     num_nodes,
///     CompFlags::default(),
///     &rayon::ThreadPoolBuilder::default().build().unwrap(),
///     bvcomp_tmp_dir.path(),
/// ).unwrap();
/// ```
pub struct ParSortPairs<L = ()> {
    num_nodes: usize,
    expected_num_pairs: Option<usize>,
    num_partitions: NonZeroUsize,
    batch_size: NonZeroUsize,
    marker: PhantomData<L>,
}

impl ParSortPairs<()> {
    pub fn par_sort_pairs(
        &self,
        pairs: impl ParallelIterator<Item = (usize, usize)>,
    ) -> Result<Vec<impl IntoIterator<Item = (usize, usize), IntoIter: Clone + Send + Sync>>> {
        Ok(self
            .par_sort_labeled_pairs(&(), (), pairs.map(|(src, dst)| (src, dst, ())))?
            .into_iter()
            .map(|into_iter| {
                into_iter
                    .into_iter()
                    .map(|(src, dst, ())| (src, dst))
            })
            .collect())
    }
}

impl<L> ParSortPairs<L> {
    pub fn new(num_nodes: usize) -> Result<Self> {
        Ok(Self {
            num_nodes,
            expected_num_pairs: None,
            num_partitions: NonZeroUsize::new(num_cpus::get()).context("zero CPUs")?,
            // TODO: compute default batch_size from available RAM and number of threads
            batch_size: NonZeroUsize::new(100_000_000).unwrap(),
            marker: PhantomData,
        })
    }

    /// Approximate number of pairs to be sorted. Used only for progress reporting
    pub fn expected_num_pairs(self, expected_num_pairs: usize) -> Self {
        Self {
            expected_num_pairs: Some(expected_num_pairs),
            ..self
        }
    }

    /// How many partitions to split the nodes into.
    ///
    /// Defaults to `num_cpus::get()` which is usually the optimal value
    pub fn num_partitions(self, num_partitions: NonZeroUsize) -> Self {
        Self {
            num_partitions,
            ..self
        }
    }

    /// How many pairs **per thread** to keep in memory before flushing to disk
    ///
    /// Larger values are logarithmically faster (by reducing the number of merges
    /// to do afterward) but consume linearly more memory.
    pub fn batch_size(self, batch_size: NonZeroUsize) -> Self {
        Self { batch_size, ..self }
    }

    pub fn par_sort_labeled_pairs<S, D>(
        &self,
        serializer: &S,
        deserializer: D,
        pairs: impl ParallelIterator<Item = (usize, usize, L)>,
    ) -> Result<Vec<impl IntoIterator<Item = (usize, usize, L), IntoIter: Clone + Send + Sync>>>
    where
        L: Copy + Send + Sync + BitDeserializer<NE, BitReader, DeserType = L>,
        S: Sync + BitSerializer<NE, BitWriter, SerType = L>,
        D: Clone + Send + Sync + BitDeserializer<NE, BitReader, DeserType = L>,
    {
        // we could relax `L: BitDeserializer<NE, BitReader, DeserType=L>` as:
        // `BitDeserializer<NE, BitReader, DeserType: BitDeserializer<NE, BitReader> >`,
        // but then the return type would have to be:
        //
        // ```
        // Result<Vec<impl IntoIterator<Item = (
        //     usize,
        //     usize,
        //     <
        //         <L as BitDeserializer<NE, BitReader>>::DeserType
        //         as BitDeserializer<NE, BitReader>
        //      >::DeserType
        // )>>>
        // ```
        //
        // and no one wants to deal with that kind of type.
        //
        // Plus, it constrains future changes to this function's internals too much.
        let unsorted_pairs = pairs;

        // {partition_id: ([BatchIterator], [pair])}
        type PartitionedWorkerData<D, L> = Vec<(Vec<BatchIterator<D>>, Vec<Triple<L>>)>;

        let num_partitions = self.num_partitions.into();
        let batch_size = self.batch_size.into();
        let num_nodes_per_partition = self.num_nodes.div_ceil(num_partitions);

        let mut pl = progress_logger!(
            display_memory = true,
            item_name = "pair",
            local_speed = true,
            expected_updates = self.expected_num_pairs,
        );
        pl.start("[1/2] Reading and sorting pairs");

        let shared_pl = Mutex::new(&mut pl);
        let actual_num_pairs = AtomicU64::new(0);
        let worker_id = AtomicU64::new(0);
        let presort_tmp_dir =
            tempfile::tempdir().context("Could not create temporary directory")?;

        // iterators in partitioned_presorted_pairs[partition_id] contain all pairs (src, dst, label)
        // where num_nodes_per_partition*partition_id <= src < num_nodes_per_partition*(partition_id+1)
        let partitioned_presorted_pairs: Vec<Vec<BatchIterator<D>>> = unsorted_pairs
        .try_fold(
            || (worker_id.fetch_add(1, Ordering::Relaxed), (0..num_partitions).map(|_| (Vec::new(), Vec::with_capacity(batch_size))).collect()),
            |(worker_id, mut worker_data): (_, PartitionedWorkerData<D, L>), (src, dst, label)| -> Result<_> {
                ensure!(src < self.num_nodes, "Expected {}, but got {src}", self.num_nodes);
                let partition_id = src / num_nodes_per_partition;
                let (sorted_pairs, ref mut buf) = &mut worker_data[partition_id];
                if buf.len() >= buf.capacity() {
                    let buf_len = buf.len();
                    flush_buffer(presort_tmp_dir.path(), serializer, deserializer.clone(), worker_id, partition_id, sorted_pairs, buf).context("Could not flush buffer")?;
                    assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                    actual_num_pairs.fetch_add(buf_len.try_into().expect("number of pairs overflowed u64"), Ordering::Relaxed);
                    shared_pl.lock().unwrap().update_with_count(buf_len);
                }

                buf.push(Triple { pair: [src, dst], label });
                Ok((worker_id, worker_data))
            },
        )
        // flush remaining buffers
        .map(|res: Result<(u64, PartitionedWorkerData<D, L>)>| {
            let (worker_id, worker_data) = res?;
            let mut partitioned_sorted_pairs = Vec::with_capacity(num_partitions);
            for (partition_id, (mut sorted_pairs, mut buf)) in worker_data.into_iter().enumerate() {
                let buf_len = buf.len();
                flush_buffer(presort_tmp_dir.path(), serializer, deserializer.clone(), worker_id, partition_id, &mut sorted_pairs, &mut buf).context("Could not flush buffer at the end")?;
                assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                actual_num_pairs.fetch_add(buf_len.try_into().expect("number of pairs overflowed u64"), Ordering::Relaxed);
                shared_pl.lock().unwrap().update_with_count(buf_len);

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

        let actual_num_pairs = actual_num_pairs.into_inner();

        let merge_tmp_dir = tempfile::tempdir().context("Could not create temporary directory")?;
        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "pair",
            local_speed = true,
            expected_updates = actual_num_pairs.try_into().ok(),
        );
        pl.start("[2/2] Merging subiterators");
        let partitioned_pairs: Vec<Vec<BatchIterator<D>>> = partitioned_presorted_pairs
        .into_par_iter()
        .enumerate()
        .map_with(
            pl.clone(),
            |pl, (partition_id, partition)| -> Result<Vec<BatchIterator<D>>> {
                // 'partition' contains N iterators that are not sorted with respect to each other.
                // We merge them and turn them into N iterator that are in ascending order.
                let mut iterators = Vec::with_capacity(partition.len());
                let mut buf = Vec::with_capacity(batch_size);
                for (src, dst, label) in KMergeIters::new(partition) {
                    assert!(partition_id * num_nodes_per_partition <= src, "partition_id={partition_id}, num_nodes_per_partition={num_nodes_per_partition}, src={src}");
                    assert!(src < (partition_id + 1) * num_nodes_per_partition, "partition_id={partition_id}, num_nodes_per_partition={num_nodes_per_partition}, src={src}");
                    if buf.len() == buf.capacity() {
                        flush_buffer(
                            merge_tmp_dir.path(),
                            serializer,
                            deserializer.clone(),
                            0,
                            partition_id,
                            &mut iterators,
                            &mut buf,
                        )
                        .context("Could not flush buffer")?;
                    }
                    assert!(
                        buf.len() < buf.capacity(),
                        "flush_buffer did not flush the buffer"
                    );
                    buf.push(Triple { pair: [src, dst], label });
                    pl.light_update();
                }
                flush_buffer(
                    merge_tmp_dir.path(),
                    serializer,
                    deserializer.clone(),
                    0,
                    partition_id,
                    &mut iterators,
                    &mut buf,
                )
                .context("Could not flush buffer at the end")?;
                Ok(iterators)
            },
        )
        .collect::<Result<_>>()?;

        pl.done();

        drop(presort_tmp_dir); // don't need this anymore, we can free disk space early

        Ok(partitioned_pairs
            .into_iter()
            .map(|pair_partition: Vec<BatchIterator<D>>| pair_partition.into_iter().flatten())
            .collect())
    }
}

fn flush_buffer<
    L: Copy + Send + Sync + BitDeserializer<NE, BitReader>,
    S: BitSerializer<NE, BitWriter, SerType = L>,
    D: BitDeserializer<NE, BitReader, DeserType = L>,
>(
    tmp_dir: &Path,
    serializer: &S,
    deserializer: D,
    worker_id: u64,
    partition_id: usize,
    sorted_pairs: &mut Vec<BatchIterator<D>>,
    buf: &mut Vec<Triple<L>>,
) -> Result<()> {
    buf.radix_sort_unstable();
    assert!(buf.windows(2).all(|w| w[0] <= w[1]), "buffer is not sorted");
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
