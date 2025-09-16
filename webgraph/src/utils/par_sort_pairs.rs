/*
 * SPDX-FileCopyrightText: 2025 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

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
/// `num_partitions` defaults to `num_cpus::get()` which is usually the optimal value.
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
/// use webgraph::utils::par_sort_pairs::par_sort_pairs;
///
/// let num_partitions = 2;
/// let num_nodes: usize = 5;
/// let num_nodes_per_partition = num_nodes.div_ceil(num_partitions);
/// let unsorted_pairs = vec![(1, 3), (3, 2), (2, 1), (1, 0), (0, 4)];
///
/// let partitioned_sorted_pairs = || par_sort_pairs(
///     num_nodes,
///     Some(unsorted_pairs.len()),
///     Some(NonZeroUsize::new(num_partitions).unwrap()),
///     unsorted_pairs.par_iter().copied()
/// ).unwrap();
///
/// assert_eq!(
///     partitioned_sorted_pairs()
///         .into_iter()
///         .map(|partition| partition.into_iter().collect::<Vec<_>>())
///     .collect::<Vec<_>>(),
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
///     partitioned_sorted_pairs()
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
pub fn par_sort_pairs(
    num_nodes: usize,
    expected_num_arcs: Option<usize>,
    num_partitions: Option<NonZeroUsize>,
    pairs: impl ParallelIterator<Item = (usize, usize)>,
) -> Result<Vec<impl IntoIterator<Item = (usize, usize), IntoIter: Clone + Send + Sync>>> {
    Ok(par_sort_triples(
        num_nodes,
        expected_num_arcs,
        num_partitions,
        &(),
        (),
        pairs.map(|(src, dst)| (src, dst, ())),
    )?
    .into_iter()
    .map(|into_iter| {
        into_iter
            .into_iter()
            .map(|(src, dst, ())| (src, dst))
            .collect::<Vec<_>>()
    })
    .collect())
}

pub fn par_sort_triples<L, S, D>(
    num_nodes: usize,
    expected_num_arcs: Option<usize>,
    num_partitions: Option<NonZeroUsize>,
    serializer: &S,
    deserializer: D,
    triples: impl ParallelIterator<Item = (usize, usize, L)>,
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
    let unsorted_triples = triples;

    let batch_size = 100_000_000; // TODO: configurable

    let num_partitions: usize = num_partitions.map(Into::into).unwrap_or_else(num_cpus::get);
    let num_nodes_per_partition = num_nodes.div_ceil(num_partitions);

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "triple",
        local_speed = true,
        expected_updates = expected_num_arcs,
    );
    pl.start("[1/2] Reading and sorting triples");

    let shared_pl = Mutex::new(&mut pl);
    let actual_num_triples = AtomicU64::new(0);
    let worker_id = AtomicU64::new(0);
    let presort_tmp_dir = tempfile::tempdir().context("Could not create temporary directory")?;

    // iterators in partitioned_presorted_triples[partition_id] contain all triples (src, dst, label)
    // where num_nodes_per_partition*partition_id <= src < num_nodes_per_partition*(partition_id+1)
    let partitioned_presorted_triples: Vec<Vec<BatchIterator<D>>> = unsorted_triples
        .try_fold(
            || (worker_id.fetch_add(1, Ordering::Relaxed), (0..num_partitions).map(|_| (Vec::new(), Vec::with_capacity(batch_size))).collect()),
            |(worker_id, mut worker_data): (_, Vec<_>), (src, dst, label)| -> Result<_> {
                ensure!(src < num_nodes, "Expected {num_nodes}, but got {src}");
                let partition_id = src / num_nodes_per_partition;
                let (sorted_triples, ref mut buf) = &mut worker_data[partition_id];
                if buf.len() >= buf.capacity() {
                    let buf_len = buf.len();
                    flush_buffer(presort_tmp_dir.path(), serializer, deserializer.clone(), worker_id, partition_id, sorted_triples, buf).context("Could not flush buffer")?;
                    assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                    actual_num_triples.fetch_add(buf_len.try_into().expect("number of triples overflowed u64"), Ordering::Relaxed);
                    shared_pl.lock().unwrap().update_with_count(buf_len);
                }

                buf.push(Triple { pair: [src, dst], label });
                Ok((worker_id, worker_data))
            },
        )
        // flush remaining buffers
        .map(|res: Result<(u64, Vec<(Vec<BatchIterator<D>>, Vec<Triple<L>>)>)>| {
            let (worker_id, worker_data) = res?;
            let mut partioned_sorted_triples = Vec::with_capacity(num_partitions);
            for (partition_id, (mut sorted_triples, mut buf)) in worker_data.into_iter().enumerate() {
                let buf_len = buf.len();
                flush_buffer(presort_tmp_dir.path(), serializer, deserializer.clone(), worker_id, partition_id, &mut sorted_triples, &mut buf).context("Could not flush buffer at the end")?;
                assert!(buf.is_empty(), "flush_buffer did not empty the buffer");
                actual_num_triples.fetch_add(buf.len().try_into().expect("number of triples overflowed u64"), Ordering::Relaxed);
                shared_pl.lock().unwrap().update_with_count(buf_len);

                partioned_sorted_triples.push(sorted_triples);
            }
            Ok(partioned_sorted_triples)
        })
        // At this point, the iterator could be collected into
        // {worker_id -> {partition_id -> [iterators]}}
        // ie. Vec<Vec<Vec<BatchIterator>>>>.
        //
        // Let's merge the {partition_id -> [iterators]} maps of each worker
        .try_reduce(
            || (0..num_partitions).map(|_| Vec::new()).collect(),
            |mut triple_partitions1: Vec<Vec<BatchIterator<D>>>, triple_partitions2: Vec<Vec<BatchIterator<D>>>| -> Result<Vec<Vec<BatchIterator<D>>>> {
            assert_eq!(triple_partitions1.len(), num_partitions);
            assert_eq!(triple_partitions2.len(), num_partitions);
            for (partition1, partition2) in triple_partitions1.iter_mut().zip(triple_partitions2.into_iter()) {
                partition1.extend(partition2.into_iter());
            }
            Ok(triple_partitions1)
        })?
        // At this point, the iterator was turned into
        // {partition_id -> [iterators]}
        // ie. Vec<Vec<BatchIterator>>>.
        ;
    pl.done();

    let actual_num_triples = actual_num_triples.into_inner();

    let merge_tmp_dir = tempfile::tempdir().context("Could not create temporary directory")?;
    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "triple",
        local_speed = true,
        expected_updates = actual_num_triples.try_into().ok(),
    );
    pl.start("[2/2] Merging subiterators");
    let partitioned_triples: Vec<Vec<BatchIterator<D>>> = partitioned_presorted_triples
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

    Ok(partitioned_triples
        .into_iter()
        .map(|triple_partition: Vec<BatchIterator<D>>| triple_partition.into_iter().flatten())
        .collect())
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
    sorted_triples: &mut Vec<BatchIterator<D>>,
    buf: &mut Vec<Triple<L>>,
) -> Result<()> {
    buf.radix_sort_unstable();
    assert!(buf.windows(2).all(|w| w[0] <= w[1]), "buffer is not sorted");
    let path = tmp_dir.join(format!(
        "sorted_batch_{worker_id}_{partition_id}_{}",
        sorted_triples.len()
    ));

    // Safety check. It's not foolproof (TOCTOU) but should catch most programming errors.
    ensure!(
        !path.exists(),
        "Can't create temporary file {}, it already exists",
        path.display()
    );
    sorted_triples.push(
        BatchIterator::new_from_vec_sorted_labeled(&path, &buf, serializer, deserializer)
            .with_context(|| format!("Could not write sorted batch to {}", path.display()))?,
    );
    buf.clear();
    Ok(())
}
