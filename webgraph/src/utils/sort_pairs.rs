/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Facilities to sort externally pairs of nodes with an associated label.
#![allow(clippy::non_canonical_partial_ord_impl)]

use super::{ArcMmapHelper, MmapHelper};
use crate::{
    traits::{BitDeserializer, BitSerializer, SortedIterator},
    utils::MemoryUsage,
};
use anyhow::{anyhow, Context};
use dary_heap::PeekMut;
use dsi_bitstream::prelude::*;
use log::debug;
use mmap_rs::MmapFlags;
use rdst::*;
use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
    sync::Arc,
};

pub type BitWriter = BufBitWriter<NE, WordAdapter<usize, BufWriter<File>>>;
pub type BitReader = BufBitReader<NE, MemWordReader<u32, ArcMmapHelper<u32>>>;

/// An arc expressed as a pair of nodes and the associated label.
///
/// Equality and order are defined only (lexicographically) on the pair of
/// nodes.
#[derive(Clone, Debug, Copy)]
pub struct Triple<L: Copy> {
    pub pair: [usize; 2],
    pub label: L,
}

impl<T: Copy> RadixKey for Triple<T> {
    const LEVELS: usize = 16;

    fn get_level(&self, level: usize) -> u8 {
        (self.pair[1 - level / 8] >> ((level % 8) * 8)) as u8
    }
}

impl<T: Copy> PartialEq for Triple<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pair == other.pair
    }
}

impl<T: Copy> Eq for Triple<T> {}

impl<T: Copy> PartialOrd for Triple<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.pair.cmp(&other.pair))
    }
}

impl<T: Copy> Ord for Triple<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pair.cmp(&other.pair)
    }
}

/// A struct that provides external sorting for pairs of nodes with an
/// associated label.
///
/// An instance of this structure ingests pairs of nodes with an associated
/// label, sort them in chunks of `batch_size` triples, and dumps them to disk.
/// Then, a call to [`iter`](SortPairs::iter) returns an iterator that merges
/// the batches on disk on the fly, returning the triples sorted by
/// lexicographical order of the pairs of nodes.
///
/// A batch should be as large as possible, given the available memory.
/// Small batches are inefficient because they requires significantly
/// more I/O, and more effort during the merge phase.
///
/// Note that batches will be memory-mapped. If you encounter OS-level errors
/// using this class (e.g., `ENOMEM: Out of memory` under Linux), please review
/// the limitations of your OS regarding memory-mapping (e.g.,
/// `/proc/sys/vm/max_map_count` under Linux).
///
/// The structure accept as type parameter a [`BitSerializer`] and a
/// [`BitDeserializer`] that are used to serialize and deserialize the labels.
/// In case they are both `()`, the structure behaves as if there is no label.
///
/// The bit deserializer must be [`Clone`] because we need one for each
/// [`BatchIterator`], and there are possible scenarios in which the
/// deserializer might be stateful.
///
/// You can use this structure in two ways: either create an instance with
/// [`new_labeled`](SortPairs::new_labeled) and add labeled pairs using
/// [`push_labeled`](SortPairs::push_labeled), and then iterate over the sorted
/// pairs using [`iter`](SortPairs::iter), or create a new instance and
/// immediately sort an iterator of pairs using
/// [`sort_labeled`](SortPairs::sort_labeled) or
/// [`try_sort_labeled`](SortPairs::try_sort_labeled).
///
/// `SortPairs<(), ()>` has commodity [`new`](SortPairs::new),
/// [`push`](SortPairs::push), [`sort`](SortPairs::sort), and
/// [`try_sort`](SortPairs::try_sort) methods without labels. Note however that
/// the [resulting iterator](SortPairs::iter) is labeled, and returns pairs
/// labeled with `()`. Use [`Left`](crate::prelude::proj::Left) to project away
/// the labels if needed.
pub struct SortPairs<
    S: BitSerializer<NE, BitWriter> = (),
    D: BitDeserializer<NE, BitReader> + Clone = (),
> where
    S::SerType: Send + Sync + Copy,
{
    /// The batch size.
    batch_size: usize,
    /// Where we are going to store the batches.
    tmp_dir: PathBuf,
    /// A stateful serializer we will pass to batch iterators to serialize
    /// the labels to a bitstream.
    serializer: S,
    /// A stateful deserializer we will pass to batch iterators to deserialize
    /// the labels from a bitstream.
    deserializer: D,
    /// Keeps track of how many batches we created.
    num_batches: usize,
    /// The length of the last batch, which might be smaller than [`SortPairs::batch_size`].
    last_batch_len: usize,
    /// The batch of triples we are currently building.
    batch: Vec<Triple<S::SerType>>,
}

impl SortPairs<(), ()> {
    /// Creates a new `SortPairs` without labels.
    ///
    /// The `tmp_dir` must be empty, and in particular it must not be shared
    /// with other `SortPairs` instances.
    ///
    /// We suggest to use the [`tempfile`](https://crates.io/crates/tempfile)
    /// crate to obtain a suitable temporary directory, as it will be
    /// automatically deleted when no longer needed, but be careful to not pass
    /// the directory obtained directly, but rather its path (i.e., use
    /// `dir.path()`) because otherwise [the directory will be deleted too
    /// soon](https://github.com/Stebalien/tempfile/issues/115).
    pub fn new<P: AsRef<Path>>(memory_usage: MemoryUsage, tmp_dir: P) -> anyhow::Result<Self> {
        Self::new_labeled(memory_usage, tmp_dir, (), ())
    }
    /// Adds a unlabeled pair to the graph.
    pub fn push(&mut self, x: usize, y: usize) -> anyhow::Result<()> {
        self.push_labeled(x, y, ())
    }

    /// Takes an iterator of pairs, pushes all elements, and returns an iterator
    /// over the sorted pairs.
    ///
    /// This is a convenience method that combines multiple
    /// [`push`](SortPairs::push) calls with [`iter`](SortPairs::iter).
    pub fn sort(
        &mut self,
        pairs: impl IntoIterator<Item = (usize, usize)>,
    ) -> anyhow::Result<KMergeIters<BatchIterator<()>, ()>> {
        self.try_sort::<std::convert::Infallible>(pairs.into_iter().map(Ok))
    }

    /// Takes an iterator of fallible pairs, pushes all elements, and returns an
    /// iterator over the sorted pairs.
    ///
    /// This is a convenience method that combines multiple
    /// [`push`](SortPairs::push) calls with [`iter`](SortPairs::iter).
    pub fn try_sort<E: Into<anyhow::Error>>(
        &mut self,
        pairs: impl IntoIterator<Item = Result<(usize, usize), E>>,
    ) -> anyhow::Result<KMergeIters<BatchIterator<()>, ()>> {
        for pair in pairs {
            let (x, y) = pair.map_err(Into::into)?;
            self.push(x, y)?;
        }
        self.iter()
    }
}

impl<S: BitSerializer<NE, BitWriter>, D: BitDeserializer<NE, BitReader> + Clone> SortPairs<S, D>
where
    S::SerType: Send + Sync + Copy,
{
    /// Creates a new `SortPairs` with labels.
    ///
    /// The `dir` must be empty, and in particular it must not be shared
    /// with other `SortPairs` instances. Please use the
    /// [`tempfile`](https://crates.io/crates/tempfile) crate to obtain
    /// a suitable directory.
    pub fn new_labeled<P: AsRef<Path>>(
        memory_usage: MemoryUsage,
        dir: P,
        serializer: S,
        deserializer: D,
    ) -> anyhow::Result<Self> {
        let dir = dir.as_ref();
        let mut dir_entries =
            std::fs::read_dir(dir).with_context(|| format!("Could not list {}", dir.display()))?;
        if dir_entries.next().is_some() {
            Err(anyhow!("{} is not empty", dir.display()))
        } else {
            let batch_size = memory_usage.batch_size::<(usize, usize)>();
            Ok(SortPairs {
                batch_size,
                serializer,
                tmp_dir: dir.to_owned(),
                deserializer,
                num_batches: 0,
                last_batch_len: 0,
                batch: Vec::with_capacity(batch_size),
            })
        }
    }

    /// Adds a labeled pair to the graph.
    pub fn push_labeled(&mut self, x: usize, y: usize, t: S::SerType) -> anyhow::Result<()> {
        self.batch.push(Triple {
            pair: [x, y],
            label: t,
        });
        if self.batch.len() >= self.batch_size {
            self.dump()?;
        }
        Ok(())
    }

    /// Dump the current batch to disk
    fn dump(&mut self) -> anyhow::Result<()> {
        // This method must be idempotent as it is called by `iter`
        if self.batch.is_empty() {
            return Ok(());
        }

        // Creates a batch file where to dump
        let batch_name = self.tmp_dir.join(format!("{:06x}", self.num_batches));
        BatchIterator::new_from_vec_labeled(
            batch_name,
            &mut self.batch,
            &self.serializer,
            self.deserializer.clone(),
        )?;
        self.last_batch_len = self.batch.len();
        self.batch.clear();
        self.num_batches += 1;
        Ok(())
    }

    /// Returns an iterator over the labeled pairs, lexicographically sorted.
    pub fn iter(&mut self) -> anyhow::Result<KMergeIters<BatchIterator<D>, D::DeserType>> {
        self.dump()?;
        Ok(KMergeIters::new((0..self.num_batches).map(|batch_idx| {
            BatchIterator::new_labeled(
                self.tmp_dir.join(format!("{batch_idx:06x}")),
                if batch_idx == self.num_batches - 1 {
                    self.last_batch_len
                } else {
                    self.batch_size
                },
                self.deserializer.clone(),
            )
            .unwrap()
        })))
    }

    /// Takes an iterator of labeled pairs, pushes all elements, and returns an
    /// iterator over the sorted pairs.
    ///
    /// This is a convenience method that combines multiple
    /// [`push_labeled`](SortPairs::push_labeled) calls with
    /// [`iter`](SortPairs::iter).
    pub fn sort_labeled(
        &mut self,
        pairs: impl IntoIterator<Item = ((usize, usize), S::SerType)>,
    ) -> anyhow::Result<KMergeIters<BatchIterator<D>, D::DeserType>> {
        self.try_sort_labeled::<std::convert::Infallible>(pairs.into_iter().map(Ok))
    }

    /// Takes an iterator of fallible labeled pairs, pushes all elements, and
    /// returns an iterator over the sorted pairs.
    ///
    /// This is a convenience method that combines multiple
    /// [`push_labeled`](SortPairs::push_labeled) calls with
    /// [`iter`](SortPairs::iter).
    pub fn try_sort_labeled<E: Into<anyhow::Error>>(
        &mut self,
        pairs: impl IntoIterator<Item = Result<((usize, usize), S::SerType), E>>,
    ) -> anyhow::Result<KMergeIters<BatchIterator<D>, D::DeserType>> {
        for pair in pairs {
            let ((x, y), label) = pair.map_err(Into::into)?;
            self.push_labeled(x, y, label)?;
        }
        self.iter()
    }
}

/// An iterator that can read the batch files generated by [`SortPairs`].
pub struct BatchIterator<D: BitDeserializer<NE, BitReader> = ()> {
    stream: BitReader,
    len: usize,
    current: usize,
    prev_src: usize,
    prev_dst: usize,
    deserializer: D,
}

impl BatchIterator<()> {
    /// Sorts the given unlabeled pairs in memory, dumps them in `file_path` and
    /// return an iterator over them.
    #[inline]
    pub fn new_from_vec<P: AsRef<Path>>(
        file_path: P,
        batch: &mut [(usize, usize)],
    ) -> anyhow::Result<Self> {
        Self::new_from_vec_labeled(
            file_path,
            unsafe { core::mem::transmute::<&mut [(usize, usize)], &mut [Triple<()>]>(batch) },
            &(),
            (),
        )
    }
    /// Dump the given triples in `file_path` and return an iterator over
    /// them, assuming they are already sorted.
    pub fn new_from_vec_sorted<P: AsRef<Path>>(
        file_path: P,
        batch: &[(usize, usize)],
    ) -> anyhow::Result<Self> {
        Self::new_from_vec_sorted_labeled(
            file_path,
            unsafe { core::mem::transmute::<&[(usize, usize)], &[Triple<()>]>(batch) },
            &(),
            (),
        )
    }
}

impl<D: BitDeserializer<NE, BitReader>> BatchIterator<D> {
    /// Sort the given labeled pairs in memory, dump them in `file_path` and
    /// return an iterator over them.
    #[inline]
    pub fn new_from_vec_labeled<S: BitSerializer<NE, BitWriter>>(
        file_path: impl AsRef<Path>,
        batch: &mut [Triple<S::SerType>],
        serializer: &S,
        deserializer: D,
    ) -> anyhow::Result<Self>
    where
        S::SerType: Send + Sync + Copy,
    {
        let start = std::time::Instant::now();
        batch.radix_sort_unstable();
        debug!("Sorted {} arcs in {:?}", batch.len(), start.elapsed());
        Self::new_from_vec_sorted_labeled(file_path, batch, serializer, deserializer)
    }

    /// Dump the given labeled pairs in `file_path` and return an iterator
    /// over them, assuming they are already sorted.
    pub fn new_from_vec_sorted_labeled<S: BitSerializer<NE, BitWriter>>(
        file_path: impl AsRef<Path>,
        batch: &[Triple<S::SerType>],
        serializer: &S,
        deserializer: D,
    ) -> anyhow::Result<Self>
    where
        S::SerType: Send + Sync + Copy,
    {
        // create a batch file where to dump
        let file_path = file_path.as_ref();
        let file = std::io::BufWriter::with_capacity(
            1 << 16,
            std::fs::File::create(file_path).with_context(|| {
                format!(
                    "Could not create BatchIterator temporary file {}",
                    file_path.display()
                )
            })?,
        );
        // create a bitstream to write to the file
        let mut stream = <BufBitWriter<NE, _>>::new(<WordAdapter<usize, _>>::new(file));
        // dump the triples to the bitstream
        let (mut prev_src, mut prev_dst) = (0, 0);
        for Triple {
            pair: [src, dst],
            label,
        } in batch.iter()
        {
            // write the source gap as gamma
            stream
                .write_gamma((src - prev_src) as _)
                .with_context(|| format!("Could not write {src} after {prev_src}"))?;
            if *src != prev_src {
                // Reset prev_y
                prev_dst = 0;
            }
            // write the destination gap as gamma
            stream
                .write_gamma((dst - prev_dst) as _)
                .with_context(|| format!("Could not write {dst} after {prev_dst}"))?;
            // write the label
            serializer
                .serialize(label, &mut stream)
                .context("Could not serialize label")?;
            (prev_src, prev_dst) = (*src, *dst);
        }
        // flush the stream and reset the buffer
        stream.flush().context("Could not flush stream")?;

        Self::new_labeled(file_path, batch.len(), deserializer)
    }

    /// Creates a new iterator over the triples previously serialized in `file_path`.
    pub fn new_labeled<P: AsRef<std::path::Path>>(
        file_path: P,
        len: usize,
        deserializer: D,
    ) -> anyhow::Result<Self> {
        let stream = <BufBitReader<NE, _>>::new(MemWordReader::new(ArcMmapHelper(Arc::new(
            MmapHelper::mmap(
                file_path.as_ref(),
                MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SEQUENTIAL,
            )
            .with_context(|| format!("Could not mmap {}", file_path.as_ref().display()))?,
        ))));
        Ok(BatchIterator {
            stream,
            len,
            current: 0,
            prev_src: 0,
            prev_dst: 0,
            deserializer,
        })
    }
}

impl<D: BitDeserializer<NE, BitReader> + Clone> Clone for BatchIterator<D> {
    fn clone(&self) -> Self {
        BatchIterator {
            stream: self.stream.clone(),
            len: self.len,
            current: self.current,
            prev_src: self.prev_src,
            prev_dst: self.prev_dst,
            deserializer: self.deserializer.clone(),
        }
    }
}

unsafe impl<D: BitDeserializer<NE, BitReader>> SortedIterator for BatchIterator<D> {}

impl<D: BitDeserializer<NE, BitReader>> Iterator for BatchIterator<D> {
    type Item = ((usize, usize), D::DeserType);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.len {
            return None;
        }
        let src = self.prev_src + self.stream.read_gamma().unwrap() as usize;
        if src != self.prev_src {
            // Reset prev_y
            self.prev_dst = 0;
        }
        let dst = self.prev_dst + self.stream.read_gamma().unwrap() as usize;
        let label = self.deserializer.deserialize(&mut self.stream).unwrap();
        self.prev_src = src;
        self.prev_dst = dst;
        self.current += 1;
        Some(((src, dst), label))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<D: BitDeserializer<NE, BitReader>> ExactSizeIterator for BatchIterator<D> {
    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Clone, Debug)]
/// Private struct that can be used to sort labeled pairs based only on the pair of
/// nodes and ignoring the label.
struct HeadTail<T, I: Iterator<Item = ((usize, usize), T)>> {
    head: ((usize, usize), T),
    tail: I,
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> PartialEq for HeadTail<T, I> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.head.0 == other.head.0
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> Eq for HeadTail<T, I> {}

impl<T, I: Iterator<Item = ((usize, usize), T)>> PartialOrd for HeadTail<T, I> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.head.0.cmp(&self.head.0))
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> Ord for HeadTail<T, I> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.head.0.cmp(&self.head.0)
    }
}

/// A structure using a [quaternary heap](dary_heap::QuaternaryHeap) to merge sorted iterators.
///
/// The iterators must be sorted by the pair of nodes, and the structure will return the labeled pairs
/// sorted by lexicographical order of the pairs of nodes.
///
/// The structure implements [`Iterator`] and returns labeled pairs of the form `((src, dst), label)`.
///
/// The structure implements [`Default`], [`core::iter::Sum`],
/// [`core::ops::AddAssign`], [`Extend`], and [`core::iter::FromIterator`]
/// so you can compute different KMergeIters / Iterators / IntoIterators in
/// parallel and then merge them using either `+=`, `sum()` or `collect()`:
/// ```rust
/// use webgraph::utils::sort_pairs::KMergeIters;
///
/// let (tx, rx) = std::sync::mpsc::channel();
///
/// std::thread::scope(|s| {
///     for _ in 0..10 {
///         let tx = tx.clone();
///         s.spawn(move || {
///             // create a dummy KMergeIters
///             tx.send(KMergeIters::new(vec![(0..10).map(|j| ((j, j), j + j))])).unwrap()
///         });
///     }
/// });
/// drop(tx);
/// // merge the KMergeIters
/// let merged = rx.iter().sum::<KMergeIters<core::iter::Map<core::ops::Range<usize>, _>, usize>>();
/// ```
/// or with plain iterators:
/// ```rust
/// use webgraph::utils::sort_pairs::KMergeIters;
///
/// let iter = vec![vec![((0, 0), 0), ((0, 1), 1)], vec![((1, 0), 1), ((1, 1), 2)]];
/// let merged = iter.into_iter().collect::<KMergeIters<_, usize>>();
/// ```
#[derive(Clone, Debug)]
pub struct KMergeIters<I: Iterator<Item = ((usize, usize), T)>, T = ()> {
    heap: dary_heap::QuaternaryHeap<HeadTail<T, I>>,
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> KMergeIters<I, T> {
    pub fn new(iters: impl IntoIterator<Item = I>) -> Self {
        let iters = iters.into_iter();
        let mut heap = dary_heap::QuaternaryHeap::with_capacity(iters.size_hint().1.unwrap_or(10));
        for mut iter in iters {
            if let Some((pair, label)) = iter.next() {
                heap.push(HeadTail {
                    head: (pair, label),
                    tail: iter,
                });
            }
        }
        KMergeIters { heap }
    }
}

unsafe impl<T, I: Iterator<Item = ((usize, usize), T)> + SortedIterator> SortedIterator
    for KMergeIters<I, T>
{
}

#[allow(clippy::uninit_assumed_init)]
impl<T, I: Iterator<Item = ((usize, usize), T)>> Iterator for KMergeIters<I, T> {
    type Item = ((usize, usize), T);

    fn next(&mut self) -> Option<Self::Item> {
        let mut head_tail = self.heap.peek_mut()?;

        match head_tail.tail.next() {
            None => Some(PeekMut::pop(head_tail).head),
            Some((pair, label)) => Some(std::mem::replace(&mut head_tail.head, (pair, label))),
        }
    }
}
impl<T, I: Iterator<Item = ((usize, usize), T)> + ExactSizeIterator> ExactSizeIterator
    for KMergeIters<I, T>
{
    fn len(&self) -> usize {
        self.heap
            .iter()
            .map(|head_tail| {
                // The head is always a labeled pair, so we can count it
                1 + head_tail.tail.len()
            })
            .sum()
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> core::default::Default for KMergeIters<I, T> {
    fn default() -> Self {
        KMergeIters {
            heap: dary_heap::QuaternaryHeap::default(),
        }
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> core::iter::Sum for KMergeIters<I, T> {
    fn sum<J: Iterator<Item = Self>>(iter: J) -> Self {
        let mut heap = dary_heap::QuaternaryHeap::default();
        for mut kmerge in iter {
            heap.extend(kmerge.heap.drain());
        }
        KMergeIters { heap }
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>> core::iter::Sum<I>
    for KMergeIters<I::IntoIter, T>
{
    fn sum<J: Iterator<Item = I>>(iter: J) -> Self {
        KMergeIters::new(iter.map(IntoIterator::into_iter))
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> core::iter::FromIterator<Self>
    for KMergeIters<I, T>
{
    fn from_iter<J: IntoIterator<Item = Self>>(iter: J) -> Self {
        iter.into_iter().sum()
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>> core::iter::FromIterator<I>
    for KMergeIters<I::IntoIter, T>
{
    fn from_iter<J: IntoIterator<Item = I>>(iter: J) -> Self {
        KMergeIters::new(iter.into_iter().map(IntoIterator::into_iter))
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>> core::ops::AddAssign<I>
    for KMergeIters<I::IntoIter, T>
{
    fn add_assign(&mut self, rhs: I) {
        let mut rhs = rhs.into_iter();
        if let Some((pair, label)) = rhs.next() {
            self.heap.push(HeadTail {
                head: (pair, label),
                tail: rhs,
            });
        }
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> core::ops::AddAssign for KMergeIters<I, T> {
    fn add_assign(&mut self, mut rhs: Self) {
        self.heap.extend(rhs.heap.drain());
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>> Extend<I> for KMergeIters<I::IntoIter, T> {
    fn extend<J: IntoIterator<Item = I>>(&mut self, iter: J) {
        self.heap.extend(iter.into_iter().filter_map(|iter| {
            let mut iter = iter.into_iter();
            let (pair, label) = iter.next()?;
            Some(HeadTail {
                head: (pair, label),
                tail: iter,
            })
        }));
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> Extend<KMergeIters<I, T>> for KMergeIters<I, T> {
    fn extend<J: IntoIterator<Item = KMergeIters<I, T>>>(&mut self, iter: J) {
        for mut kmerge in iter {
            self.heap.extend(kmerge.heap.drain());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    struct MyDessert;

    impl BitDeserializer<NE, BitReader> for MyDessert {
        type DeserType = usize;
        fn deserialize(
            &self,
            bitstream: &mut BitReader,
        ) -> Result<Self::DeserType, <BitReader as BitRead<NE>>::Error> {
            bitstream.read_delta().map(|x| x as usize)
        }
    }

    impl BitSerializer<NE, BitWriter> for MyDessert {
        type SerType = usize;
        fn serialize(
            &self,
            value: &Self::SerType,
            bitstream: &mut BitWriter,
        ) -> Result<usize, <BitWriter as BitWrite<NE>>::Error> {
            bitstream.write_delta(*value as u64)
        }
    }

    #[test]
    fn test_sort_pairs() -> anyhow::Result<()> {
        use tempfile::Builder;

        let dir = Builder::new().prefix("test_sort_pairs_").tempdir()?;
        let mut sp =
            SortPairs::new_labeled(MemoryUsage::BatchSize(10), dir.path(), MyDessert, MyDessert)?;
        let n = 25;
        for i in 0..n {
            sp.push_labeled(i, i + 1, i + 2)?;
        }
        let mut iter = sp.iter()?;
        let mut cloned = iter.clone();

        for _ in 0..n {
            let ((x, y), p) = iter.next().unwrap();
            println!("{} {} {}", x, y, p);
            assert_eq!(x + 1, y);
            assert_eq!(x + 2, p);
        }

        for _ in 0..n {
            let ((x, y), p) = cloned.next().unwrap();
            println!("{} {} {}", x, y, p);
            assert_eq!(x + 1, y);
            assert_eq!(x + 2, p);
        }
        Ok(())
    }

    #[test]
    fn test_sort_and_sort_labeled() -> anyhow::Result<()> {
        use tempfile::Builder;

        // Test unlabeled sort
        let dir = Builder::new().prefix("test_sort_").tempdir()?;
        let mut sp = SortPairs::new(MemoryUsage::BatchSize(10), dir.path())?;

        let pairs = vec![(3, 4), (1, 2), (5, 6), (0, 1), (2, 3)];
        let mut iter = sp.sort(pairs)?;

        let mut sorted_pairs = Vec::new();
        while let Some(((x, y), _)) = iter.next() {
            sorted_pairs.push((x, y));
        }
        assert_eq!(sorted_pairs, vec![(0, 1), (1, 2), (2, 3), (3, 4), (5, 6)]);

        // Test labeled sort
        let dir2 = Builder::new().prefix("test_sort_labeled_").tempdir()?;
        let mut sp2 =
            SortPairs::new_labeled(MemoryUsage::BatchSize(5), dir2.path(), MyDessert, MyDessert)?;

        let labeled_pairs = vec![
            ((3, 4), 7),
            ((1, 2), 5),
            ((5, 6), 9),
            ((0, 1), 4),
            ((2, 3), 6),
        ];
        let mut iter2 = sp2.sort_labeled(labeled_pairs)?;

        let mut sorted_labeled = Vec::new();
        while let Some(((x, y), label)) = iter2.next() {
            sorted_labeled.push((x, y, label));
        }
        assert_eq!(
            sorted_labeled,
            vec![(0, 1, 4), (1, 2, 5), (2, 3, 6), (3, 4, 7), (5, 6, 9)]
        );

        Ok(())
    }
}
