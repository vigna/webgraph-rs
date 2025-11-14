/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Facilities to sort externally pairs of nodes with an associated label.
#![allow(clippy::non_canonical_partial_ord_impl)]

use crate::{
    traits::SortedIterator,
    utils::{BatchCodec, CodecIter, DefaultBatchCodec, MemoryUsage},
};
use anyhow::{anyhow, Context};
use dary_heap::PeekMut;
use std::path::{Path, PathBuf};

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
/// The structure accept as type parameter a [`BatchCodec`] is used to serialize
/// and deserialize the triples.
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
pub struct SortPairs<C: BatchCodec = DefaultBatchCodec> {
    /// The batch size.
    batch_size: usize,
    /// Where we are going to store the batches.
    tmp_dir: PathBuf,
    /// A potentially stateful serializer and deserializer we will pass to batch iterators to serialize
    /// the labels to a bitstream.
    batch_codec: C,
    /// Keeps track of how many batches we created.
    num_batches: usize,
    /// The length of the last batch, which might be smaller than [`SortPairs::batch_size`].
    last_batch_len: usize,
    /// The batch of triples we are currently building.
    batch: Vec<((usize, usize), C::Label)>,
}

impl SortPairs {
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
        Self::new_labeled(memory_usage, tmp_dir, DefaultBatchCodec::default())
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
    ) -> anyhow::Result<KMergeIters<CodecIter<DefaultBatchCodec>>> {
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
    ) -> anyhow::Result<KMergeIters<CodecIter<DefaultBatchCodec>, ()>> {
        for pair in pairs {
            let (x, y) = pair.map_err(Into::into)?;
            self.push(x, y)?;
        }
        self.iter()
    }
}

impl<C: BatchCodec> SortPairs<C> {
    /// Creates a new `SortPairs` with labels.
    ///
    /// The `dir` must be empty, and in particular it must not be shared
    /// with other `SortPairs` instances. Please use the
    /// [`tempfile`](https://crates.io/crates/tempfile) crate to obtain
    /// a suitable directory.
    pub fn new_labeled<P: AsRef<Path>>(
        memory_usage: MemoryUsage,
        dir: P,
        batch_codec: C,
    ) -> anyhow::Result<Self> {
        let dir = dir.as_ref();
        let mut dir_entries =
            std::fs::read_dir(dir).with_context(|| format!("Could not list {}", dir.display()))?;
        if dir_entries.next().is_some() {
            Err(anyhow!("{} is not empty", dir.display()))
        } else {
            let batch_size = memory_usage.batch_size::<(usize, usize, C::Label)>();
            Ok(SortPairs {
                batch_size,
                batch_codec,
                tmp_dir: dir.to_owned(),
                num_batches: 0,
                last_batch_len: 0,
                batch: Vec::with_capacity(batch_size),
            })
        }
    }

    /// Adds a labeled pair to the graph.
    pub fn push_labeled(&mut self, x: usize, y: usize, t: C::Label) -> anyhow::Result<()> {
        self.batch.push(((x, y), t));
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

        let batch_path = self.tmp_dir.join(format!("{:06x}", self.num_batches));
        let start = std::time::Instant::now();
        let (bit_size, stats) = self.batch_codec.encode_batch(batch_path, &mut self.batch)?;
        log::info!(
            "Dumped batch {} with {} arcs ({} bits, {:.2} bits / arc) in {:.3} seconds, stats: {}",
            self.num_batches,
            self.batch.len(),
            bit_size,
            bit_size as f64 / self.batch.len() as f64,
            start.elapsed().as_secs_f64(),
            stats
        );
        self.last_batch_len = self.batch.len();
        self.batch.clear();
        self.num_batches += 1;
        Ok(())
    }

    /// Returns an iterator over the labeled pairs, lexicographically sorted.
    pub fn iter(&mut self) -> anyhow::Result<KMergeIters<CodecIter<C>, C::Label>> {
        self.dump()?;
        Ok(KMergeIters::new((0..self.num_batches).map(|batch_idx| {
            let batch_path = self.tmp_dir.join(format!("{batch_idx:06x}"));
            self.batch_codec
                .decode_batch(batch_path)
                .unwrap()
                .into_iter()
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
        pairs: impl IntoIterator<Item = ((usize, usize), C::Label)>,
    ) -> anyhow::Result<KMergeIters<CodecIter<C>, C::Label>> {
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
        pairs: impl IntoIterator<Item = Result<((usize, usize), C::Label), E>>,
    ) -> anyhow::Result<KMergeIters<CodecIter<C>, C::Label>> {
        for pair in pairs {
            let ((x, y), label) = pair.map_err(Into::into)?;
            self.push_labeled(x, y, label)?;
        }
        self.iter()
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
    use crate::{
        traits::{BitDeserializer, BitSerializer},
        utils::{gaps::GapsCodec, BitReader, BitWriter},
    };
    use dsi_bitstream::prelude::*;

    #[derive(Clone, Debug)]
    struct MyDessert<E: Endianness> {
        _marker: std::marker::PhantomData<E>,
    }

    impl<E: Endianness> Default for MyDessert<E> {
        fn default() -> Self {
            MyDessert {
                _marker: std::marker::PhantomData,
            }
        }
    }

    impl<E: Endianness> BitDeserializer<E, BitReader<E>> for MyDessert<E>
    where
        BitReader<E>: BitRead<E> + CodesRead<E>,
    {
        type DeserType = usize;
        fn deserialize(
            &self,
            bitstream: &mut BitReader<E>,
        ) -> Result<Self::DeserType, <BitReader<E> as BitRead<E>>::Error> {
            bitstream.read_delta().map(|x| x as usize)
        }
    }

    impl<E: Endianness> BitSerializer<E, BitWriter<E>> for MyDessert<E>
    where
        BitWriter<E>: BitWrite<E> + CodesWrite<E>,
    {
        type SerType = usize;
        fn serialize(
            &self,
            value: &Self::SerType,
            bitstream: &mut BitWriter<E>,
        ) -> Result<usize, <BitWriter<E> as BitWrite<E>>::Error> {
            bitstream.write_delta(*value as u64)
        }
    }

    #[test]
    fn test_sort_pairs() -> anyhow::Result<()> {
        use tempfile::Builder;

        let dir = Builder::new().prefix("test_sort_pairs_").tempdir()?;
        let mut sp = SortPairs::new_labeled(
            MemoryUsage::BatchSize(10),
            dir.path(),
            GapsCodec::<BE, MyDessert<BE>, MyDessert<BE>>::default(),
        )?;

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
        let iter = sp.sort(pairs)?;

        let mut sorted_pairs = Vec::new();
        for ((x, y), _) in iter {
            sorted_pairs.push((x, y));
        }
        assert_eq!(sorted_pairs, vec![(0, 1), (1, 2), (2, 3), (3, 4), (5, 6)]);

        // Test labeled sort
        let dir2 = Builder::new().prefix("test_sort_labeled_").tempdir()?;
        let mut sp2 = SortPairs::new_labeled(
            MemoryUsage::BatchSize(5),
            dir2.path(),
            GapsCodec::<BE, MyDessert<BE>, MyDessert<BE>>::default(),
        )?;

        let labeled_pairs = vec![
            ((3, 4), 7),
            ((1, 2), 5),
            ((5, 6), 9),
            ((0, 1), 4),
            ((2, 3), 6),
        ];
        let iter2 = sp2.sort_labeled(labeled_pairs)?;

        let mut sorted_labeled = Vec::new();
        for ((x, y), label) in iter2 {
            sorted_labeled.push((x, y, label));
        }
        assert_eq!(
            sorted_labeled,
            vec![(0, 1, 4), (1, 2, 5), (2, 3, 6), (3, 4, 7), (5, 6, 9)]
        );

        Ok(())
    }
}
