/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Facilities to merge sorted iterators of labeled pairs.

use crate::traits::SortedIterator;
use dary_heap::PeekMut;

/// Private struct that keeps the head of an iterator and its tail.
///
/// Note that we cannot use [`Peekable`] for the same purpose because
/// [`Peekable::peek`] needs a mutable reference, but we would be calling it
/// inside [`Ord::cmp`], which only has an immutable reference.
///
/// [`Peekable`]: std::iter::Peekable
/// [`Peekable::peek`]: std::iter::Peekable::peek
/// [`Ord::cmp`]: std::cmp::Ord::cmp
///
/// Comparison is implemented only on the pair of nodes and ignoring the label.
#[derive(Clone, Debug)]
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
        Some(self.cmp(other))
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> Ord for HeadTail<T, I> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.head.0.cmp(&self.head.0)
    }
}

/// Builds the heap used by [`KMergeIters`] from a collection of sorted
/// iterators.
fn build_kmerge_heap<T, I: Iterator<Item = ((usize, usize), T)>>(
    iters: impl IntoIterator<Item = I>,
) -> dary_heap::QuaternaryHeap<HeadTail<T, I>> {
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
    heap
}

/// A structure using a [quaternary heap] to merge sorted iterators.
///
/// [quaternary heap]: dary_heap::QuaternaryHeap
///
/// The iterators must be sorted by the pair of nodes, and the structure will return the labeled pairs
/// sorted by lexicographical order of the pairs of nodes.
///
/// The structure implements [`Iterator`] and returns labeled pairs of the form `((src, dst), label)`.
///
/// If `DEDUP` is `true`, the iterator will skip consecutive elements sharing
/// the same pair of nodes, keeping only the first occurrence. Use
/// [`new_dedup`] to enable deduplication.
///
/// [`new_dedup`]: KMergeIters::new_dedup
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
pub struct KMergeIters<I: Iterator<Item = ((usize, usize), T)>, T = (), const DEDUP: bool = false> {
    heap: dary_heap::QuaternaryHeap<HeadTail<T, I>>,
    /// The last pair returned, used for deduplication.
    last_pair: Option<(usize, usize)>,
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> KMergeIters<I, T, DEDUP> {
    pub fn new(iters: impl IntoIterator<Item = I>) -> Self {
        KMergeIters {
            heap: build_kmerge_heap(iters),
            last_pair: None,
        }
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>> KMergeIters<I, T> {
    /// Creates a new `KMergeIters` that deduplicates consecutive elements
    /// sharing the same pair of nodes.
    pub fn new_dedup(iters: impl IntoIterator<Item = I>) -> KMergeIters<I, T, true> {
        KMergeIters {
            heap: build_kmerge_heap(iters),
            last_pair: None,
        }
    }
}

// SAFETY: the merge of sorted iterators is itself sorted.
unsafe impl<T, I: Iterator<Item = ((usize, usize), T)> + SortedIterator, const DEDUP: bool>
    SortedIterator for KMergeIters<I, T, DEDUP>
{
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> Iterator
    for KMergeIters<I, T, DEDUP>
{
    type Item = ((usize, usize), T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut head_tail = self.heap.peek_mut()?;

            let result = match head_tail.tail.next() {
                None => PeekMut::pop(head_tail).head,
                Some((pair, label)) => std::mem::replace(&mut head_tail.head, (pair, label)),
            };

            if DEDUP {
                if self.last_pair == Some(result.0) {
                    continue;
                }
                self.last_pair = Some(result.0);
            }

            return Some(result);
        }
    }

    fn count(self) -> usize {
        if DEDUP {
            self.fold(0, |count, _| count + 1)
        } else {
            self.heap
                .into_iter()
                .map(|head_tail| 1 + head_tail.tail.count())
                .sum()
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

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::iter::FusedIterator
    for KMergeIters<I, T, DEDUP>
{
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::default::Default
    for KMergeIters<I, T, DEDUP>
{
    fn default() -> Self {
        KMergeIters {
            heap: dary_heap::QuaternaryHeap::default(),
            last_pair: None,
        }
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::iter::Sum
    for KMergeIters<I, T, DEDUP>
{
    fn sum<J: Iterator<Item = Self>>(iter: J) -> Self {
        let mut heap = dary_heap::QuaternaryHeap::default();
        for mut kmerge in iter {
            heap.extend(kmerge.heap.drain());
        }
        KMergeIters {
            heap,
            last_pair: None,
        }
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::iter::Sum<I>
    for KMergeIters<I::IntoIter, T, DEDUP>
{
    fn sum<J: Iterator<Item = I>>(iter: J) -> Self {
        KMergeIters::new(iter.map(IntoIterator::into_iter))
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::iter::FromIterator<Self>
    for KMergeIters<I, T, DEDUP>
{
    fn from_iter<J: IntoIterator<Item = Self>>(iter: J) -> Self {
        iter.into_iter().sum()
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::iter::FromIterator<I>
    for KMergeIters<I::IntoIter, T, DEDUP>
{
    fn from_iter<J: IntoIterator<Item = I>>(iter: J) -> Self {
        KMergeIters::new(iter.into_iter().map(IntoIterator::into_iter))
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::ops::AddAssign<I>
    for KMergeIters<I::IntoIter, T, DEDUP>
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

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> core::ops::AddAssign
    for KMergeIters<I, T, DEDUP>
{
    fn add_assign(&mut self, mut rhs: Self) {
        self.heap.extend(rhs.heap.drain());
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, const DEDUP: bool> Extend<I>
    for KMergeIters<I::IntoIter, T, DEDUP>
{
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

impl<T, I: Iterator<Item = ((usize, usize), T)>, const DEDUP: bool> Extend<KMergeIters<I, T, DEDUP>>
    for KMergeIters<I, T, DEDUP>
{
    fn extend<J: IntoIterator<Item = KMergeIters<I, T, DEDUP>>>(&mut self, iter: J) {
        for mut kmerge in iter {
            self.heap.extend(kmerge.heap.drain());
        }
    }
}
