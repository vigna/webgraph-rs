/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Facilities to merge sorted iterators of labeled pairs.

use crate::traits::SortedIterator;
use crate::utils::{MergePolicy, NoMerge};
use dary_heap::PeekMut;

/// Private struct that keeps the head of an iterator and its tail.
///
/// Note that we cannot use [`Peekable`] for the same purpose because
/// [`Peekable::peek`] needs a mutable reference, but we would be calling it
/// inside [`Ord::cmp`], which only has an immutable reference.
///
/// Comparison is implemented only on the pair of nodes and ignoring the label.
///
/// [`Peekable`]: std::iter::Peekable
/// [`Peekable::peek`]: std::iter::Peekable::peek
/// [`Ord::cmp`]: std::cmp::Ord::cmp
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
/// The iterators must be sorted by the pair of nodes, and the structure will
/// return the labeled pairs sorted by lexicographical order of the pairs of
/// nodes.
///
/// The structure implements [`Iterator`] and returns labeled pairs of the form
/// `((src, dst), label)`.
///
/// The [merge policy] `P` decides what happens to pairs appearing several
/// times: with [`NoMerge`] (the default), they are all returned; with
/// [`Dedup`], only one of them is returned, with one of their labels; with
/// [`WithMerge`], only one of them is returned, with a label obtained by
/// merging their labels, which are then states of a [`MergeLabels`], and by
/// [finishing] the result. The type of the returned labels is thus given by
/// [`MergePolicy::Output`].
///
/// Finally, the type parameter `A` can be used to include an *anchor*, which is
/// usually `Arc<TempDir>`. The purpose of the ancor is to keep alive some value
/// whose destructor would cause the iterators to become invalid, such as a
/// temporary directory containing memory-mapped files. The anchor is ignored by
/// the logic of the structure, and is only kept alive as long as the structure
/// is. The default value `()` makes the anchor disappear.
///
/// The structure also implements [`Default`], [`core::iter::Sum`],
/// [`core::ops::AddAssign`], [`Extend`], and [`core::iter::FromIterator`],
/// so `KMergeIters` instances can be merged using `+=`, `sum()`, or
/// `collect()`. Constructors not taking a policy require it to implement
/// [`Default`].
///
/// # Examples
///
/// ```rust
/// use webgraph::utils::kmerge_iters::KMergeIters;
///
/// let iter = vec![vec![((0, 0), 0), ((0, 1), 1)], vec![((1, 0), 1), ((1, 1), 2)]];
/// let merged: KMergeIters<_, i32> =
///     KMergeIters::new(iter.into_iter().map(|v| v.into_iter()));
/// ```
///
/// Counting the occurrences of each pair:
///
/// ```rust
/// use webgraph::traits::CountRepeats;
/// use webgraph::utils::WithMerge;
/// use webgraph::utils::kmerge_iters::KMergeIters;
///
/// // The labels are partial counts
/// let iter = vec![vec![((0, 1), 2), ((1, 0), 1)], vec![((0, 1), 1)]];
/// let merged: KMergeIters<_, u64, WithMerge<CountRepeats>> =
///     KMergeIters::new(iter.into_iter().map(|v| v.into_iter()));
/// assert_eq!(merged.collect::<Vec<_>>(), [((0, 1), 3), ((1, 0), 1)]);
/// ```
///
/// [quaternary heap]: dary_heap::QuaternaryHeap
/// [merge policy]: crate::utils::merge_policy
/// [`Dedup`]: crate::utils::Dedup
/// [`WithMerge`]: crate::utils::WithMerge
/// [`MergeLabels`]: crate::traits::MergeLabels
/// [finishing]: crate::traits::MergeLabels::finish
#[derive(Debug)]
pub struct KMergeIters<
    I: Iterator<Item = ((usize, usize), T)>,
    T = (),
    P = NoMerge,
    A: Send + Sync = (),
> {
    heap: dary_heap::QuaternaryHeap<HeadTail<T, I>>,
    policy: P,
    _anchor: A,
}

impl<T: Clone, I: Clone + Iterator<Item = ((usize, usize), T)>, P: Clone, A: Clone + Send + Sync>
    Clone for KMergeIters<I, T, P, A>
{
    fn clone(&self) -> Self {
        Self {
            heap: self.heap.clone(),
            policy: self.policy.clone(),
            _anchor: self._anchor.clone(),
        }
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, P: Default> KMergeIters<I, T, P, ()> {
    /// Creates a new `KMergeIters` merging the given iterators.
    pub fn new(iters: impl IntoIterator<Item = I>) -> Self {
        Self::with_policy_and_anchor(iters, P::default(), ())
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, P: Default, A: Send + Sync>
    KMergeIters<I, T, P, A>
{
    /// Creates a new `KMergeIters` with the given anchor.
    ///
    /// The anchor is kept alive as long as this iterator (or any of its
    /// clones) exists. This is typically used to prevent a [`TempDir`] from
    /// being deleted while the iterators still read from memory-mapped files
    /// inside it. The typical anchor type is `Arc<TempDir>`.
    ///
    /// [`TempDir`]: tempfile::TempDir
    pub fn with_anchor(iters: impl IntoIterator<Item = I>, anchor: A) -> Self {
        Self::with_policy_and_anchor(iters, P::default(), anchor)
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, P, A: Send + Sync> KMergeIters<I, T, P, A> {
    /// Creates a new `KMergeIters` with the given [merge policy] and anchor.
    ///
    /// See [`with_anchor`] for the purpose of the anchor.
    ///
    /// [merge policy]: crate::utils::merge_policy
    /// [`with_anchor`]: Self::with_anchor
    pub fn with_policy_and_anchor(
        iters: impl IntoIterator<Item = I>,
        policy: P,
        anchor: A,
    ) -> Self {
        KMergeIters {
            heap: build_kmerge_heap(iters),
            policy,
            _anchor: anchor,
        }
    }
}

// SAFETY: the merge of sorted iterators is itself sorted.
unsafe impl<
    T,
    I: Iterator<Item = ((usize, usize), T)> + SortedIterator,
    P: MergePolicy<T>,
    A: Send + Sync,
> SortedIterator for KMergeIters<I, T, P, A>
{
}

/// Removes the head of the iterator at the top of the heap, replacing it
/// with the next element of the iterator, or removing the iterator if it is
/// exhausted.
#[inline(always)]
fn take_head<T, I: Iterator<Item = ((usize, usize), T)>>(
    mut head_tail: PeekMut<'_, HeadTail<T, I>, 4>,
) -> ((usize, usize), T) {
    match head_tail.tail.next() {
        None => PeekMut::pop(head_tail).head,
        Some((pair, label)) => std::mem::replace(&mut head_tail.head, (pair, label)),
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, P: MergePolicy<T>, A: Send + Sync> Iterator
    for KMergeIters<I, T, P, A>
{
    type Item = ((usize, usize), P::Output);

    fn next(&mut self) -> Option<Self::Item> {
        let (pair, mut state) = take_head(self.heap.peek_mut()?);
        if P::COLLAPSES {
            while self
                .heap
                .peek()
                .is_some_and(|head_tail| head_tail.head.0 == pair)
            {
                // The heap is not empty, as we just peeked
                let (_, other) = take_head(self.heap.peek_mut().unwrap());
                self.policy.merge(&mut state, other);
            }
        }
        Some((pair, self.policy.finish(state)))
    }

    fn count(self) -> usize {
        if P::COLLAPSES {
            self.fold(0, |count, _| count + 1)
        } else {
            self.heap
                .into_iter()
                .map(|head_tail| 1 + head_tail.tail.count())
                .sum()
        }
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)> + ExactSizeIterator, A: Send + Sync>
    ExactSizeIterator for KMergeIters<I, T, NoMerge, A>
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

impl<T, I: Iterator<Item = ((usize, usize), T)>, P: MergePolicy<T>, A: Send + Sync>
    core::iter::FusedIterator for KMergeIters<I, T, P, A>
{
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, P: Default> core::default::Default
    for KMergeIters<I, T, P>
{
    fn default() -> Self {
        KMergeIters {
            heap: dary_heap::QuaternaryHeap::default(),
            policy: P::default(),
            _anchor: (),
        }
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, P: Default> core::iter::Sum
    for KMergeIters<I, T, P>
{
    fn sum<J: Iterator<Item = Self>>(iter: J) -> Self {
        let mut heap = dary_heap::QuaternaryHeap::default();
        for mut kmerge in iter {
            heap.extend(kmerge.heap.drain());
        }
        KMergeIters {
            heap,
            policy: P::default(),
            _anchor: (),
        }
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, P: Default> core::iter::Sum<I>
    for KMergeIters<I::IntoIter, T, P>
{
    fn sum<J: Iterator<Item = I>>(iter: J) -> Self {
        KMergeIters::new(iter.map(IntoIterator::into_iter))
    }
}

impl<T, I: Iterator<Item = ((usize, usize), T)>, P: Default> core::iter::FromIterator<Self>
    for KMergeIters<I, T, P>
{
    fn from_iter<J: IntoIterator<Item = Self>>(iter: J) -> Self {
        iter.into_iter().sum()
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, P: Default> core::iter::FromIterator<I>
    for KMergeIters<I::IntoIter, T, P>
{
    fn from_iter<J: IntoIterator<Item = I>>(iter: J) -> Self {
        KMergeIters::new(iter.into_iter().map(IntoIterator::into_iter))
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, P> core::ops::AddAssign<I>
    for KMergeIters<I::IntoIter, T, P>
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

impl<T, I: Iterator<Item = ((usize, usize), T)>, P> core::ops::AddAssign for KMergeIters<I, T, P> {
    fn add_assign(&mut self, mut rhs: Self) {
        self.heap.extend(rhs.heap.drain());
    }
}

impl<T, I: IntoIterator<Item = ((usize, usize), T)>, P> Extend<I>
    for KMergeIters<I::IntoIter, T, P>
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

impl<T, I: Iterator<Item = ((usize, usize), T)>, P> Extend<KMergeIters<I, T, P>>
    for KMergeIters<I, T, P>
{
    fn extend<J: IntoIterator<Item = KMergeIters<I, T, P>>>(&mut self, iter: J) {
        for mut kmerge in iter {
            self.heap.extend(kmerge.heap.drain());
        }
    }
}
