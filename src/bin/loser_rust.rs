use itertools;

use std::fmt;
use std::mem::replace;

/// Head element and Tail iterator pair
///
/// `PartialEq`, `Eq`, `PartialOrd` and `Ord` are implemented by comparing sequences based on
/// first items (which are guaranteed to exist).
///
/// The meanings of `PartialOrd` and `Ord` are reversed so as to turn the heap used in
/// `KMerge` into a min-heap.
#[derive(Debug)]
struct HeadTail<I>
where
    I: Iterator,
{
    head: I::Item,
    tail: I,
}

impl<I> HeadTail<I>
where
    I: Iterator,
{
    /// Constructs a `HeadTail` from an `Iterator`. Returns `None` if the `Iterator` is empty.
    fn new(mut it: I) -> Option<HeadTail<I>> {
        let head = it.next();
        head.map(|h| HeadTail { head: h, tail: it })
    }

    /// Get the next element and update `head`, returning the old head in `Some`.
    ///
    /// Returns `None` when the tail is exhausted (only `head` then remains).
    fn next(&mut self) -> Option<I::Item> {
        if let Some(next) = self.tail.next() {
            Some(replace(&mut self.head, next))
        } else {
            None
        }
    }
}

/// An iterator adaptor that merges an abitrary number of base iterators in ascending order.
/// If all base iterators are sorted (ascending), the result is sorted.
///
/// Iterator element type is `I::Item`.
///
/// See [`.kmerge()`](crate::Itertools::kmerge) for more information.
pub type KMerge<I> = KMergeBy<I, KMergeByLt>;

pub trait KMergePredicate<T> {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> bool;
}

#[derive(Clone, Debug)]
pub struct KMergeByLt;

impl<T: PartialOrd> KMergePredicate<T> for KMergeByLt {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> bool {
        a < b
    }
}

impl<T, F: FnMut(&T, &T) -> bool> KMergePredicate<T> for F {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> bool {
        self(a, b)
    }
}

/// Create an iterator that merges elements of the contained iterators using
/// the ordering function.
///
/// [`IntoIterator`] enabled version of [`Itertools::kmerge`].
///
/// ```
/// use itertools::kmerge;
///
/// for elt in kmerge(vec![vec![0, 2, 4], vec![1, 3, 5], vec![6, 7]]) {
///     /* loop body */
/// }
/// ```
pub fn kmerge<I>(iterable: I) -> KMerge<<I::Item as IntoIterator>::IntoIter>
where
    I: IntoIterator,
    I::Item: IntoIterator,
    <<I as IntoIterator>::Item as IntoIterator>::Item: PartialOrd,
{
    kmerge_by(iterable, KMergeByLt)
}

/// An iterator adaptor that merges an abitrary number of base iterators
/// according to an ordering function.
///
/// Iterator element type is `I::Item`.
///
/// See [`.kmerge_by()`](crate::Itertools::kmerge_by) for more
/// information.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct KMergeBy<I, F>
where
    I: Iterator,
{
    heap: Vec<HeadTail<I>>,
    tree: Vec<usize>,
    exhausted: Vec<bool>,
    less_than: F,
}

/// Create an iterator that merges elements of the contained iterators.
///
/// [`IntoIterator`] enabled version of [`Itertools::kmerge_by`].
pub fn kmerge_by<I, F>(
    iterable: I,
    mut less_than: F,
) -> KMergeBy<<I::Item as IntoIterator>::IntoIter, F>
where
    I: IntoIterator,
    I::Item: IntoIterator,
    F: KMergePredicate<<<I as IntoIterator>::Item as IntoIterator>::Item>,
{
    let iter = iterable.into_iter();
    let (lower, _) = iter.size_hint();
    let heap: Vec<_> = iter
        .filter_map(|it| HeadTail::new(it.into_iter()))
        .collect();
    let len = heap.len();
    let mut tree = vec![0_usize; len];
    // Winner tree
    // Safe pairs of data
    let mut safe = (len + 1) / 2;
    for i in (safe..len).rev() {
        let (a, b) = (2 * i - len, 2 * i - len + 1);
        tree[i] = if less_than.kmerge_pred(&heap[a].head, &heap[b].head) {
            a
        } else {
            b
        };
    }
    // Obnoxious edge case
    /*if len % 2 != 0 {
        safe -= 1;
        tree[safe] = min_by_key(tree[2 * safe], 2 * safe + 1 - len, |x| &data[*x]);
    }*/
    // Safe pairs in the tree
    for i in (1..safe).rev() {
        let (a, b) = (tree[2 * i], tree[2 * i + 1]);
        tree[i] = if less_than.kmerge_pred(&heap[a].head, &heap[b].head) {
            a
        } else {
            b
        };
    }

    // Loser tree
    // Safe pairs in the tree
    let mut safe = len / 2;
    tree[0] = tree[1]; // winner
    for i in 1..safe {
        let (a, b) = (tree[2 * i], tree[2 * i + 1]);
        tree[i] = if less_than.kmerge_pred(&heap[a].head, &heap[b].head) {
            b
        } else {
            a
        };
    }
    // Obnoxious edge case
    /*     if len % 2 != 0 {
        tree[safe] = max_by_key(tree[2 * safe], 2 * safe + 1 - len, |x| &data[*x]);
        safe += 1;
    }*/
    // Safe pairs of data
    for i in safe..len {
        let (a, b) = (2 * i - len, 2 * i - len + 1);
        tree[i] = if less_than.kmerge_pred(&heap[a].head, &heap[b].head) {
            b
        } else {
            a
        };
    }

    let exhausted = vec![false; len];

    KMergeBy {
        heap,
        tree,
        exhausted,
        less_than,
    }
}

impl<I, F> Iterator for KMergeBy<I, F>
where
    I: Iterator,
    F: KMergePredicate<I::Item>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let tree = &mut self.tree;
        let heap = &mut self.heap;
        let exhausted = &mut self.exhausted;
        let len = heap.len();

        let mut winner = tree[0];
        let result = heap[winner].next().unwrap();

        let mut parent = (winner + len) / 2;

        while parent != 0 {
            if !exhausted[tree[parent]]
                && (exhausted[winner]
                    || self
                        .less_than
                        .kmerge_pred(&heap[tree[parent]].head, &heap[winner].head))
            {
                std::mem::swap(&mut tree[parent], &mut winner);
            }
            parent = parent / 2;
        }
        tree[0] = winner;
        Some(result)
    }
}

fn main() {
    let mut v = vec![];
    for i in 0..4 {
        v.push((0..1_000_000_000).into_iter());
    }
    //let mut m = itertools::kmerge(v);

    let mut m = kmerge(v);
    let mut curr = 0;
    for i in 0..(4000000000_usize - 4) {
        let next = m.next().unwrap();
        assert!(curr <= next);
        curr = next;
        //std::hint::black_box(m.next());
    }
}
