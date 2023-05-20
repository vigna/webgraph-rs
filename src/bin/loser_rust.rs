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

//#[derive(Debug)]
pub struct OptionalHeadTail<I: Iterator>(Option<HeadTail<I>>);

impl<I> OptionalHeadTail<I>
where
    I: Iterator,
{
    /// Constructs a `HeadTail` from an `Iterator`. Returns `None` if the `Iterator` is empty.
    fn new(mut it: I) -> OptionalHeadTail<I> {
        let head = it.next();
        OptionalHeadTail(head.map(|h| HeadTail { head: h, tail: it }))
    }

    /// Get the next element and update `head`, returning the old head in `Some`.
    ///
    /// Returns `None` when the tail is exhausted (only `head` then remains).
    fn next(&mut self, count: &mut usize) -> Option<I::Item> {
        if let Some(HeadTail { head, tail }) = &mut self.0 {
            if let Some(next) = tail.next() {
                return Some(replace(head, next));
            } else {
                *count -= 1;
                return replace(&mut self.0, None).map(|ht| ht.head);
            }
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

pub trait KMergePredicate<T: Iterator> {
    fn kmerge_pred(&mut self, a: &OptionalHeadTail<T>, b: &OptionalHeadTail<T>) -> bool;
}

#[derive(Clone, Debug)]
pub struct KMergeByLt;

impl<T: Iterator> KMergePredicate<T> for KMergeByLt
where
    T::Item: PartialOrd,
{
    fn kmerge_pred(&mut self, a: &OptionalHeadTail<T>, b: &OptionalHeadTail<T>) -> bool {
        if let Some(a) = &a.0 {
            if let Some(b) = &b.0 {
                return a.head < b.head;
            } else {
                return true;
            }
        }
        false
    }
}

impl<T: Iterator, F: FnMut(&T, &T) -> bool> KMergePredicate<T> for F
where
    T::Item: PartialOrd,
{
    fn kmerge_pred(&mut self, a: &OptionalHeadTail<T>, b: &OptionalHeadTail<T>) -> bool {
        if let Some(a) = &a.0 {
            if let Some(b) = &b.0 {
                return true; //self(a.head, b.head);
            }
        }
        false
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
    src: Vec<OptionalHeadTail<I>>,
    tree: Vec<usize>,
    active: usize,
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
    F: KMergePredicate<<<I as IntoIterator>::Item as IntoIterator>::IntoIter>,
{
    let iter = iterable.into_iter();
    let (lower, _) = iter.size_hint();
    let src: Vec<_> = iter
        .map(|it| OptionalHeadTail::new(it.into_iter()))
        .collect();
    let active = src.iter().filter(|&x| x.0.is_some()).count();
    let len = src.len();
    let mut tree = vec![0_usize; len];

    let idx_to_item = |idx: usize, tree: &Vec<usize>| -> usize {
        if idx < len {
            tree[idx]
        } else {
            idx - len
        }
    };

    // Winner tree
    for i in (1..len).rev() {
        let (left, right) = (idx_to_item(2 * i, &tree), idx_to_item(2 * i + 1, &tree));
        tree[i] = if less_than.kmerge_pred(&src[left], &src[right]) {
            left
        } else {
            right
        };
    }
    // Loser tree
    if len > 1 {
        tree[0] = tree[1]; // winner
    }
    for i in 1..len {
        let (left, right) = (idx_to_item(2 * i, &tree), idx_to_item(2 * i + 1, &tree));
        tree[i] = if less_than.kmerge_pred(&src[left], &src[right]) {
            right
        } else {
            left
        };
    }

    KMergeBy {
        src,
        tree,
        active,
        less_than,
    }
}

impl<I, F> Iterator for KMergeBy<I, F>
where
    I: Iterator,
    F: KMergePredicate<I>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let tree = &mut self.tree;
        let src = &mut self.src;
        let len = src.len();

        if self.active == 0 {
            return None;
        }

        let mut winner = tree[0];
        if let Some(result) = src[winner].next(&mut self.active) {
            let mut parent = (winner + len) / 2;

            while parent != 0 {
                if self.less_than.kmerge_pred(&src[tree[parent]], &src[winner]) {
                    std::mem::swap(&mut tree[parent], &mut winner);
                }
                parent = parent / 2;
            }
            tree[0] = winner;

            if self.active < src.len() / 2 {
                src.retain(|x| x.0.is_some());
                self.active = src.len();
                *tree = vec![0_usize; self.active];

                let idx_to_item = |idx: usize, tree: &Vec<usize>| -> usize {
                    if idx < self.active {
                        tree[idx]
                    } else {
                        idx - self.active
                    }
                };

                for i in (1..self.active).rev() {
                    let (left, right) = (idx_to_item(2 * i, &tree), idx_to_item(2 * i + 1, &tree));
                    tree[i] = if self.less_than.kmerge_pred(&src[left], &src[right]) {
                        left
                    } else {
                        right
                    };
                }
                // Loser tree
                if self.active > 1 {
                    tree[0] = tree[1]; // winner
                }
                for i in 1..self.active {
                    let (left, right) = (idx_to_item(2 * i, &tree), idx_to_item(2 * i + 1, &tree));
                    tree[i] = if self.less_than.kmerge_pred(&src[left], &src[right]) {
                        right
                    } else {
                        left
                    };
                }
            }

            return Some(result);
        }
        None
    }
}

fn main() {
    let mut v = vec![];
    for i in 0..1000 {
        v.push((0..1_000).into_iter());
    }
    v.push((0..1_000_000_000_usize).into_iter());
    //let mut m = itertools::kmerge(v);
    let mut m = kmerge(v);
    for i in m {
        std::hint::black_box(i);
    }
}

#[cfg(test)]
#[test]
fn test_kmerge() {
    let mut v = vec![];
    for i in 0..5 {
        v.push((0..10).into_iter());
    }
    v.push((0..100).into_iter());

    let mut curr = 0;
    let mut c = 0;
    for i in kmerge(v) {
        assert!(curr <= i);
        curr = i;
        c += 1;
    }

    assert_eq!(c, 150);
}
