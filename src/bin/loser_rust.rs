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
pub struct HeadTail<I>
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
}

/// An iterator adaptor that merges an abitrary number of base iterators in ascending order.
/// If all base iterators are sorted (ascending), the result is sorted.
///
/// Iterator element type is `I::Item`.
///
/// See [`.kmerge()`](crate::Itertools::kmerge) for more information.
pub type KMerge<I> = KMergeBy<I, KMergeByLt>;

pub trait KMergePredicate<T: Iterator> {
    fn kmerge_pred(
        &mut self,
        a: &Option<HeadTail<T>>,
        i: usize,
        b: &Option<HeadTail<T>>,
        j: usize,
    ) -> bool;
}

#[derive(Clone, Debug)]
pub struct KMergeByLt;

impl<T: Iterator> KMergePredicate<T> for KMergeByLt
where
    T::Item: PartialOrd,
{
    fn kmerge_pred(
        &mut self,
        a: &Option<HeadTail<T>>,
        i: usize,
        b: &Option<HeadTail<T>>,
        j: usize,
    ) -> bool {
        if let Some(a) = &a {
            if let Some(b) = &b {
                return a.head < b.head || a.head == b.head && i < j;
            } else {
                return true;
            }
        }
        // We do not need to make stable the case where both are None
        false
    }
}

impl<T: Iterator, F: FnMut(&T, &T) -> bool> KMergePredicate<T> for F
where
    T::Item: PartialOrd,
{
    fn kmerge_pred(
        &mut self,
        a: &Option<HeadTail<T>>,
        i: usize,
        b: &Option<HeadTail<T>>,
        j: usize,
    ) -> bool {
        if let Some(a) = &a {
            if let Some(b) = &b {
                return true; //self(&a.head, &b.head) || a.head == b.head && i < j;
            } else {
                return true;
            }
        }
        // We do not need to make stable the case where both are None
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
    src: Vec<Option<HeadTail<I>>>,
    tree: Vec<usize>,
    active: usize,
    less_than: F,
}

impl<I, F> KMergeBy<I, F>
where
    I: Iterator,
    F: KMergePredicate<I>,
{
    fn build_tree(src: &Vec<Option<HeadTail<I>>>, less_than: &mut F) -> Vec<usize>
    where
        I: Iterator,
        F: KMergePredicate<I>,
    {
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
            tree[i] = if less_than.kmerge_pred(&src[left], left, &src[right], right) {
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
            tree[i] = if less_than.kmerge_pred(&src[left], left, &src[right], right) {
                right
            } else {
                left
            };
        }

        tree
    }

    fn fix_tree(&mut self, winner: usize) {
        let mut winner = winner;
        let mut parent = (winner + self.tree.len()) / 2;
        while parent != 0 {
            if self.less_than.kmerge_pred(
                &self.src[self.tree[parent]],
                self.tree[parent],
                &self.src[winner],
                winner,
            ) {
                std::mem::swap(&mut self.tree[parent], &mut winner);
            }
            parent = parent / 2;
        }
        self.tree[0] = winner;
    }
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
        .filter_map(|it| HeadTail::new(it.into_iter()))
        .map(Some)
        .collect();
    let active = src.len();
    let tree = KMergeBy::build_tree(&src, &mut less_than);

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
        if self.active == 0 {
            return None;
        }

        let winner = self.tree[0];
        let head_tail = self.src[winner].as_mut().expect("winner is None");

        let result;
        if let Some(next) = head_tail.tail.next() {
            result = replace(&mut head_tail.head, next);
            self.fix_tree(winner);
        } else {
            result = replace(&mut self.src[winner], None)
                .map(|ht| ht.head)
                .unwrap();
            self.fix_tree(winner);

            self.active -= 1;
            if self.active < self.src.len() / 2 {
                self.src.retain(Option::is_some);
                debug_assert_eq!(self.src.len(), self.active);
                self.tree = KMergeBy::build_tree(&mut self.src, &mut self.less_than);
            }
        };

        Some(result)
    }
}

fn main() {
    let mut v = vec![];
    for i in 0..7 {
        v.push(((i / 2) * 100_000_000..((i + 1) / 2 + 1) * 100_000_000).into_iter());
    }
    //let m = itertools::kmerge(v);
    let m = kmerge(v);
    for i in m {
        std::hint::black_box(i);
    }
}

#[cfg(test)]
#[test]
fn test_kmerge() {
    let mut v = vec![];
    for i in 0..3 {
        v.push((0..10).into_iter());
    }
    v.push((5..20).into_iter());
    v.push((50..60).into_iter());

    let mut curr = 0;
    let mut c = 0;
    for i in kmerge(v) {
        assert!(curr <= i);
        curr = i;
        c += 1;
    }

    assert_eq!(c, 55);
}

#[test]
fn test_stability() {
    let mut v: Vec<Vec<(usize, usize)>> = vec![];
    for i in 0..3_usize {
        v.push((0..100_usize).into_iter().map(|x| (x, i)).collect());
    }

    let mut curr = (0, 0);
    let mut c = 0;
    for i in kmerge(v) {
        assert!(curr <= i);
        curr = i;
        c += 1;
    }

    assert_eq!(c, 300);
}
