/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Zipping (cartesian product) of labelings.


use lender::{IntoLender, Lend, Lender, Lending, unsafe_assume_covariance};

use crate::prelude::{
    LabeledRandomAccessGraph, LabeledSequentialGraph, LenderIntoIter, LenderIntoIterator,
    LenderLabel, NodeLabelsLender, Pair, RandomAccessGraph, RandomAccessLabeling, SequentialGraph,
    SequentialLabeling, SortedIterator, SortedLender,
};

/// Zips together two labelings.
///
/// A wrapper tuple struct that zips together two labelings, and provides
/// in return a labeling on pairs. It can be used simply to combine labelings
/// over the same graph, but, more importantly, to attach a labeling to a graph,
/// obtaining a labeled graph. Depending on the traits implemented by the two
/// component labelings, the resulting labeling will be [sequential] or
/// [random-access].
///
/// Note that the two labelings should be on the same graph: iteration will
/// panic if the two sequential iterators have different lengths, return nodes
/// in a different order, or return a different number of labels for a node
/// (labels are checked lazily, while they are enumerated). For an eager
/// check, consider using [`Zip::verify`], which performs a complete scan of
/// the two labelings.
///
/// See also [`Left`] and [`Right`] for projecting a zipped labeling back to
/// one of its components.
///
/// [sequential]: SequentialLabeling
/// [random-access]: RandomAccessLabeling
/// [`Left`]: super::Left
/// [`Right`]: super::Right

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Zip<L: SequentialLabeling, R: SequentialLabeling>(pub L, pub R);

impl<L: SequentialLabeling, R: SequentialLabeling> Zip<L, R> {
    /// Performs a complete scan of the content of the two component labelings,
    /// returning true if they are compatible, that is, their iterators have the
    /// same length and return nodes in the same order, and the two iterators
    /// paired to each node return the same number of elements.
    pub fn verify(&self) -> bool {
        let mut iter0 = self.0.iter();
        let mut iter1 = self.1.iter();
        loop {
            match (iter0.next(), iter1.next()) {
                (None, None) => return true,
                (Some((x0, i0)), Some((x1, i1))) => {
                    if x0 != x1 {
                        return false;
                    }

                    let mut i0 = i0.into_iter();
                    let mut i1 = i1.into_iter();
                    loop {
                        match (i0.next(), i1.next()) {
                            (None, None) => break,
                            (Some(_), Some(_)) => continue,
                            _ => return false,
                        }
                    }
                }
                _ => return false,
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
#[doc(hidden)]
pub struct NodeLabels<L, R>(L, R);

impl<'succ, L, R> NodeLabelsLender<'succ> for NodeLabels<L, R>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    R: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = (LenderLabel<'succ, L>, LenderLabel<'succ, R>);
    type IntoIterator = StrictZip<LenderIntoIter<'succ, L>, LenderIntoIter<'succ, R>>;
}

impl<'succ, L, R> Lending<'succ> for NodeLabels<L, R>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    R: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Lend = (usize, LenderIntoIterator<'succ, Self>);
}

impl<L, R> Lender for NodeLabels<L, R>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    R: Lender + for<'next> NodeLabelsLender<'next>,
{
    // SAFETY: the lend is covariant as it zips iterators from covariant lenders L and R.
    unsafe_assume_covariance!();

    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        let left = self.0.next();
        let right = self.1.next();
        assert_eq!(
            left.is_none(),
            right.is_none(),
            "the zipped labelings have different lengths"
        );
        let left = left?.into_pair();
        let right = right?.into_pair();
        assert_eq!(
            left.0, right.0,
            "the zipped labelings returned different nodes"
        );
        Some((left.0, StrictZip(left.1.into_iter(), right.1.into_iter())))
    }
}

impl<'a, L: SequentialLabeling, R: SequentialLabeling> IntoLender for &'a Zip<L, R> {
    type Lender = <Zip<L, R> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: SequentialLabeling, R: SequentialLabeling> SequentialLabeling for Zip<L, R> {
    type Label = (L::Label, R::Label);

    type Lender<'node>
        = NodeLabels<L::Lender<'node>, R::Lender<'node>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        // Here as assert_eq! as the cost is very low
        assert_eq!(self.0.num_nodes(), self.1.num_nodes());
        self.0.num_nodes()
    }

    fn num_arcs_hint(&self) -> Option<u64> {
        match (self.0.num_arcs_hint(), self.1.num_arcs_hint()) {
            (Some(a), Some(b)) => {
                assert_eq!(a, b, "the zipped labelings have different arc counts");
                Some(a)
            }
            (a, b) => a.or(b),
        }
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        NodeLabels(self.0.iter_from(from), self.1.iter_from(from))
    }
}

impl<L: RandomAccessLabeling, R: RandomAccessLabeling> RandomAccessLabeling for Zip<L, R> {
    type Labels<'succ>
        = StrictZip<
        <<L as RandomAccessLabeling>::Labels<'succ> as IntoIterator>::IntoIter,
        <<R as RandomAccessLabeling>::Labels<'succ> as IntoIterator>::IntoIter,
    >
    where
        <L as RandomAccessLabeling>::Labels<'succ>:
            IntoIterator<Item = <L as SequentialLabeling>::Label>,
        <R as RandomAccessLabeling>::Labels<'succ>:
            IntoIterator<Item = <R as SequentialLabeling>::Label>,
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        // Here as assert_eq! as the cost is very low
        assert_eq!(self.0.num_arcs(), self.1.num_arcs());
        self.0.num_arcs()
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        StrictZip(
            self.0.labels(node_id).into_iter(),
            self.1.labels(node_id).into_iter(),
        )
    }

    fn outdegree(&self, _node_id: usize) -> usize {
        // Here we just debug_assert_eq! because it would be too onerous in release mode
        debug_assert_eq!(self.0.outdegree(_node_id), self.1.outdegree(_node_id));
        self.0.outdegree(_node_id)
    }
}

impl<G: SequentialGraph, L: SequentialLabeling> LabeledSequentialGraph<L::Label> for Zip<G, L> {}

impl<G: RandomAccessGraph, L: RandomAccessLabeling> LabeledRandomAccessGraph<L::Label>
    for Zip<G, L>
{
}

// SAFETY: both underlying lenders are sorted, and zipping preserves order.
unsafe impl<
    L: SortedLender + for<'next> NodeLabelsLender<'next>,
    R: SortedLender + for<'next> NodeLabelsLender<'next>,
> SortedLender for NodeLabels<L, R>
{
}

/// Zips two iterators, panicking if they have different lengths.
///
/// Contrarily to [`Iterator::zip`], when exactly one of the two iterators is
/// exhausted this iterator panics instead of stopping, so zipped labelings
/// with mismatched label counts fail fast instead of being silently
/// truncated. The check adds no measurable cost, as [`Iterator::zip`] already
/// examines both iterators.
#[derive(Clone, Debug)]
pub struct StrictZip<I, J>(I, J);

impl<I: Iterator, J: Iterator> Iterator for StrictZip<I, J> {
    type Item = (I::Item, J::Item);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match (self.0.next(), self.1.next()) {
            (Some(a), Some(b)) => Some((a, b)),
            (None, None) => None,
            (a, b) => panic!(
                "the zipped labelings returned a different number of labels for a node (left {}, right {})",
                if a.is_some() { "still has labels" } else { "is exhausted" },
                if b.is_some() { "still has labels" } else { "is exhausted" },
            ),
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // Items yielded before a mismatch panics are exactly those of
        // Iterator::zip, so the standard zip bounds apply.
        let (l_low, l_high) = self.0.size_hint();
        let (r_low, r_high) = self.1.size_hint();
        let high = match (l_high, r_high) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (h, None) | (None, h) => h,
        };
        (l_low.min(r_low), high)
    }
}

impl<I: ExactSizeIterator, J: ExactSizeIterator> ExactSizeIterator for StrictZip<I, J> {
    #[inline(always)]
    fn len(&self) -> usize {
        let len = self.0.len();
        assert_eq!(
            len,
            self.1.len(),
            "the zipped labelings returned a different number of labels for a node"
        );
        len
    }
}

impl<I: core::iter::FusedIterator, J: core::iter::FusedIterator> core::iter::FusedIterator
    for StrictZip<I, J>
{
}

// SAFETY: both underlying iterators enumerate in the order induced by
// ascending successors, and zipping pairs them positionally without
// reordering, so the pairs are enumerated in the same order.
unsafe impl<I: SortedIterator, J: SortedIterator> SortedIterator for StrictZip<I, J> {}
