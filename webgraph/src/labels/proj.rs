/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Left and right projections.

The two structures in this module, [`Left`] and [`Right`], provide
projection of a labeling whose labels are pairs. In particular,
`Left(Zip(g,h))` is the same labeling as `g` and
`Right(Zip(g,h))` is the same labeling as `h'.

*/
use crate::prelude::{
    LenderIntoIterator, LenderLabel, NodeLabelsLender, Pair, RandomAccessGraph,
    RandomAccessLabeling, SequentialGraph, SequentialLabeling, SortedIterator, SortedLender,
};
use crate::traits::SplitLabeling;
use lender::{ExactSizeLender, IntoLender, Lend, Lender, Lending};

// The projection onto the first component of a pair.
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Left<S: SequentialLabeling>(pub S)
where
    S::Label: Pair;

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct LeftIterator<L>(pub L);

impl<'succ, L> NodeLabelsLender<'succ> for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    type Label = <LenderLabel<'succ, L> as Pair>::Left;
    type IntoIterator = LeftIntoIterator<<L as NodeLabelsLender<'succ>>::IntoIterator>;
}

impl<'succ, L> Lending<'succ> for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    type Lend = (usize, LenderIntoIterator<'succ, Self>);
}

impl<L: ExactSizeLender> ExactSizeLender for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct LeftIntoIterator<I: IntoIterator>(pub I)
where
    I::Item: Pair;

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct LeftIntoIter<I: Iterator>(pub I)
where
    I::Item: Pair;

impl<I: Iterator> Iterator for LeftIntoIter<I>
where
    I::Item: Pair,
{
    type Item = <I::Item as Pair>::Left;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|x| x.into_pair().0)
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for LeftIntoIter<I>
where
    I::Item: Pair,
{
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<I: DoubleEndedIterator> DoubleEndedIterator for LeftIntoIter<I>
where
    I::Item: Pair,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|x| x.into_pair().0)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth_back(n).map(|x| x.into_pair().0)
    }
}

impl<I: IntoIterator> IntoIterator for LeftIntoIterator<I>
where
    I::Item: Pair,
{
    type Item = <I::Item as Pair>::Left;
    type IntoIter = LeftIntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        LeftIntoIter(self.0.into_iter())
    }
}

impl<L> Lender for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.0.next().map(|x| {
            let (node, succ) = x.into_pair();
            (node, LeftIntoIterator(succ))
        })
    }
}

impl<'a, S: SequentialLabeling> IntoLender for &'a Left<S>
where
    S::Label: Pair,
{
    type Lender = <Left<S> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<G> SplitLabeling for Left<G>
where
    G: SequentialLabeling + SplitLabeling,
    G::Label: Pair,
{
    type SplitLender<'a>
        = LeftIterator<G::SplitLender<'a>>
    where
        Self: 'a;
    type IntoIterator<'a>
        = core::iter::Map<
        <G::IntoIterator<'a> as IntoIterator>::IntoIter,
        fn(G::SplitLender<'a>) -> Self::SplitLender<'a>,
    >
    where
        Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        self.0
            .split_iter(how_many)
            .into_iter()
            .map(|lender| LeftIterator(lender))
    }
}

impl<S: SequentialLabeling> SequentialLabeling for Left<S>
where
    S::Label: Pair,
{
    type Label = <S::Label as Pair>::Left;

    type Lender<'node>
        = LeftIterator<S::Lender<'node>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        LeftIterator(self.0.iter_from(from))
    }

    fn num_arcs_hint(&self) -> Option<u64> {
        self.0.num_arcs_hint()
    }
}

impl<R: RandomAccessLabeling> RandomAccessLabeling for Left<R>
where
    R::Label: Pair,
{
    type Labels<'succ>
        = LeftIntoIterator<<R as RandomAccessLabeling>::Labels<'succ>>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        LeftIntoIterator(self.0.labels(node_id))
    }

    fn outdegree(&self, _node_id: usize) -> usize {
        self.0.outdegree(_node_id)
    }
}

impl<S: SequentialLabeling> SequentialGraph for Left<S> where S::Label: Pair<Left = usize> {}

impl<R: RandomAccessLabeling> RandomAccessGraph for Left<R> where R::Label: Pair<Left = usize> {}

unsafe impl<L: SortedLender> SortedLender for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
}

unsafe impl<I: SortedIterator> SortedIterator for LeftIntoIter<I> where I::Item: Pair {}

// The projection onto the second component of a pair.
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Right<S: SequentialLabeling>(pub S)
where
    S::Label: Pair;

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct RightIterator<L>(pub L);

impl<'succ, L> NodeLabelsLender<'succ> for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    type Label = <LenderLabel<'succ, L> as Pair>::Right;
    type IntoIterator = RightIntoIterator<<L as NodeLabelsLender<'succ>>::IntoIterator>;
}

impl<'succ, L> Lending<'succ> for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    type Lend = (usize, LenderIntoIterator<'succ, Self>);
}

impl<L: ExactSizeLender> ExactSizeLender for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct RightIntoIterator<I: IntoIterator>(pub I)
where
    I::Item: Pair;

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct RightIntoIter<I: Iterator>(pub I)
where
    I::Item: Pair;

impl<I: Iterator> Iterator for RightIntoIter<I>
where
    I::Item: Pair,
{
    type Item = <I::Item as Pair>::Right;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|x| x.into_pair().1)
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for RightIntoIter<I>
where
    I::Item: Pair,
{
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<I: DoubleEndedIterator> DoubleEndedIterator for RightIntoIter<I>
where
    I::Item: Pair,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|x| x.into_pair().1)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth_back(n).map(|x| x.into_pair().1)
    }
}

impl<I: IntoIterator> IntoIterator for RightIntoIterator<I>
where
    I::Item: Pair,
{
    type Item = <I::Item as Pair>::Right;
    type IntoIter = RightIntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        RightIntoIter(self.0.into_iter())
    }
}

impl<L> Lender for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.0.next().map(|x| {
            let (node, succ) = x.into_pair();
            (node, RightIntoIterator(succ))
        })
    }
}

impl<'a, S: SequentialLabeling> IntoLender for &'a Right<S>
where
    S::Label: Pair,
{
    type Lender = <Right<S> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<G> SplitLabeling for Right<G>
where
    G: SequentialLabeling + SplitLabeling,
    G::Label: Pair,
{
    type SplitLender<'a>
        = RightIterator<G::SplitLender<'a>>
    where
        Self: 'a;
    type IntoIterator<'a>
        = core::iter::Map<
        <G::IntoIterator<'a> as IntoIterator>::IntoIter,
        fn(G::SplitLender<'a>) -> Self::SplitLender<'a>,
    >
    where
        Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        self.0
            .split_iter(how_many)
            .into_iter()
            .map(|lender| RightIterator(lender))
    }
}

impl<S: SequentialLabeling> SequentialLabeling for Right<S>
where
    S::Label: Pair,
{
    type Label = <S::Label as Pair>::Right;

    type Lender<'node>
        = RightIterator<S::Lender<'node>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn num_arcs_hint(&self) -> Option<u64> {
        self.0.num_arcs_hint()
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        RightIterator(self.0.iter_from(from))
    }
}

impl<R: RandomAccessLabeling> RandomAccessLabeling for Right<R>
where
    R::Label: Pair,
{
    type Labels<'succ>
        = RightIntoIterator<<R as RandomAccessLabeling>::Labels<'succ>>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        RightIntoIterator(self.0.labels(node_id))
    }

    fn outdegree(&self, _node_id: usize) -> usize {
        self.0.outdegree(_node_id)
    }
}

impl<S: SequentialLabeling> SequentialGraph for Right<S> where S::Label: Pair<Right = usize> {}

impl<R: RandomAccessLabeling> RandomAccessGraph for Right<R> where R::Label: Pair<Right = usize> {}

unsafe impl<L: SortedLender> SortedLender for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    for<'next> LenderLabel<'next, L>: Pair,
{
}

unsafe impl<I: SortedIterator> SortedIterator for RightIntoIter<I> where I::Item: Pair {}
