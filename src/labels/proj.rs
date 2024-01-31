/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Left and right projections.

The two structures in this module, [`Left`] and [`Right`], provide
projection of a graph whose labels are pairs. In particular,
`Left(Zip(g,h))` is the same labeling as `g` and
`Right(Zip(g,h))` is the same labeling as `h'.

*/
use lender::{IntoLender, Lend, Lender, Lending};

use crate::prelude::{
    LenderIntoIterator, LenderLabel, NodeLabelsLender, Pair, RandomAccessGraph,
    RandomAccessLabeling, SequentialGraph, SequentialLabeling,
};

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
    type Lender = <Left<S> as SequentialLabeling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<S: SequentialLabeling> SequentialLabeling for Left<S>
where
    S::Label: Pair,
{
    type Label = <S::Label as Pair>::Left;

    type Iterator<'node> = LeftIterator<S::Iterator<'node>>
       where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
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
    type Labels<'succ> = LeftIntoIterator<<R as RandomAccessLabeling>::Labels<'succ>>
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
    type Lender = <Right<S> as SequentialLabeling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<S: SequentialLabeling> SequentialLabeling for Right<S>
where
    S::Label: Pair,
{
    type Label = <S::Label as Pair>::Right;

    type Iterator<'node> = RightIterator<S::Iterator<'node>>
       where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn num_arcs_hint(&self) -> Option<u64> {
        self.0.num_arcs_hint()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        RightIterator(self.0.iter_from(from))
    }
}

impl<R: RandomAccessLabeling> RandomAccessLabeling for Right<R>
where
    R::Label: Pair,
{
    type Labels<'succ> = RightIntoIterator<<R as RandomAccessLabeling>::Labels<'succ>>
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
