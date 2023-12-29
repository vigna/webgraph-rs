/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Left and right projections.

The two structures in this module, [`Left`] and [`Right`], provide
projection of a graph whose labels are pairs. In particular,
`Left(Zip(g,h))` is the same labelling as `g` and
`Right(Zip(g,h))` is the same labelling as `h'.

*/
use lender::{IntoLender, Lend, Lender, Lending};

use crate::{
    prelude::{
        LendingIntoIterator, LendingItem, NodeLabelsLending, RandomAccessGraph,
        RandomAccessLabelling, SequentialGraph, SequentialLabelling,
    },
    Tuple2,
};

// The projection onto the first component of a pair.
#[derive(Clone, Debug)]
pub struct Left<S: SequentialLabelling>(pub S)
where
    S::Label: Tuple2;

#[derive(Clone, Debug)]
pub struct LeftIterator<L>(pub L);

impl<'succ, L> NodeLabelsLending<'succ> for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    for<'next> LendingItem<'next, L>: Tuple2,
{
    type Item = <LendingItem<'succ, L> as Tuple2>::_0;
    type IntoIterator = LeftIntoIterator<<L as NodeLabelsLending<'succ>>::IntoIterator>;
}

impl<'succ, L> Lending<'succ> for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    for<'next> LendingItem<'next, L>: Tuple2,
{
    type Lend = (usize, LendingIntoIterator<'succ, Self>);
}

#[derive(Clone, Debug)]
pub struct LeftIntoIterator<I: IntoIterator>(pub I)
where
    I::Item: Tuple2;

#[derive(Clone, Debug)]
pub struct LeftIntoIter<I: Iterator>(pub I)
where
    I::Item: Tuple2;

impl<I: Iterator> Iterator for LeftIntoIter<I>
where
    I::Item: Tuple2,
{
    type Item = <I::Item as Tuple2>::_0;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|x| x.into_tuple().0)
    }
}

impl<I: IntoIterator> IntoIterator for LeftIntoIterator<I>
where
    I::Item: Tuple2,
{
    type Item = <I::Item as Tuple2>::_0;
    type IntoIter = LeftIntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        LeftIntoIter(self.0.into_iter())
    }
}

impl<L> Lender for LeftIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    for<'next> LendingItem<'next, L>: Tuple2,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.0.next().map(|x| {
            let (node, succ) = x.into_tuple();
            (node, LeftIntoIterator(succ))
        })
    }
}

impl<'a, S: SequentialLabelling> IntoLender for &'a Left<S>
where
    S::Label: Tuple2,
{
    type Lender = <Left<S> as SequentialLabelling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<S: SequentialLabelling> SequentialLabelling for Left<S>
where
    S::Label: Tuple2,
{
    type Label = <S::Label as Tuple2>::_0;

    type Iterator<'node> = LeftIterator<S::Iterator<'node>>
       where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        LeftIterator(self.0.iter_from(from))
    }

    fn num_arcs_hint(&self) -> Option<usize> {
        self.0.num_arcs_hint()
    }
}

impl<R: RandomAccessLabelling> RandomAccessLabelling for Left<R>
where
    R::Label: Tuple2,
{
    type Successors<'succ> = LeftIntoIterator<<R as RandomAccessLabelling>::Successors<'succ>>
    where
        Self: 'succ;

    fn num_arcs(&self) -> usize {
        self.0.num_arcs()
    }

    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Successors<'_> {
        LeftIntoIterator(self.0.successors(node_id))
    }

    fn outdegree(&self, _node_id: usize) -> usize {
        self.0.outdegree(_node_id)
    }
}

impl<S: SequentialLabelling> SequentialGraph for Left<S> where S::Label: Tuple2<_0 = usize> {}

impl<R: RandomAccessLabelling> RandomAccessGraph for Left<R> where R::Label: Tuple2<_0 = usize> {}

// The projection onto the second component of a pair.
#[derive(Clone, Debug)]
pub struct Right<S: SequentialLabelling>(pub S)
where
    S::Label: Tuple2;

#[derive(Clone, Debug)]
pub struct RightIterator<L>(pub L);

impl<'succ, L> NodeLabelsLending<'succ> for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    for<'next> LendingItem<'next, L>: Tuple2,
{
    type Item = <LendingItem<'succ, L> as Tuple2>::_1;
    type IntoIterator = RightIntoIterator<<L as NodeLabelsLending<'succ>>::IntoIterator>;
}

impl<'succ, L> Lending<'succ> for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    for<'next> LendingItem<'next, L>: Tuple2,
{
    type Lend = (usize, LendingIntoIterator<'succ, Self>);
}

#[derive(Clone, Debug)]
pub struct RightIntoIterator<I: IntoIterator>(pub I)
where
    I::Item: Tuple2;

#[derive(Clone, Debug)]
pub struct RightIntoIter<I: Iterator>(pub I)
where
    I::Item: Tuple2;

impl<I: Iterator> Iterator for RightIntoIter<I>
where
    I::Item: Tuple2,
{
    type Item = <I::Item as Tuple2>::_1;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|x| x.into_tuple().1)
    }
}

impl<I: IntoIterator> IntoIterator for RightIntoIterator<I>
where
    I::Item: Tuple2,
{
    type Item = <I::Item as Tuple2>::_1;
    type IntoIter = RightIntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        RightIntoIter(self.0.into_iter())
    }
}

impl<L> Lender for RightIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    for<'next> LendingItem<'next, L>: Tuple2,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.0.next().map(|x| {
            let (node, succ) = x.into_tuple();
            (node, RightIntoIterator(succ))
        })
    }
}

impl<'a, S: SequentialLabelling> IntoLender for &'a Right<S>
where
    S::Label: Tuple2,
{
    type Lender = <Right<S> as SequentialLabelling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<S: SequentialLabelling> SequentialLabelling for Right<S>
where
    S::Label: Tuple2,
{
    type Label = <S::Label as Tuple2>::_1;

    type Iterator<'node> = RightIterator<S::Iterator<'node>>
       where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn num_arcs_hint(&self) -> Option<usize> {
        self.0.num_arcs_hint()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        RightIterator(self.0.iter_from(from))
    }
}

impl<R: RandomAccessLabelling> RandomAccessLabelling for Right<R>
where
    R::Label: Tuple2,
{
    type Successors<'succ> = RightIntoIterator<<R as RandomAccessLabelling>::Successors<'succ>>
    where
        Self: 'succ;

    fn num_arcs(&self) -> usize {
        self.0.num_arcs()
    }

    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Successors<'_> {
        RightIntoIterator(self.0.successors(node_id))
    }

    fn outdegree(&self, _node_id: usize) -> usize {
        self.0.outdegree(_node_id)
    }
}

impl<S: SequentialLabelling> SequentialGraph for Right<S> where S::Label: Tuple2<_1 = usize> {}

impl<R: RandomAccessLabelling> RandomAccessGraph for Right<R> where R::Label: Tuple2<_1 = usize> {}
