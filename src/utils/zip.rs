/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use core::iter;

use lender::{Lend, Lender, Lending, IntoLender};

use crate::{prelude::{SequentialLabelling, RandomAccessLabelling, LabelledSequentialGraph, SequentialGraph, LabelledRandomAccessGraph, RandomAccessGraph}, Tuple2};

/// Zips two labelling

pub struct Zip<L: SequentialLabelling, R: SequentialLabelling>(pub L, pub R);

pub struct ZippedGraphIterator<L, R>(L, R);

impl<'succ, L, R> Lending<'succ> for ZippedGraphIterator<L, R>
where
    L: Lender,
    R: Lender,
    for<'next> Lend<'next, L>: Tuple2<_0 = usize>,
    for<'next> <Lend<'next, L> as Tuple2>::_1: IntoIterator,
    for<'next> Lend<'next, R>: Tuple2<_0 = usize>,
    for<'next> <Lend<'next, R> as Tuple2>::_1: IntoIterator,
{
    type Lend = (
        usize,
        std::iter::Zip<
            <<Lend<'succ, L> as Tuple2>::_1 as IntoIterator>::IntoIter,
            <<Lend<'succ, R> as Tuple2>::_1 as IntoIterator>::IntoIter,
        >,
    );
}

impl<'succ, L, R> Lender for ZippedGraphIterator<L, R>
where
    L: Lender,
    R: Lender,
    for<'next> Lend<'next, L>: Tuple2<_0 = usize>,
    for<'next> <Lend<'next, L> as Tuple2>::_1: IntoIterator,
    for<'next> Lend<'next, R>: Tuple2<_0 = usize>,
    for<'next> <Lend<'next, R> as Tuple2>::_1: IntoIterator,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        let left = self.0.next()?.into_tuple();
        let right = self.1.next()?.into_tuple();
        debug_assert_eq!(left.0, right.0);
        Some((
            left.0,
            std::iter::zip(left.1.into_iter(), right.1.into_iter()),
        ))
    }
}

impl<'a, L: SequentialLabelling, R: SequentialLabelling> IntoLender for &'a Zip<L, R> {
    type Lender = <Zip<L, R> as SequentialLabelling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}


impl<L: SequentialLabelling, R: SequentialLabelling> SequentialLabelling for Zip<L, R> {
    type Value = (L::Value, R::Value);

    type Successors<'succ> = std::iter::Zip<
        <L::Successors<'succ> as IntoIterator>::IntoIter,
        <R::Successors<'succ> as IntoIterator>::IntoIter,
    >;

    type Iterator<'node> = ZippedGraphIterator<L::Iterator<'node>, R::Iterator<'node>> 
       where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        debug_assert_eq!(self.0.num_nodes(), self.1.num_nodes());
        self.0.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        ZippedGraphIterator (
            self.0.iter_from(from),
            self.1.iter_from(from),
        )
    }
}

impl<L: RandomAccessLabelling, R: RandomAccessLabelling> RandomAccessLabelling for Zip<L, R>
        {
    type Successors<'succ> = std::iter::Zip<
        <<L as RandomAccessLabelling>::Successors<'succ> as IntoIterator>::IntoIter, 
        <<R as RandomAccessLabelling>::Successors<'succ> as IntoIterator>::IntoIter>
        where
            <L as RandomAccessLabelling>::Successors<'succ>: IntoIterator<Item = <L as SequentialLabelling>::Value>,
            <R as RandomAccessLabelling>::Successors<'succ>: IntoIterator<Item = <R as SequentialLabelling>::Value>,
        Self: 'succ;

    fn num_arcs(&self) -> usize {
        assert_eq!(self.0.num_arcs(), self.1.num_arcs());
        self.0.num_arcs()
    }

    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Successors<'_> {
        iter::zip(self.0.successors(node_id), self.1.successors(node_id))
    }

    fn outdegree(&self, _node_id: usize) -> usize {
        debug_assert_eq!(self.0.outdegree(_node_id), self.1.outdegree(_node_id));
        self.0.outdegree(_node_id)
    }
}

impl<G: SequentialGraph, L: SequentialLabelling> LabelledSequentialGraph<L::Value> for Zip<G, L> {}

impl<G: RandomAccessGraph, L: RandomAccessLabelling> LabelledRandomAccessGraph<L::Value> for Zip<G, L> {}
