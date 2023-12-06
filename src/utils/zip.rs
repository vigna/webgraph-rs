/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use core::marker::PhantomData;

use lender::{Lend, Lender, Lending, IntoLender};

use crate::{prelude::SequentialLabelling, Tuple2};

/// Zips two labelling

pub struct Zip<L: SequentialLabelling, R: SequentialLabelling> {
    left: L,
    right: R,
}

impl<L: SequentialLabelling, R: SequentialLabelling> Zip<L, R> {
    pub fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

pub struct ZippedGraphIterator<L, R> {
    left: L,
    right: R,
}

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
        let left = self.left.next()?.into_tuple();
        let right = self.right.next()?.into_tuple();

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
        debug_assert_eq!(self.left.num_nodes(), self.right.num_nodes());
        self.left.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        ZippedGraphIterator {
            left: self.left.iter_from(from),
            right: self.right.iter_from(from),
        }
    }
}
