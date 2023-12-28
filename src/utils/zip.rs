/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use core::iter;

use lender::{Lend, Lender, Lending, IntoLender};

use crate::{prelude::{SequentialLabelling, RandomAccessLabelling, LabelledSequentialGraph, SequentialGraph, LabelledRandomAccessGraph, RandomAccessGraph, NodeLabelsLending, LendingItem, LendingIntoIterator, LendingIntoIter}, Tuple2};

/**

Zips together two labellings.

A wrapper tuple struct that zips together two labellings, and provides
in return a labelling on pairs. It can be used simply to combine labellings
over the same graph, but, more importantly, to attach a labelling to a graph,
obtaining a labelled graph. Depending on the traits implemented by the two 
component labellings, the resulting labelling will be [sequential](SequentialLabelling)
or [random-access](RandomAccessLabelling).

Note that the two labellings should be on the same graph: a [`debug_assert!`]
will check if two sequential iterators have the same length and return nodes in the
same order, but no such check is possible for labels as we use [`Iterator::zip`],
which does not perform length checks. For extra safety, consider using
[`Zip::verify`] to perform a complete scan of the two labellings.

*/

pub struct Zip<L: SequentialLabelling, R: SequentialLabelling>(pub L, pub R);

impl<L: SequentialLabelling, R: SequentialLabelling> Zip<L, R> {
    // Performs a complete scan of the content of the two component
    // labellings, returning true if they are compatible, that is,
    // their iterators have the same length and return nodes in the
    // same order, and the two iterators paired to each node return
    // the same number of elements.
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
                    loop{ 
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

pub struct ZippedGraphIterator<L, R> (L, R);

impl<'succ, L, R> NodeLabelsLending<'succ> for ZippedGraphIterator<L, R>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    R: Lender + for<'next> NodeLabelsLending<'next>,
{
    type Item = (LendingItem<'succ, L>, LendingItem<'succ, R>);
    type IntoIterator = std::iter::Zip<LendingIntoIter<'succ, L>, LendingIntoIter<'succ, R>>;

}

impl<'succ, L, R> Lending<'succ> for ZippedGraphIterator<L, R>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    R: Lender + for<'next> NodeLabelsLending<'next>,
{
    type Lend = (
        usize,
        LendingIntoIterator<'succ, Self>
    );
}

impl<L, R> Lender for ZippedGraphIterator<L, R>
where
    L: Lender + for<'next> NodeLabelsLending<'next>,
    R: Lender + for<'next> NodeLabelsLending<'next>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        let left = self.0.next();
        let right = self.1.next();
        debug_assert_eq!(left.is_none(), right.is_none());
        let left = left?.into_tuple();
        let right = right?.into_tuple();
        debug_assert_eq!(left.0, right.0);
        Some((
            left.0,
            std::iter::zip(left.1, right.1),
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
    type Label = (L::Label, R::Label);


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
            <L as RandomAccessLabelling>::Successors<'succ>: IntoIterator<Item = <L as SequentialLabelling>::Label>,
            <R as RandomAccessLabelling>::Successors<'succ>: IntoIterator<Item = <R as SequentialLabelling>::Label>,
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

impl<G: SequentialGraph, L: SequentialLabelling> LabelledSequentialGraph<L::Label> for Zip<G, L> {}

impl<G: RandomAccessGraph, L: RandomAccessLabelling> LabelledRandomAccessGraph<L::Label> for Zip<G, L> {}
