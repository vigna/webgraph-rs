/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use core::iter;

use lender::{IntoLender, Lend, Lender, Lending};

use crate::prelude::{
    LabeledRandomAccessGraph, LabeledSequentialGraph, LenderIntoIter, LenderIntoIterator,
    LenderLabel, NodeLabelsLender, Pair, RandomAccessGraph, RandomAccessLabeling, SequentialGraph,
    SequentialLabeling,
};

/**

Zips together two labelings.

A wrapper tuple struct that zips together two labelings, and provides
in return a labeling on pairs. It can be used simply to combine labelings
over the same graph, but, more importantly, to attach a labeling to a graph,
obtaining a labeled graph. Depending on the traits implemented by the two
component labelings, the resulting labeling will be [sequential](SequentialLabeling)
or [random-access](RandomAccessLabeling).

Note that the two labelings should be on the same graph: a [`debug_assert!`]
will check if two sequential iterators have the same length and return nodes in the
same order, but no such check is possible for labels as we use [`Iterator::zip`],
which does not perform length checks. For extra safety, consider using
[`Zip::verify`] to perform a complete scan of the two labelings.

*/

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Zip<L: SequentialLabeling, R: SequentialLabeling>(pub L, pub R);

impl<L: SequentialLabeling, R: SequentialLabeling> Zip<L, R> {
    // Performs a complete scan of the content of the two component
    // labelings, returning true if they are compatible, that is,
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
pub struct Iter<L, R>(L, R);

impl<'succ, L, R> NodeLabelsLender<'succ> for Iter<L, R>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    R: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = (LenderLabel<'succ, L>, LenderLabel<'succ, R>);
    type IntoIterator = std::iter::Zip<LenderIntoIter<'succ, L>, LenderIntoIter<'succ, R>>;
}

impl<'succ, L, R> Lending<'succ> for Iter<L, R>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    R: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Lend = (usize, LenderIntoIterator<'succ, Self>);
}

impl<L, R> Lender for Iter<L, R>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    R: Lender + for<'next> NodeLabelsLender<'next>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        let left = self.0.next();
        let right = self.1.next();
        debug_assert_eq!(left.is_none(), right.is_none());
        let left = left?.into_pair();
        let right = right?.into_pair();
        debug_assert_eq!(left.0, right.0);
        Some((left.0, std::iter::zip(left.1, right.1)))
    }
}

impl<'a, L: SequentialLabeling, R: SequentialLabeling> IntoLender for &'a Zip<L, R> {
    type Lender = <Zip<L, R> as SequentialLabeling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: SequentialLabeling, R: SequentialLabeling> SequentialLabeling for Zip<L, R> {
    type Label = (L::Label, R::Label);

    type Iterator<'node> = Iter<L::Iterator<'node>, R::Iterator<'node>>
        where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        debug_assert_eq!(self.0.num_nodes(), self.1.num_nodes());
        self.0.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        Iter(self.0.iter_from(from), self.1.iter_from(from))
    }
}

impl<L: RandomAccessLabeling, R: RandomAccessLabeling> RandomAccessLabeling for Zip<L, R> {
    type Labels<'succ> = std::iter::Zip<
        <<L as RandomAccessLabeling>::Labels<'succ> as IntoIterator>::IntoIter,
        <<R as RandomAccessLabeling>::Labels<'succ> as IntoIterator>::IntoIter>
        where
            <L as RandomAccessLabeling>::Labels<'succ>: IntoIterator<Item = <L as SequentialLabeling>::Label>,
            <R as RandomAccessLabeling>::Labels<'succ>: IntoIterator<Item = <R as SequentialLabeling>::Label>,
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        assert_eq!(self.0.num_arcs(), self.1.num_arcs());
        self.0.num_arcs()
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        iter::zip(self.0.labels(node_id), self.1.labels(node_id))
    }

    fn outdegree(&self, _node_id: usize) -> usize {
        debug_assert_eq!(self.0.outdegree(_node_id), self.1.outdegree(_node_id));
        self.0.outdegree(_node_id)
    }
}

impl<G: SequentialGraph, L: SequentialLabeling> LabeledSequentialGraph<L::Label> for Zip<G, L> {}

impl<G: RandomAccessGraph, L: RandomAccessLabeling> LabeledRandomAccessGraph<L::Label>
    for Zip<G, L>
{
}
