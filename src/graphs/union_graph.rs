/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use lender::*;

#[derive(Debug, Clone)]
/// A wrapper exhibiting the union of two graphs.
pub struct UnionGraph<G: SequentialGraph, H: SequentialGraph>(pub G, pub H);

impl<G: SequentialGraph, H: SequentialGraph> SequentialLabeling for UnionGraph<G, H>
where
    for<'a> G::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
    for<'a> H::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'b, H::Lender<'a>>: SortedIterator,
{
    type Label = usize;
    type Lender<'b> = Iter<G::Lender<'b>, H::Lender<'b>>
        where
            Self: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes().max(self.1.num_nodes())
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        None
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        Iter(
            self.0.iter_from(from.min(self.0.num_nodes())),
            self.1.iter_from(from.min(self.1.num_nodes())),
        )
    }
}

impl<G: SequentialGraph, H: SequentialGraph> SplitLabeling for UnionGraph<G, H>
where
    for<'a> G::Lender<'a>: SortedLender + Clone + ExactSizeLender + Send + Sync,
    for<'a, 'b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
    for<'a> H::Lender<'a>: SortedLender + Clone + ExactSizeLender + Send + Sync,
    for<'a, 'b> LenderIntoIter<'b, H::Lender<'a>>: SortedIterator,
{
    type SplitLender<'a> = split::seq::Lender<'a, UnionGraph<G, H> > where Self: 'a;
    type IntoIterator<'a> = split::seq::IntoIterator<'a, UnionGraph<G, H>> where Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::seq::Iter::new(self.iter(), how_many)
    }
}

impl<G: SequentialGraph, H: SequentialGraph> SequentialGraph for UnionGraph<G, H>
where
    for<'a> G::Lender<'a>: SortedLender + Clone + ExactSizeLender + Send + Sync,
    for<'a, 'b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
    for<'a> H::Lender<'a>: SortedLender + Clone + ExactSizeLender + Send + Sync,
    for<'a, 'b> LenderIntoIter<'b, H::Lender<'a>>: SortedIterator,
{
}

impl<'c, G: SequentialGraph, H: SequentialGraph> IntoLender for &'c UnionGraph<G, H>
where
    for<'a> G::Lender<'a>: SortedLender + Clone + ExactSizeLender + Send + Sync,
    for<'a, 'b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
    for<'a> H::Lender<'a>: SortedLender + Clone + ExactSizeLender + Send + Sync,
    for<'a, 'b> LenderIntoIter<'b, H::Lender<'a>>: SortedIterator,
{
    type Lender = <UnionGraph<G, H> as SequentialLabeling>::Lender<'c>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Iter<L, M>(L, M);

impl<
        'succ,
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
        M: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
    > NodeLabelsLender<'succ> for Iter<L, M>
{
    type Label = usize;
    type IntoIterator = Succ<LenderIntoIter<'succ, L>, LenderIntoIter<'succ, M>>;
}

impl<
        'succ,
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
        M: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
    > Lending<'succ> for Iter<L, M>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
        M: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
    > Lender for Iter<L, M>
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        let (node0, iter0) = self.0.next().unzip();
        let (node1, iter1) = self.1.next().unzip();
        Some((
            node0.or(node1)?,
            Succ::new(
                iter0.map(IntoIterator::into_iter),
                iter1.map(IntoIterator::into_iter),
            ),
        ))
    }
}

impl<
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize> + ExactSizeLender,
        M: Lender + for<'next> NodeLabelsLender<'next, Label = usize> + ExactSizeLender,
    > ExactSizeLender for Iter<L, M>
{
    fn len(&self) -> usize {
        self.0.len().max(self.1.len())
    }
}

unsafe impl<
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize> + SortedLender,
        M: Lender + for<'next> NodeLabelsLender<'next, Label = usize> + SortedLender,
    > SortedLender for Iter<L, M>
{
}

#[derive(Debug, Clone)]
pub struct Succ<I: Iterator<Item = usize>, J: Iterator<Item = usize>> {
    iter0: Option<core::iter::Peekable<I>>,
    iter1: Option<core::iter::Peekable<J>>,
}

impl<I: Iterator<Item = usize>, J: Iterator<Item = usize>> Succ<I, J> {
    pub fn new(iter0: Option<I>, iter1: Option<J>) -> Self {
        Self {
            iter0: iter0.map(Iterator::peekable),
            iter1: iter1.map(Iterator::peekable),
        }
    }
}
impl<I: Iterator<Item = usize>, J: Iterator<Item = usize>> Iterator for Succ<I, J> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let next0 = self.iter0.as_mut().and_then(|iter| iter.peek().copied());
        let next1 = self.iter1.as_mut().and_then(|iter| iter.peek().copied());
        match next0
            .unwrap_or(usize::MAX)
            .cmp(&next1.unwrap_or(usize::MAX))
        {
            std::cmp::Ordering::Greater => self.iter1.as_mut().and_then(Iterator::next),
            std::cmp::Ordering::Less => self.iter0.as_mut().and_then(Iterator::next),
            std::cmp::Ordering::Equal => {
                self.iter0.as_mut().and_then(Iterator::next);
                self.iter1.as_mut().and_then(Iterator::next)
            }
        }
    }
}

// TODO
unsafe impl<I: Iterator<Item = usize> + SortedIterator, J: Iterator<Item = usize> + SortedIterator>
    SortedIterator for Succ<I, J>
{
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;

    #[test]
    fn test_union_graph() -> anyhow::Result<()> {
        use crate::{graphs::vec_graph::VecGraph, prelude::proj::Left};
        let g = [
            Left(VecGraph::from_arc_list([
                (0, 1),
                (0, 3),
                (1, 2),
                (2, 0),
                (2, 2),
                (2, 4),
                (3, 4),
                (3, 5),
                (4, 1),
            ])),
            Left(VecGraph::from_arc_list([
                (1, 2),
                (1, 3),
                (2, 1),
                (2, 3),
                (2, 3),
                (4, 0),
                (5, 1),
                (5, 2),
                (6, 6),
            ])),
        ];
        for i in 0..2 {
            // TODO: why borrowing doesn't work? I should be able to do
            // let union = UnionGraph(&g[i], &g[1 - i]);
            let union = UnionGraph(g[i].clone(), g[1 - i].clone());
            assert_eq!(union.num_nodes(), 7);

            let mut iter = union.iter();
            let Some((x, s)) = iter.next() else { panic!() };
            assert_eq!(x, 0);
            assert_eq!(s.collect::<Vec<_>>(), vec![1, 3]);
            let Some((x, s)) = iter.next() else { panic!() };
            assert_eq!(x, 1);
            assert_eq!(s.collect::<Vec<_>>(), vec![2, 3]);
            let Some((x, s)) = iter.next() else { panic!() };
            assert_eq!(x, 2);
            assert_eq!(s.collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
            let Some((x, s)) = iter.next() else { panic!() };
            assert_eq!(x, 3);
            assert_eq!(s.collect::<Vec<_>>(), vec![4, 5]);
            let Some((x, s)) = iter.next() else { panic!() };
            assert_eq!(x, 4);
            assert_eq!(s.collect::<Vec<_>>(), vec![0, 1]);
            let Some((x, s)) = iter.next() else { panic!() };
            assert_eq!(x, 5);
            assert_eq!(s.collect::<Vec<_>>(), vec![1, 2]);
            let Some((x, s)) = iter.next() else { panic!() };
            assert_eq!(x, 6);
            assert_eq!(s.collect::<Vec<_>>(), vec![6]);
            assert!(iter.next().is_none());
        }
        Ok(())
    }
}
