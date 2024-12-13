/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use lender::*;

#[derive(Debug, Clone)]
/// A wrapper that removes self-loops from a graph. Since we don't know how many
/// self-loops there are, we can't provide an exact number of arcs or outdegree
/// for each node. Therefore, we can't implement random access to the successors.
pub struct NoSelfLoopsGraph<G>(pub G);

impl<G: SequentialGraph> SequentialLabeling for NoSelfLoopsGraph<G> {
    type Label = usize;
    type Lender<'b>
        = Iter<G::Lender<'b>>
    where
        Self: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        // it's just a hint, and we don't know how many self-loops there are
        self.0.num_arcs_hint()
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        Iter {
            iter: self.0.iter_from(from),
        }
    }
}

impl<G: SequentialGraph + SplitLabeling> SplitLabeling for NoSelfLoopsGraph<G>
where
    for<'a> <G as SequentialLabeling>::Lender<'a>: Clone + Send + Sync,
{
    type SplitLender<'a>
        = split::seq::Lender<'a, NoSelfLoopsGraph<G>>
    where
        Self: 'a;
    type IntoIterator<'a>
        = split::seq::IntoIterator<'a, NoSelfLoopsGraph<G>>
    where
        Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::seq::Iter::new(self.iter(), self.num_nodes(), how_many)
    }
}

impl<G: SequentialGraph> SequentialGraph for NoSelfLoopsGraph<G> {}

impl<'b, G: SequentialGraph> IntoLender for &'b NoSelfLoopsGraph<G> {
    type Lender = <NoSelfLoopsGraph<G> as SequentialLabeling>::Lender<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

/// An iterator over the nodes of a graph that applies on the fly a permutation of the nodes.
#[derive(Debug, Clone)]
pub struct Iter<I> {
    iter: I,
}

impl<'succ, I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>> NodeLabelsLender<'succ>
    for Iter<I>
{
    type Label = usize;
    type IntoIterator = Succ<LenderIntoIter<'succ, I>>;
}

impl<'succ, I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>> Lending<'succ>
    for Iter<I>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

unsafe impl<I: SortedLender + Lender + for<'next> NodeLabelsLender<'next, Label = usize>>
    SortedLender for Iter<I>
{
}

impl<L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>> Lender for Iter<L> {
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.iter.next().map(|x| {
            let (node, succ) = x.into_pair();
            (
                node,
                Succ {
                    src: node,
                    iter: succ.into_iter(),
                },
            )
        })
    }
}

impl<L: ExactSizeLender + for<'next> NodeLabelsLender<'next, Label = usize>> ExactSizeLender
    for Iter<L>
{
    fn len(&self) -> usize {
        self.iter.len()
    }
}

#[derive(Debug, Clone)]
pub struct Succ<I: Iterator<Item = usize>> {
    src: usize,
    iter: I,
}

impl<I: Iterator<Item = usize>> Iterator for Succ<I> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let dst = self.iter.next()?;
            if dst != self.src {
                return Some(dst);
            }
        }
    }
}

unsafe impl<I: Iterator<Item = usize> + SortedIterator> SortedIterator for Succ<I> {}

impl<I: ExactSizeIterator<Item = usize>> ExactSizeIterator for Succ<I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

#[cfg(test)]
#[test]
fn test_no_selfloops_graph() -> anyhow::Result<()> {
    use crate::{graphs::vec_graph::VecGraph, prelude::proj::Left};
    let g = VecGraph::from_arc_list([(0, 1), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2)]);
    let p = NoSelfLoopsGraph(Left(g));
    assert_eq!(p.num_nodes(), 3);

    let mut iter = p.iter();
    assert_eq!(iter.next().unwrap().1.collect::<Vec<_>>(), vec![1]);
    assert_eq!(iter.next().unwrap().1.collect::<Vec<_>>(), vec![2]);
    assert_eq!(iter.next().unwrap().1.collect::<Vec<_>>(), vec![0, 1]);
    assert!(iter.next().is_none());

    Ok(())
}
