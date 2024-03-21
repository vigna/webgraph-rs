/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use lender::*;
use std::iter::Iterator;

#[derive(Clone)]
/// A wrapper applying a permutation to the iterators of an underlying graph.
///
/// Note that nodes are simply remapped: thus, neither the iterator on the graph
/// nor the successors are sorted.
pub struct PermutedGraph<'a, G: SequentialGraph> {
    pub graph: &'a G,
    pub perm: &'a [usize],
}

impl<'a, G: SequentialGraph> SequentialLabeling for PermutedGraph<'a, G> {
    type Label = usize;
    type Iterator<'b> = Iter<'b, G::Iterator<'b>>
        where
            Self: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        self.graph.num_arcs_hint()
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        Iter {
            iter: self.graph.iter_from(from),
            perm: self.perm,
        }
    }
}

impl<'b, G: SequentialGraph> SplitLabeling for PermutedGraph<'b, G>
where
    for<'a> <G as SequentialLabeling>::Iterator<'a>: Clone + ExactSizeLender,
{
    type Lender<'a> = split::seq::Lender<'a, PermutedGraph<'b, G> > where Self: 'a;
    type IntoIterator<'a> = split::seq::IntoIterator<'a, PermutedGraph<'b, G>> where Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::seq::Iter::new(self.iter(), how_many)
    }
}

impl<'a, G: SequentialGraph> SequentialGraph for PermutedGraph<'a, G> {}

impl<'a, 'b, G: SequentialGraph> IntoLender for &'b PermutedGraph<'a, G> {
    type Lender = <PermutedGraph<'a, G> as SequentialLabeling>::Iterator<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

/// An iterator over the nodes of a graph that applies on the fly a permutation of the nodes.
#[derive(Debug, Clone)]
pub struct Iter<'node, I> {
    iter: I,
    perm: &'node [usize],
}

impl<'node, 'succ, I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>>
    NodeLabelsLender<'succ> for Iter<'node, I>
{
    type Label = usize;
    type IntoIterator = Succ<'succ, LenderIntoIter<'succ, I>>;
}

impl<'node, 'succ, I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>> Lending<'succ>
    for Iter<'node, I>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<'a, L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>> Lender for Iter<'a, L> {
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.iter.next().map(|x| {
            let (node, succ) = x.into_pair();
            (
                self.perm[node],
                Succ {
                    iter: succ.into_iter(),
                    perm: self.perm,
                },
            )
        })
    }
}

impl<'a, L: ExactSizeLender + for<'next> NodeLabelsLender<'next, Label = usize>> ExactSizeLender
    for Iter<'a, L>
{
    fn len(&self) -> usize {
        self.iter.len()
    }
}

#[derive(Clone)]
pub struct Succ<'a, I: Iterator<Item = usize>> {
    iter: I,
    perm: &'a [usize],
}

impl<'a, I: Iterator<Item = usize>> Iterator for Succ<'a, I> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|succ| self.perm[succ])
    }
}

impl<'a, I: ExactSizeIterator<Item = usize>> ExactSizeIterator for Succ<'a, I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

#[cfg(test)]
#[test]
fn test_permuted_graph() -> anyhow::Result<()> {
    use crate::{graphs::vec_graph::VecGraph, prelude::proj::Left};
    let g = VecGraph::from_arc_list([(0, 1), (1, 2), (2, 0), (2, 1)]);
    let p = PermutedGraph {
        graph: &Left(g),
        perm: &[2, 0, 1],
    };
    assert_eq!(p.num_nodes(), 3);
    assert_eq!(p.num_arcs_hint(), Some(4));
    let v = Left(VecGraph::from_lender(p.iter()));

    assert_eq!(v.num_nodes(), 3);
    assert_eq!(v.outdegree(0), 1);
    assert_eq!(v.outdegree(1), 2);
    assert_eq!(v.outdegree(2), 1);
    assert_eq!(v.successors(0).into_iter().collect::<Vec<_>>(), vec![1]);
    assert_eq!(v.successors(1).into_iter().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(v.successors(2).into_iter().collect::<Vec<_>>(), vec![0]);

    Ok(())
}
