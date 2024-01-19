/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::*;

use crate::prelude::*;

#[derive(Clone)]
/// A wrapper applying a permutation to the iterators of an underlying graph.
///
/// Note that nodes are simply remapped: thus, neither the iterator on the graph
/// nor the successors are sorted.
pub struct PermutedGraph<'a, G: SequentialGraph> {
    pub graph: &'a G,
    pub perm: &'a [usize],
}

impl<'a, G: SequentialGraph> SequentialLabelling for PermutedGraph<'a, G> {
    type Label = usize;
    type Iterator<'b> = PermutedGraphIterator<'b, G::Iterator<'b>>
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
        PermutedGraphIterator {
            iter: self.graph.iter_from(from),
            perm: self.perm,
        }
    }
}

impl<'a, G: SequentialGraph> SequentialGraph for PermutedGraph<'a, G> {}

impl<'a, 'b, G: SequentialGraph> IntoLender for &'b PermutedGraph<'a, G> {
    type Lender = <PermutedGraph<'a, G> as SequentialLabelling>::Iterator<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

/// An iterator over the nodes of a graph that applies on the fly a permutation of the nodes
pub struct PermutedGraphIterator<'node, I> {
    iter: I,
    perm: &'node [usize],
}

impl<'node, 'succ, I> NodeLabelsLender<'succ> for PermutedGraphIterator<'node, I>
where
    I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
{
    type Label = usize;
    type IntoIterator = PermutedSuccessors<'succ, LenderIntoIter<'succ, I>>;
}

impl<'node, 'succ, I> Lending<'succ> for PermutedGraphIterator<'node, I>
where
    I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<'a, L> Lender for PermutedGraphIterator<'a, L>
where
    L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.iter.next().map(|x| {
            let (node, succ) = x.into_pair();
            (
                self.perm[node],
                PermutedSuccessors {
                    iter: succ.into_iter(),
                    perm: self.perm,
                },
            )
        })
    }
}

#[derive(Clone)]
pub struct PermutedSuccessors<'a, I: Iterator<Item = usize>> {
    iter: I,
    perm: &'a [usize],
}

impl<'a, I: Iterator<Item = usize>> Iterator for PermutedSuccessors<'a, I> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|succ| self.perm[succ])
    }
}

impl<'a, I: ExactSizeIterator<Item = usize>> ExactSizeIterator for PermutedSuccessors<'a, I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

#[cfg(test)]
#[test]
fn test_permuted_graph() -> anyhow::Result<()> {
    use crate::{graph::vec_graph::VecGraph, prelude::proj::Left};
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
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
