/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use hrtb_lending_iterator::*;

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

impl<'a, G: SequentialGraph> SequentialGraph for PermutedGraph<'a, G> {
    type Iterator<'b> = PermutedGraphIterator<'b, G::Iterator<'b>>
        where
            Self: 'b;
    type Successors<'b> = PermutedSuccessors<'b, <G::Successors<'b> as IntoIterator>::IntoIter>;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
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

impl<'a, 'b, G: SequentialGraph> IntoLendingIterator for &'b PermutedGraph<'a, G> {
    type IntoIter = <PermutedGraph<'a, G> as SequentialGraph>::Iterator<'b>;

    #[inline(always)]
    fn into_lend_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

//#[derive(Clone)]
/// An iterator over the nodes of a graph that applies on the fly a permutation of the nodes
pub struct PermutedGraphIterator<'node, I> {
    iter: I,
    perm: &'node [usize],
}

impl<'node, 'succ, I> LendingIteratorItem<'succ> for PermutedGraphIterator<'node, I>
where
    I: LendingIterator,
    for<'next> Item<'next, I>: Tuple2<_0 = usize>,
    for<'next> <Item<'next, I> as Tuple2>::_1: IntoIterator<Item = usize>,
{
    type T = (
        usize,
        PermutedSuccessors<'succ, <<Item<'succ, I> as Tuple2>::_1 as IntoIterator>::IntoIter>,
    );
}

impl<'a, L> LendingIterator for PermutedGraphIterator<'a, L>
where
    L: LendingIterator,
    for<'next> Item<'next, L>: Tuple2<_0 = usize>,
    for<'next> <Item<'next, L> as Tuple2>::_1: IntoIterator<Item = usize>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Item<'_, Self>> {
        self.iter.next().map(|x| {
            let (node, succ) = x.into_tuple();
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
    use crate::graph::vec_graph::VecGraph;
    use crate::traits::graph::RandomAccessGraph;
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let p = PermutedGraph {
        graph: &g,
        perm: &[2, 0, 1],
    };
    assert_eq!(p.num_nodes(), 3);
    assert_eq!(p.num_arcs_hint(), Some(4));
    let v = VecGraph::from_node_iter::<PermutedGraphIterator<'_, IteratorImpl<'_, VecGraph<()>>>>(
        p.iter(),
    );

    assert_eq!(v.num_nodes(), 3);
    assert_eq!(v.outdegree(0), 1);
    assert_eq!(v.outdegree(1), 2);
    assert_eq!(v.outdegree(2), 1);
    assert_eq!(v.successors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(v.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(v.successors(2).collect::<Vec<_>>(), vec![0]);

    Ok(())
}
