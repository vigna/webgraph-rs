/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

#[derive(Clone)]
/// A Graph wrapper that applies on the fly a permutation of the nodes
pub struct PermutedGraph<'a, G: SequentialGraph> {
    pub graph: &'a G,
    pub perm: &'a [usize],
}

impl<'a, G: SequentialGraph> SequentialGraph for PermutedGraph<'a, G> {
    type Iterator<'b> = PermutedGraphIterator<'b, G::Iterator<'b>>
        where
            Self: 'b;
    type Successors<'b> = PermutedSuccessors<'b, G::Successors<'b>>;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        self.graph.num_arcs_hint()
    }

    #[inline(always)]
    fn iter_nodes_from(&self, from: usize) -> Self::Iterator<'_> {
        PermutedGraphIterator {
            iter: self.graph.iter_nodes_from(from),
            perm: self.perm,
        }
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
        PermutedSuccessors<'succ, <Item<'succ, I> as Tuple2>::_1>,
    );
}

impl<'a, L> LendingIterator for PermutedGraphIterator<'a, L>
where
    L: LendingIterator,
    for<'next> Item<'next, L>: Tuple2<_0 = usize>,
    for<'next> <Item<'next, L> as Tuple2>::_1: IntoIterator<Item = usize>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<<Self as LendingIteratorItem>::T> {
        self.iter.next().map(|x| {
            let (node, succ) = x.is_tuple();
            (
                self.perm[node],
                PermutedSuccessors {
                    into_iter: succ,
                    perm: self.perm,
                },
            )
        })
    }
}

/*
impl<'a, I: ExactSizeIterator<Item = (usize, J)>, J: Iterator<Item = usize>> ExactSizeIterator
    for PermutedGraphIterator<'a, I, J>
{
    fn len(&self) -> usize {
        self.iter.len()
    }
} */

#[derive(Clone)]
/// An iterator over the successors of a node of a graph that applies on the fly a permutation of the nodes
pub struct PermutedSuccessors<'a, I: IntoIterator<Item = usize>> {
    into_iter: I,
    perm: &'a [usize],
}

impl<'a, I: IntoIterator<Item = usize>> IntoIterator for PermutedSuccessors<'a, I> {
    type Item = usize;
    type IntoIter = I::IntoIter;
    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.into_iter.into_iter() //. TODO map(|succ| self.perm[succ])
    }
}

/*TODO
impl<'a, I: ExactSizeIterator<Item = usize>> ExactSizeIterator for PermutedSuccessors<'a, I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }
}
*/
/* TODO
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
    let v = VecGraph::from_node_iter(p.iter_nodes());

    assert_eq!(v.num_nodes(), 3);
    assert_eq!(v.outdegree(0), 1);
    assert_eq!(v.outdegree(1), 2);
    assert_eq!(v.outdegree(2), 1);
    assert_eq!(v.successors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(v.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(v.successors(2).collect::<Vec<_>>(), vec![0]);

    Ok(())
}
*/
