/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use core::marker::PhantomData;

use crate::traits::{SequentialGraph, StreamingIterator};

#[derive(Clone)]
/// A Graph wrapper that applies on the fly a permutation of the nodes
pub struct PermutedGraph<'a, G: SequentialGraph> {
    pub graph: &'a G,
    pub perm: &'a [usize],
}

impl<'a, G: SequentialGraph> SequentialGraph for PermutedGraph<'a, G> {
    type NodesStream<'b> = NodePermutedIterator<'b, G::NodesStream<'b>>
        where
            Self: 'b;
    type SuccessorStream<'b> =
        PermSeqSuccIterator<'b, <G::SuccessorStream<'b> as IntoIterator>::IntoIter>
		where Self: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        self.graph.num_arcs_hint()
    }

    #[inline(always)]
    fn stream_nodes(&self) -> Self::NodesStream<'_> {
        NodePermutedIterator {
            iter: self.graph.stream_nodes(),
            perm: self.perm,
        }
    }

    /*#[inline(always)]
    fn iter_nodes_from(&self, start_node: usize) -> Self::NodesIter<'_> {
        NodePermutedIterator {
            iter: self.graph.iter_nodes_from(start_node),
            perm: self.perm,
        }
    }*/
}

//#[derive(Clone)]
/// An iterator over the nodes of a graph that applies on the fly a permutation of the nodes
pub struct NodePermutedIterator<'a, I: StreamingIterator> {
    iter: I,
    perm: &'a [usize],
}

impl<'a, I: for<'b> StreamingIterator> StreamingIterator for NodePermutedIterator<'a, I>
where
    <I as StreamingIterator>::StreamItem<'a>: Iterator<Item = usize>,
    for<'b> I: 'b,
{
    type StreamItem<'c> = (usize, PermSeqSuccIterator<'c, I::StreamItem<'a>>)
        where <I as StreamingIterator>::StreamItem<'a>: Iterator<Item = usize>,
        Self: 'c;

    #[inline(always)]
    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>> {
        self.iter.next_stream().map(|(node, succ)| {
            (
                self.perm[node],
                PermSeqSuccIterator {
                    iter: succ.into_iter(),
                    perm: self.perm,
                },
            )
        })
    }
}

/*
impl<'a, I: ExactSizeIterator<Item = (usize, J)>, J: Iterator<Item = usize>> ExactSizeIterator
    for NodePermutedIterator<'a, I, J>
{
    fn len(&self) -> usize {
        self.iter.len()
    }
} */

#[derive(Clone)]
/// An iterator over the successors of a node of a graph that applies on the fly a permutation of the nodes
pub struct PermSeqSuccIterator<'a, I: Iterator<Item = usize>> {
    iter: I,
    perm: &'a [usize],
}

impl<'a, I: Iterator<Item = usize>> Iterator for PermSeqSuccIterator<'a, I> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| self.perm[x])
    }
}

impl<'a, I: ExactSizeIterator<Item = usize>> ExactSizeIterator for PermSeqSuccIterator<'a, I> {
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
