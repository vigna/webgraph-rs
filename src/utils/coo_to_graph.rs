/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::*;
use core::marker::PhantomData;

/// A Sequential graph built on an iterator of pairs of nodes
#[derive(Debug, Clone)]
pub struct COOIterToGraph<I: Clone> {
    num_nodes: usize,
    iter: I,
}

impl<I: Iterator<Item = (usize, usize)> + Clone> COOIterToGraph<I> {
    /// Create a new graph from an iterator of pairs of nodes
    #[inline(always)]
    pub fn new(num_nodes: usize, iter: I) -> Self {
        Self { num_nodes, iter }
    }
}

impl<I: Iterator<Item = (usize, usize)> + Clone> SequentialGraph for COOIterToGraph<I> {
    type NodesStream<'b> =  SortedNodePermutedIterator<'b, I>
        where
            Self: 'b;
    type SuccessorStream<'b> = SortedSequentialPermutedIterator<'b, I> where Self: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    /// Get an iterator over the nodes of the graph
    fn stream_nodes(&self) -> Self::NodesStream<'_> {
        SortedNodePermutedIterator::new(self.num_nodes, self.iter.clone())
    }
}

#[derive(Debug, Clone)]
pub struct SortedNodePermutedIterator<'a, I: Iterator<Item = (usize, usize)>> {
    num_nodes: usize,
    curr_node: usize,
    next_pair: (usize, usize),
    iter: I,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, I: Iterator<Item = (usize, usize)>> SortedNodePermutedIterator<'a, I> {
    pub fn new(num_nodes: usize, mut iter: I) -> Self {
        SortedNodePermutedIterator {
            num_nodes,
            curr_node: 0_usize.wrapping_sub(1), // No node seen yet
            next_pair: iter.next().unwrap_or((usize::MAX, usize::MAX)),
            iter,
            _marker: PhantomData,
        }
    }
}

impl<'a, I: Iterator<Item = (usize, usize)>> StreamingIterator
    for SortedNodePermutedIterator<'a, I>
{
    type StreamItem<'b> = (usize, SortedSequentialPermutedIterator<'b, I>)
    where
        Self: 'b
    ;

    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>> {
        self.curr_node = self.curr_node.wrapping_add(1);
        if self.curr_node == self.num_nodes {
            return None;
        }

        // This happens if the user doesn't use the successors iter
        while self.next_pair.0 < self.curr_node {
            self.next_pair = self.iter.next().unwrap_or((usize::MAX, usize::MAX));
        }

        Some((
            self.curr_node,
            SortedSequentialPermutedIterator {
                node_iter_ptr: {
                    let self_ptr: *mut Self = self;
                    self_ptr
                },
            },
        ))
    }
}

impl<'a, I: Iterator<Item = (usize, usize)>> Iterator for SortedNodePermutedIterator<'a, I> {
    type Item = (usize, Vec<usize>);
    fn next(&mut self) -> Option<Self::Item> {
        self.next_stream()
            .map(|(node_id, succ)| (node_id, succ.collect::<Vec<_>>()))
    }
}

/*
impl<'a, I: Iterator<Item = (usize, usize)>> ExactSizeIterator
    for SortedNodePermutedIterator<'a, I>
{
    fn len(&self) -> usize {
        self.num_nodes - self.curr_node - 1
    }
}*/

#[derive(Debug, Clone)]
/// Iter until we found a triple with src different than curr_node
pub struct SortedSequentialPermutedIterator<'a, I: Iterator<Item = (usize, usize)>> {
    node_iter_ptr: *mut SortedNodePermutedIterator<'a, I>,
}

impl<'a, I: Iterator<Item = (usize, usize)>> Iterator for SortedSequentialPermutedIterator<'a, I> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        let node_iter = unsafe { &mut *self.node_iter_ptr };
        // if we reached a new node, the successors of curr_node are finished
        if node_iter.next_pair.0 != node_iter.curr_node {
            None
        } else {
            // get the next triple
            let pair = node_iter.iter.next().unwrap_or((usize::MAX, usize::MAX));
            // store the triple and return the previous successor
            // storing the label since it should be one step behind the successor
            let (_src, dst) = core::mem::replace(&mut node_iter.next_pair, pair);
            Some(dst)
        }
    }
}

#[cfg(test)]
#[cfg_attr(test, test)]
fn test_coo_iter() -> anyhow::Result<()> {
    use crate::graph::vec_graph::VecGraph;
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = VecGraph::from_arc_list(&arcs);
    let coo = COOIterToGraph::new(g.num_nodes(), arcs.clone().into_iter());
    let g2 = VecGraph::from_node_iter(coo.iter_nodes());
    assert_eq!(g, g2);
    Ok(())
}
