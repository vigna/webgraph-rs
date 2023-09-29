/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::*;
use core::marker::PhantomData;

/// A Sequential graph built on an iterator of pairs of nodes
#[derive(Debug, Clone)]
pub struct COOIterToGraph<'a, I: Clone> {
    num_nodes: usize,
    iter: &'a I,
}

impl<'a, I: Iterator<Item = (usize, usize)> + Clone> COOIterToGraph<'a, I> {
    /// Create a new graph from an iterator of pairs of nodes
    pub fn new(num_nodes: usize, iter: &mut I) -> Self {
        Self { num_nodes, iter }
    }
}

impl<'node, 'succ, I: Iterator<Item = (usize, usize)> + Clone> LendingIteratorItem<'succ>
    for NodeIterator<'node, I>
{
    type T = (usize, Successors<'succ, I>);
}

impl<'node, 'succ, I: Iterator<Item = (usize, usize)> + Clone> LendingIterator
    for NodeIterator<'node, I>
{
    fn next(&mut self) -> Option<(usize, Successors<'_, I>)> {
        self.curr_node = self.curr_node.wrapping_add(1);
        if self.curr_node == self.num_nodes {
            return None;
        }

        // This happens if the user doesn't use the successors iter
        while self.next_pair.0 < self.curr_node {
            self.next_pair = self.iter.next().unwrap_or((usize::MAX, usize::MAX));
        }

        Some((self.curr_node, Successors { node_iter: self }))
    }
}

impl<'node, I: Iterator<Item = (usize, usize)> + Clone> SequentialGraph
    for COOIterToGraph<'node, I>
{
    type Successors<'succ> = Successors<'succ, I>;
    /// Iterator over the nodes of the graph
    type Iterator<'a> = NodeIterator<'a, I>
    where Self: 'a;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    /// Get an iterator over the nodes of the graph
    fn iter_nodes_from(&self, from: usize) -> NodeIterator<'_, I> {
        NodeIterator::new(self.num_nodes, self.iter.clone())
    }
}

#[derive(Debug, Clone)]
pub struct NodeIterator<'a, I: Iterator<Item = (usize, usize)>> {
    num_nodes: usize,
    curr_node: usize,
    next_pair: (usize, usize),
    iter: I,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, I: Iterator<Item = (usize, usize)>> NodeIterator<'a, I> {
    pub fn new(num_nodes: usize, mut iter: I) -> Self {
        NodeIterator {
            num_nodes,
            curr_node: 0_usize.wrapping_sub(1), // No node seen yet
            next_pair: iter.next().unwrap_or((usize::MAX, usize::MAX)),
            iter,
            _marker: PhantomData,
        }
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

/// Iter until we found a triple with src different than curr_node
pub struct Successors<'a, I: Iterator<Item = (usize, usize)>> {
    node_iter: &'a mut NodeIterator<'a, I>,
}

impl<'a, I: Iterator<Item = (usize, usize)>> Iterator for Successors<'a, I> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        // if we reached a new node, the successors of curr_node are finished
        if self.node_iter.next_pair.0 != self.node_iter.curr_node {
            None
        } else {
            // get the next triple
            let pair = self
                .node_iter
                .iter
                .next()
                .unwrap_or((usize::MAX, usize::MAX));
            // store the triple and return the previous successor
            // storing the label since it should be one step behind the successor
            let (_src, dst) = core::mem::replace(&mut self.node_iter.next_pair, pair);
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
    let coo = COOIterToGraph::new(g.num_nodes(), &mut arcs.clone().into_iter());
    let g2 = VecGraph::from_node_iter(coo.iter_nodes());
    assert_eq!(g, g2);
    Ok(())
}
