use crate::traits::*;
use core::marker::PhantomData;

/// A Sequential graph built on an iterator of pairs of nodes
#[derive(Debug, Clone)]
pub struct COOIterToGraph<I: Iterator<Item = (usize, usize)> + Clone> {
    num_nodes: usize,
    iter: I,
}

impl<I: Iterator<Item = (usize, usize)> + Clone> COOIterToGraph<I> {
    /// Create a new graph from an iterator of pairs of nodes
    pub fn new(num_nodes: usize, iter: I) -> Self {
        Self { num_nodes, iter }
    }
}

impl<I: Iterator<Item = (usize, usize)> + Clone> SequentialGraph for COOIterToGraph<I> {
    type NodesIter<'b> = SortedNodePermutedIterator<'b, I> where Self: 'b;
    type SequentialSuccessorIter<'b> = SortedSequentialPermutedIterator<'b, I> where Self: 'b;

    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        let mut iter = self.iter.clone();

        SortedNodePermutedIterator {
            num_nodes: self.num_nodes,
            curr_node: 0_usize.wrapping_sub(1), // No node seen yet
            next_pair: iter.next().unwrap_or((usize::MAX, usize::MAX)),
            iter,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SortedNodePermutedIterator<'a, I: Iterator<Item = (usize, usize)> + Clone> {
    num_nodes: usize,
    curr_node: usize,
    next_pair: (usize, usize),
    iter: I,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, I: Iterator<Item = (usize, usize)> + Clone> Iterator
    for SortedNodePermutedIterator<'a, I>
{
    type Item = (usize, SortedSequentialPermutedIterator<'a, I>);
    fn next(&mut self) -> Option<Self::Item> {
        self.curr_node = self.curr_node.wrapping_add(1);
        if self.curr_node == self.num_nodes {
            return None;
        }

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

#[derive(Debug, Clone)]
pub struct SortedSequentialPermutedIterator<'a, I: Iterator<Item = (usize, usize)> + Clone> {
    node_iter_ptr: *mut SortedNodePermutedIterator<'a, I>,
}

impl<'a, I: Iterator<Item = (usize, usize)> + Clone> Iterator
    for SortedSequentialPermutedIterator<'a, I>
{
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        let node_iter = unsafe { &mut *self.node_iter_ptr };
        if node_iter.next_pair.0 != node_iter.curr_node {
            None
        } else {
            loop {
                // Skip duplicate pairs
                let pair = node_iter.iter.next().unwrap_or((usize::MAX, usize::MAX));
                if pair != node_iter.next_pair {
                    let result = node_iter.next_pair.1;
                    node_iter.next_pair = pair;
                    return Some(result);
                }
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(test, test)]
fn test_coo_iter() -> anyhow::Result<()> {
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = crate::prelude::VecGraph::from_arc_list(&arcs);
    let coo = COOIterToGraph::new(g.num_nodes(), arcs.clone().into_iter());
    let g2 = crate::prelude::VecGraph::from_node_iter(coo.iter_nodes());
    assert_eq!(g, g2);
    Ok(())
}
