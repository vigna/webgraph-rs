use core::{cell::RefCell, marker::PhantomData};

use crate::traits::{NumNodes, SequentialGraph};
pub struct PermutedGraph<'a, G: SequentialGraph> {
    pub graph: &'a G,
    pub perm: &'a [usize],
}

impl<'a, G: SequentialGraph> NumNodes for PermutedGraph<'a, G> {
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
}

impl<'a, G: SequentialGraph> SequentialGraph for PermutedGraph<'a, G> {
    type NodesIter<'b> =
        NodePermutedIterator<'b, G::NodesIter<'b>, G::SequentialSuccessorIter<'b>>
		where Self: 'b;
    type SequentialSuccessorIter<'b> =
        SequentialPermutedIterator<'b, G::SequentialSuccessorIter<'b>>
		where Self: 'b;

    fn num_arcs_hint(&self) -> Option<usize> {
        self.graph.num_arcs_hint()
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        NodePermutedIterator {
            iter: self.graph.iter_nodes(),
            perm: self.perm,
        }
    }
}

pub struct NodePermutedIterator<'a, I: Iterator<Item = (usize, J)>, J: Iterator<Item = usize>> {
    iter: I,
    perm: &'a [usize],
}

impl<'a, I: Iterator<Item = (usize, J)>, J: Iterator<Item = usize>> Iterator
    for NodePermutedIterator<'a, I, J>
{
    type Item = (usize, SequentialPermutedIterator<'a, J>);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(node, iter)| {
            (
                self.perm[node],
                SequentialPermutedIterator {
                    iter,
                    perm: self.perm,
                },
            )
        })
    }
}

pub struct SequentialPermutedIterator<'a, I: Iterator<Item = usize>> {
    iter: I,
    perm: &'a [usize],
}

impl<'a, I: Iterator<Item = usize>> Iterator for SequentialPermutedIterator<'a, I> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| self.perm[x])
    }
}

impl<'a, I: ExactSizeIterator<Item = usize>> ExactSizeIterator
    for SequentialPermutedIterator<'a, I>
{
    fn len(&self) -> usize {
        self.iter.len()
    }
}

use super::{BatchIterator, SortPairs};
use anyhow::Result;
pub struct Sorted {
    num_nodes: usize,
    sort_pairs: SortPairs<()>,
}

impl Sorted {
    pub fn new(num_nodes: usize, batch_size: usize) -> anyhow::Result<Self> {
        Ok(Sorted {
            num_nodes,
            sort_pairs: SortPairs::new(batch_size)?,
        })
    }

    pub fn push(&mut self, x: usize, y: usize) -> Result<()> {
        self.sort_pairs.push(x, y, ())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.sort_pairs.finish()
    }

    pub fn extend<I: Iterator<Item = (usize, J)>, J: Iterator<Item = usize>>(
        &mut self,
        iter_nodes: I,
    ) -> Result<()> {
        for (x, succ) in iter_nodes {
            for s in succ {
                self.push(x, s)?;
            }
        }
        Ok(())
    }

    pub fn build<'a>(self) -> MergedGraph<'a> {
        MergedGraph {
            num_nodes: self.num_nodes,
            sorted_pairs: self.sort_pairs,
            _marker: PhantomData,
        }
    }
}

pub struct MergedGraph<'a> {
    num_nodes: usize,
    sorted_pairs: SortPairs<()>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> NumNodes for MergedGraph<'a> {
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }
}

impl<'a> SequentialGraph for MergedGraph<'a> {
    type NodesIter<'b> = SortedNodePermutedIterator<'b> where Self: 'b;
    type SequentialSuccessorIter<'b> = SortedSequentialPermutedIterator<'b> where Self: 'b;

    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        let mut iter = self.sorted_pairs.iter();

        SortedNodePermutedIterator {
            state: RefCell::new(SortedNodePermutedIteratorState {
                max_node: self.num_nodes.wrapping_sub(1),
                curr_node: 0_usize.wrapping_sub(1), // No node seen yet
                next_pair: iter.next().unwrap_or((usize::MAX, usize::MAX)),
                iter,
                _marker: PhantomData,
            }),
            _marker: PhantomData,
        }
    }
}

pub struct SortedNodePermutedIterator<'a> {
    state: std::cell::RefCell<SortedNodePermutedIteratorState<'a>>,
    _marker: std::marker::PhantomData<&'a ()>,
}

struct SortedNodePermutedIteratorState<'a> {
    max_node: usize,
    curr_node: usize,
    next_pair: (usize, usize),
    iter: itertools::KMerge<BatchIterator>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Iterator for SortedNodePermutedIterator<'a> {
    type Item = (usize, SortedSequentialPermutedIterator<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        let mut self_mut = self.state.borrow_mut();
        if self_mut.curr_node == self_mut.max_node {
            return None;
        }
        self_mut.curr_node = self_mut.curr_node.wrapping_add(1);

        while self_mut.next_pair.0 < self_mut.curr_node {
            self_mut.next_pair = self_mut.iter.next().unwrap_or((usize::MAX, usize::MAX));
        }

        Some((
            self_mut.curr_node,
            SortedSequentialPermutedIterator { state: self_mut },
        ))
    }
}

pub struct SortedSequentialPermutedIterator<'a> {
    state: std::cell::RefMut<'a, SortedNodePermutedIteratorState<'a>>,
}

impl<'a> Iterator for SortedSequentialPermutedIterator<'a> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        return if self.state.next_pair.0 != self.state.curr_node {
            None
        } else {
            loop {
                // Skip duplicate pairs
                let pair = self.state.iter.next().unwrap_or((usize::MAX, usize::MAX));
                if pair != self.state.next_pair {
                    let result = self.state.next_pair.1;
                    self.state.next_pair = pair;
                    return Some(result);
                }
            }
        };
    }
}

#[cfg(test)]
#[test]

fn test_permuted_graph() {
    use crate::traits::graph::RandomAccessGraph;
    use crate::webgraph::VecGraph;
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let p = PermutedGraph {
        graph: &g,
        perm: &[2, 0, 1],
    };
    assert_eq!(p.num_nodes(), 3);
    assert_eq!(p.num_arcs_hint(), Some(4));
    let v = VecGraph::from_node_iter(p.iter_nodes());

    assert_eq!(v.num_nodes(), 3);
    assert_eq!(v.outdegree(0).unwrap(), 1);
    assert_eq!(v.outdegree(1).unwrap(), 2);
    assert_eq!(v.outdegree(2).unwrap(), 1);
    assert_eq!(v.successors(0).unwrap().collect::<Vec<_>>(), vec![1]);
    assert_eq!(v.successors(1).unwrap().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(v.successors(2).unwrap().collect::<Vec<_>>(), vec![0]);
}
