use crate::traits::{NumNodes, SequentialGraph};
use anyhow::Result;
use core::marker::PhantomData;
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

    pub fn build(mut self) -> Result<MergedGraph> {
        self.sort_pairs.finish()?;
        Ok(MergedGraph {
            num_nodes: self.num_nodes,
            sorted_pairs: self.sort_pairs,
        })
    }
}

pub struct MergedGraph {
    num_nodes: usize,
    sorted_pairs: SortPairs<()>,
}

impl NumNodes for MergedGraph {
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }
}

impl SequentialGraph for MergedGraph {
    type NodesIter<'b> = SortedNodePermutedIterator<'b> where Self: 'b;
    type SequentialSuccessorIter<'b> = SortedSequentialPermutedIterator<'b> where Self: 'b;

    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        let mut iter = self.sorted_pairs.iter();

        SortedNodePermutedIterator {
            num_nodes: self.num_nodes,
            curr_node: 0_usize.wrapping_sub(1), // No node seen yet
            next_pair: iter.iter.next().unwrap_or((usize::MAX, usize::MAX)),
            iter: iter.iter,
            _marker: PhantomData,
        }
    }
}

pub struct SortedNodePermutedIterator<'a> {
    num_nodes: usize,
    curr_node: usize,
    next_pair: (usize, usize),
    iter: itertools::KMerge<BatchIterator>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Iterator for SortedNodePermutedIterator<'a> {
    type Item = (usize, SortedSequentialPermutedIterator<'a>);
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
                    let self_ptr: *mut SortedNodePermutedIterator = self;
                    self_ptr
                },
            },
        ))
    }
}

pub struct SortedSequentialPermutedIterator<'a> {
    node_iter_ptr: *mut SortedNodePermutedIterator<'a>,
}

impl<'a> Iterator for SortedSequentialPermutedIterator<'a> {
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
#[test]
fn test_permuted_graph() -> Result<()> {
    use crate::bvgraph::VecGraph;
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

#[test]
fn test_sorted_permuted_graph() -> Result<()> {
    use crate::bvgraph::VecGraph;
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let mut s = Sorted::new(g.num_nodes(), 1)?;
    s.extend(g.iter_nodes())?;
    let m = s.build()?;
    let h = VecGraph::from_node_iter(m.iter_nodes());
    assert_eq!(g, h);

    for batch_size in vec![1, 10, 100] {
        let mut s = Sorted::new(4, batch_size)?;
        for _ in 0..100 {
            s.push(1, 2)?;
            s.push(2, 2)?;
            s.push(2, 1)?;
            s.push(1, 1)?;
        }

        let m = s.build()?;
        let mut g = VecGraph::empty(4);
        g.add_arc_list(&[(1, 1), (1, 2), (2, 2), (2, 1)]);
        let h = VecGraph::from_node_iter(m.iter_nodes());
        assert_eq!(g, h);
    }

    Ok(())
}
