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

impl<'a, I: Iterator<Item = (usize, J)>, J: ExactSizeIterator<Item = usize>> Iterator
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

pub struct SequentialPermutedIterator<'a, I: ExactSizeIterator<Item = usize>> {
    iter: I,
    perm: &'a [usize],
}

impl<'a, I: ExactSizeIterator<Item = usize>> Iterator for SequentialPermutedIterator<'a, I> {
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

    pub fn build(self) -> MergedGraph {
        MergedGraph {
            num_nodes: self.num_nodes,
            sorted_pairs: self.sort_pairs,
        }
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
    type NodesIter<'b> = SortedNodePermutedIterator;
    type SequentialSuccessorIter<'b> = SortedSequentialPermutedIterator;

    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        SortedNodePermutedIterator {
            iter: self.sorted_pairs.iter(),
        }
    }
}

pub struct SortedNodePermutedIterator {
    iter: itertools::KMerge<BatchIterator>,
}

impl Iterator for SortedNodePermutedIterator {
    type Item = (usize, SortedSequentialPermutedIterator);
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

pub struct SortedSequentialPermutedIterator {
    sorted_pairs: SortPairs<()>,
}

impl Iterator for SortedSequentialPermutedIterator {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl ExactSizeIterator for SortedSequentialPermutedIterator {
    fn len(&self) -> usize {
        0
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
