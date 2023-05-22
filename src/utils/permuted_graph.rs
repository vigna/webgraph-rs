use crate::traits::{NumNodes, SequentialGraph};
pub struct PermutedGraph<'a, G: SequentialGraph> {
    graph: &'a G,
    perm: &'a [usize],
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

#[cfg(test)]
#[test]

fn test_permuted_graph() {
    use crate::traits::graph::RandomAccessGraph;
    use crate::webgraph::VecGraph;
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let p = VecGraph::from_node_iter(
        PermutedGraph {
            graph: &g,
            perm: &[2, 0, 1],
        }
        .iter_nodes(),
    );

    assert_eq!(p.num_nodes(), 3);
    assert_eq!(p.outdegree(0).unwrap(), 1);
    assert_eq!(p.outdegree(1).unwrap(), 2);
    assert_eq!(p.outdegree(2).unwrap(), 1);
    assert_eq!(p.successors(0).unwrap().collect::<Vec<_>>(), vec![1]);
    assert_eq!(p.successors(1).unwrap().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(p.successors(2).unwrap().collect::<Vec<_>>(), vec![0]);
}
