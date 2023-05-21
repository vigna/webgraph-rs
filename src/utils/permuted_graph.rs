use crate::traits::Graph;
use crate::webgraph::VecGraph;
pub struct PermutedGraph<'a, G: Graph> {
    graph: &'a G,
    perm: &'a [usize],
}

impl<'a, G: Graph> Graph for PermutedGraph<'a, G> {
    type NodesIter<'b> =
        NodePermutedIterator<'b, G::NodesIter<'b>, G::SequentialSuccessorIter<'b>>
		where Self: 'b;
    type RandomSuccessorIter<'b> = SequentialPermutedIterator<'b, G::RandomSuccessorIter<'b>>
		where Self: 'b;
    type SequentialSuccessorIter<'b> =
        SequentialPermutedIterator<'b, G::SequentialSuccessorIter<'b>>
		where Self: 'b;
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }

    fn outdegree(&self, node: usize) -> std::result::Result<usize, anyhow::Error> {
        for (i, &perm_node) in self.perm.iter().enumerate() {
            if perm_node == node {
                return self.graph.outdegree(i);
            }
        }
        unreachable!()
    }

    fn successors(&self, node: usize) -> anyhow::Result<Self::RandomSuccessorIter<'_>> {
        for (i, &perm_node) in self.perm.iter().enumerate() {
            if perm_node == node {
                return Ok(SequentialPermutedIterator {
                    iter: self.graph.successors(i)?,
                    perm: self.perm,
                });
            }
        }
        unreachable!()
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

#[cfg(test)]
#[test]

fn test_permuted_graph() {
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let p = PermutedGraph {
        graph: &g,
        perm: &[2, 0, 1],
    };

    assert_eq!(p.num_nodes(), 3);
    assert_eq!(p.num_arcs(), 4);
    assert_eq!(p.outdegree(0).unwrap(), 1);
    assert_eq!(p.outdegree(1).unwrap(), 2);
    assert_eq!(p.outdegree(2).unwrap(), 1);
    assert_eq!(p.successors(0).unwrap().collect::<Vec<_>>(), vec![1]);
    assert_eq!(p.successors(1).unwrap().collect::<Vec<_>>(), vec![2, 0]);
    assert_eq!(p.successors(2).unwrap().collect::<Vec<_>>(), vec![0]);
}
