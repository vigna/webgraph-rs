use crate::traits::Graph;

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
