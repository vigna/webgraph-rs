use alloc::collections::BTreeSet;

use super::*;

/// Vector-based mutable [`Graph`] implementation.
/// Successors are represented using a [`BTreeSet`].
pub struct VecGraph {
    /// The number of arcs in the graph.
    number_of_arcs: usize,
    /// For each node, its list of successors.
    succ: Vec<BTreeSet<usize>>,
}

impl VecGraph {
    pub fn new() -> Self {
        Self {
            number_of_arcs: 0,
            succ: vec![],
        }
    }

    pub fn empty(n: usize) -> Self {
        Self {
            number_of_arcs: 0,
            succ: Vec::from_iter((0..n).map(|_| BTreeSet::new())),
        }
    }

    pub fn from_arc_list(arcs: &[(usize, usize)]) -> Self {
        let mut g = Self::new();
        for (u, v) in arcs {
            g.add_arc(*u, *v);
        }
        g
    }

    pub fn from_node_iter<S: Iterator<Item = usize>, I: Iterator<Item = (usize, S)>>(
        iterator: I,
    ) -> Self {
        let mut g = Self::new();
        for (node, succ) in iterator {
            for v in succ {
                g.add_arc(node, v);
            }
        }
        g
    }

    pub fn add_arc_list(&mut self, arcs: &[(usize, usize)]) -> &mut Self {
        for (u, v) in arcs {
            self.add_arc(*u, *v);
        }
        self
    }

    pub fn add_node_iter(
        &mut self,
        iterator: impl Iterator<Item = (usize, impl Iterator<Item = usize>)>,
    ) -> &mut Self {
        for (node, succ) in iterator {
            for v in succ {
                self.add_arc(node, v);
            }
        }
        self
    }

    pub fn add_arc(&mut self, u: usize, v: usize) {
        if u >= self.succ.len() {
            self.succ.resize(u + 1, BTreeSet::new());
        }
        self.succ[u].insert(v);
        self.number_of_arcs += 1;
    }

    pub fn add_node(&mut self) {
        self.succ.push(BTreeSet::new());
    }
}

pub struct VecGraphNodesIter<'a> {
    iter: std::iter::Enumerate<std::slice::Iter<'a, BTreeSet<usize>>>,
}

impl Iterator for VecGraphNodesIter<'_> {
    type Item = (usize, std::collections::btree_set::IntoIter<usize>);
    fn next(&mut self) -> Option<Self::Item> {
        None
        //self.iter.next().map(|(node, succ)| (node, succ.iter()))
    }
}

impl RandomAccessGraph for VecGraph {
    type RandomSuccessorIter<'a> = <BTreeSet<usize> as IntoIterator>::IntoIter;

    fn num_nodes(&self) -> usize {
        self.succ.len()
    }

    fn num_arcs(&self) -> usize {
        self.number_of_arcs
    }

    fn outdegree(&self, node: usize) -> Result<usize> {
        Ok(self.succ[node].len())
    }

    fn successors(&self, node: usize) -> Result<Self::RandomSuccessorIter<'_>> {
        Ok(self.succ[node].clone().into_iter())
    }
}

impl SequentialGraph for VecGraph {
    type NodesIter<'a> = VecGraphNodesIter<'a>;
    type SequentialSuccessorIter<'a> = <BTreeSet<usize> as IntoIterator>::IntoIter;

    fn num_nodes(&self) -> usize {
        self.succ.len()
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        unreachable!()
    }
}

impl SortedSuccessors for VecGraph {}
