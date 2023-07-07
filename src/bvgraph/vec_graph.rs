use alloc::collections::BTreeSet;

use super::*;

/// Vector-based mutable [`Graph`] implementation.
/// Successors are represented using a [`BTreeSet`].
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct VecGraph {
    /// The number of arcs in the graph.
    number_of_arcs: usize,
    /// For each node, its list of successors.
    succ: Vec<BTreeSet<usize>>,
}

impl core::default::Default for VecGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl VecGraph {
    /// Create a new empty graph.
    pub fn new() -> Self {
        Self {
            number_of_arcs: 0,
            succ: vec![],
        }
    }

    /// Create a new empty graph with `n` nodes.
    pub fn empty(n: usize) -> Self {
        Self {
            number_of_arcs: 0,
            succ: Vec::from_iter((0..n).map(|_| BTreeSet::new())),
        }
    }

    /// Convert a COO arc list into a graph by sorting and deduplicating.
    pub fn from_arc_list(arcs: &[(usize, usize)]) -> Self {
        let mut g = Self::new();
        for (u, v) in arcs {
            g.add_arc(*u, *v);
        }
        g
    }

    /// Convert a the `iter_nodes` iterator of a graph into a [`VecGraph`].
    pub fn from_node_iter<S: Iterator<Item = usize>, I: Iterator<Item = (usize, S)>>(
        iterator: I,
    ) -> Self {
        let mut g = Self::new();
        g.add_node_iter(iterator);
        g
    }

    /// Add an arc to the graph and return a reference to self to allow a
    /// builder-like usage.
    pub fn add_arc_list(&mut self, arcs: &[(usize, usize)]) -> &mut Self {
        for (u, v) in arcs {
            self.add_arc(*u, *v);
        }
        self
    }

    /// Add the nodes and sucessors from the `iter_nodes` iterator of a graph
    pub fn add_node_iter(
        &mut self,
        iterator: impl Iterator<Item = (usize, impl Iterator<Item = usize>)>,
    ) -> &mut Self {
        for (node, succ) in iterator {
            self.add_node(node);
            for v in succ {
                self.add_arc(node, v);
            }
        }
        self
    }

    /// Add an arc to the graph and return if it was a new one or not.
    /// `true` => already exist, `false` => new arc.
    pub fn add_arc(&mut self, u: usize, v: usize) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            self.succ.resize(max + 1, BTreeSet::new());
        }
        let result = self.succ[u].insert(v);
        self.number_of_arcs += result as usize;
        result
    }

    /// Remove an arc from the graph and return if it was present or not.
    /// Return Nones if the either nodes (`u` or `v`) do not exist.
    pub fn remove_arc(&mut self, u: usize, v: usize) -> Option<bool> {
        if u >= self.succ.len() || v >= self.succ.len() {
            return None;
        }
        let result = self.succ[u].remove(&v);
        self.number_of_arcs -= result as usize;
        Some(result)
    }

    /// Add a node to the graph without successors and return if it was a new
    /// one or not.
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| BTreeSet::new()));
        len <= node
    }
}

impl NumNodes for VecGraph {
    fn num_nodes(&self) -> usize {
        self.succ.len()
    }
}

impl RandomAccessGraph for VecGraph {
    type RandomSuccessorIter<'a> = <BTreeSet<usize> as IntoIterator>::IntoIter;

    fn num_arcs(&self) -> usize {
        self.number_of_arcs
    }

    fn outdegree(&self, node: usize) -> usize {
        self.succ[node].len()
    }

    fn successors(&self, node: usize) -> Self::RandomSuccessorIter<'_> {
        self.succ[node].clone().into_iter()
    }
}

impl SequentialGraphImpl for VecGraph {}

impl SortedNodes for VecGraph {}

impl SortedSuccessors for VecGraph {}
