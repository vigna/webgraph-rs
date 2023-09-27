/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::graph::GraphIteratorImpl;
use crate::traits::*;
use alloc::collections::BTreeSet;
use gat_lending_iterator::LendingIterator;

/// Vector-based mutable [`Graph`] implementation.
/// Successors are represented using a [`BTreeSet`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VecGraph<L: Clone> {
    /// The number of arcs in the graph.
    number_of_arcs: usize,
    /// For each node, its list of successors.
    succ: Vec<BTreeSet<DstWithLabel<L>>>,
}

impl<L: Clone> core::default::Default for VecGraph<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Clone> VecGraph<L> {
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
    pub fn from_arc_and_label_list(arcs: &[(usize, usize, L)]) -> Self {
        let mut g = Self::new();
        g.add_arc_and_label_list(arcs);
        g
    }

    /// Add an arc to the graph and return a reference to self to allow a
    /// builder-like usage.
    pub fn add_arc_and_label_list(&mut self, arcs: &[(usize, usize, L)]) -> &mut Self {
        for (u, v, l) in arcs {
            self.add_arc_with_label(*u, *v, l.clone());
        }
        self
    }
    /* TODO
    /// Convert a the `iter_nodes` iterator of a graph into a [`VecGraph`].
    pub fn from_labelled_node_iter<S, I>(iterator: I) -> Self
    where
        I: Iterator<Item = (usize, S)>,
        S: Iterator<Item = usize> + LabelledIterator<Label = L>,
    {
        let mut g = Self::new();
        g.add_labelled_node_iter(iterator);
        g
    }

    /// Add the nodes and sucessors from the `iter_nodes` iterator of a graph
    pub fn add_labelled_node_iter<S, I>(&mut self, iterator: I) -> &mut Self
    where
        I: Iterator<Item = (usize, S)>,
        S: Iterator<Item = usize> + LabelledIterator<Label = L>,
    {
        for (node, succ) in iterator {
            self.add_node(node);
            for (v, l) in succ.labelled() {
                self.add_arc_with_label(node, v, l);
            }
        }
        self
    }
    */
    /// Add an arc to the graph and return if it was a new one or not.
    /// `true` => already exist, `false` => new arc.
    pub fn add_arc_with_label(&mut self, u: usize, v: usize, l: L) -> bool {
        self.add_node(u.max(v));
        let result = self.succ[u].insert(DstWithLabel(v, l));
        self.number_of_arcs += result as usize;
        result
    }

    /// Remove an arc from the graph and return if it was present or not.
    /// Return Nones if the either nodes (`u` or `v`) do not exist.
    pub fn remove_labelled_arc(&mut self, u: usize, v: usize, l: L) -> Option<bool> {
        if u >= self.succ.len() || v >= self.succ.len() {
            return None;
        }
        let result = self.succ[u].remove(&DstWithLabel(v, l));
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

impl VecGraph<()> {
    /// Convert a COO arc list into a graph by sorting and deduplicating.
    pub fn from_arc_list(arcs: &[(usize, usize)]) -> Self {
        let mut g = Self::new();
        g.add_arc_list(arcs);
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
    /* TODO
        /// Convert a the `iter_nodes` iterator of a graph into a [`VecGraph`].
        pub fn from_graph_iter<I: GraphIterator>(iterator: I) -> Self {
            let mut g = Self::new();
            g.add_graph_iter(iterator);
            g
        }

        /// Add the nodes and sucessors from the `iter_nodes` iterator of a graph
        pub fn add_graph_iter(
            &mut self,
            iterator: impl LendingIterator<Item<'a> = (usize, impl Iterator<Item = usize> + 'a)>,
        ) -> &mut Self {
            for (node, succ) in iterator {
                self.add_node(node);
                for v in succ {
                    self.add_arc(node, v);
                }
            }
            self
        }
    */
    /* TODO
        /// Convert a the `iter_nodes` iterator of a graph into a [`VecGraph`].
        pub fn from_node_iter<I: GraphIterator>(iterator: I) -> Self {
            let mut g = Self::new();
            g.add_node_iter(iterator);
            g
        }

        /// Add the nodes and sucessors from the `iter_nodes` iterator of a graph
        pub fn add_node_iter(&mut self, iterator: GraphIterator) -> &mut Self {
            for (node, succ) in iterator {
                self.add_node(node);
                for v in succ {
                    self.add_arc(node, v);
                }
            }
            self
        }
    */
    /// Add an arc to the graph and return if it was a new one or not.
    /// `true` => already exist, `false` => new arc.
    pub fn add_arc(&mut self, u: usize, v: usize) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            self.succ.resize(max + 1, BTreeSet::new());
        }
        let result = self.succ[u].insert(DstWithLabel(v, ()));
        self.number_of_arcs += result as usize;
        result
    }

    /// Remove an arc from the graph and return if it was present or not.
    /// Return Nones if the either nodes (`u` or `v`) do not exist.
    pub fn remove_arc(&mut self, u: usize, v: usize) -> Option<bool> {
        if u >= self.succ.len() || v >= self.succ.len() {
            return None;
        }
        let result = self.succ[u].remove(&DstWithLabel(v, ()));
        self.number_of_arcs -= result as usize;
        Some(result)
    }
}

impl<L: Clone> Labelled for VecGraph<L> {
    type Label = L;
}

impl<L: Clone> RandomAccessGraph for VecGraph<L> {
    type Successors<'a> = VecGraphIter<'a, L> where Self: 'a;

    #[inline(always)]
    fn num_arcs(&self) -> usize {
        self.number_of_arcs
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.succ[node].len()
    }

    #[inline(always)]
    fn successors(&self, node: usize) -> <Self as RandomAccessGraph>::Successors<'_> {
        VecGraphIter {
            iter: self.succ[node].iter(),
            label: unsafe {
                #[allow(clippy::uninit_assumed_init)]
                core::mem::MaybeUninit::uninit().assume_init()
            },
        }
    }
}

impl<L: Clone> SequentialGraph for VecGraph<L> {
    type Successors<'a> = VecGraphIter<'a, L>
    where L: 'a;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.succ.len()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        Some(self.num_arcs())
    }

    #[inline(always)]
    fn iter_nodes_from_inner<T>(&self, from: usize) -> T
    where
        T: LendingIterator,
        for<'c> <T as LendingIterator>::Item<'c>: Tuple2<_0 = usize>,
        for<'c> <<T as LendingIterator>::Item<'c> as Tuple2>::_1: Iterator<Item = usize>,
    {
        GraphIteratorImpl {
            graph: self,
            nodes: (from..self.num_nodes()),
        }
    }
}

pub struct VecGraphIter<'a, L: Clone> {
    label: L,
    iter: std::collections::btree_set::Iter<'a, DstWithLabel<L>>,
}

impl<'a, T: Clone> Iterator for VecGraphIter<'a, T> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let x = self.iter.next()?;
        self.label = x.1.clone();
        Some(x.0)
    }
}

impl<'a, L: Clone> Labelled for VecGraphIter<'a, L> {
    type Label = L;
}
/* TODO
impl<'a, T: Clone> LabelledIterator for VecGraphIter<'a, T> {
    fn label(&self) -> Self::Label {
        self.label.clone()
    }
}
*/
unsafe impl<'a, T: Clone> SortedSuccessors for VecGraphIter<'a, T> {}

impl<'a, T: Clone> ExactSizeIterator for VecGraphIter<'a, T> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

#[derive(Clone, Debug)]
struct DstWithLabel<L>(usize, L);

impl<L> PartialEq for DstWithLabel<L> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<L> Eq for DstWithLabel<L> {}

impl<L> PartialOrd for DstWithLabel<L> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<L> Ord for DstWithLabel<L> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
