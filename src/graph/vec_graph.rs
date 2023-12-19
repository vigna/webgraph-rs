/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{for_iter, prelude::*};
use alloc::collections::BTreeSet;
use lender::*;

/// A vector-based mutable [`Graph`]/[`LabeledGraph`] implementation.
///
/// Successors are represented using a [`BTreeSet`]. Choosing `()`
/// as the label type will result in a [`Graph`] implementation.

#[derive(Clone, Debug, PartialEq, Eq)]
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

    /// Remove an arc from the graph and return if it was present or not.
    /// Return Nones if the either nodes (`u` or `v`) do not exist.
    pub fn remove_labeled_arc(&mut self, u: usize, v: usize) -> Option<bool> {
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
    /*
    /// Convert the `iter_nodes` iterator of a graph into a [`VecGraph`].
    pub fn from_labeled_node_iter<I>(iter_nodes: I) -> Self
    where
        I: IntoLender,
        for<'next> Lend<'next, I::Lender>: Tuple2<_0 = usize>,
        for<'next> <Lend<'next, I::Lender> as Tuple2>::_1: IntoIterator<Item = (usize, L)>,
        for<'next> <Lend<'next, I::Lender> as Tuple2>::_1: Labeled<Label = L>,
    {
        let mut g = Self::new();
        g.add_labeled_node_iter(iter_nodes);
        g
    }

    /// Add the nodes and sucessors from the `iter_nodes` iterator of a graph
    pub fn add_labeled_node_iter<I>(&mut self, iter_nodes: I) -> &mut Self
    where
        I: IntoLender,
        for<'next> Lend<'next, I::Lender>: Tuple2<_0 = usize>,
        for<'next> <Lend<'next, I::Lender> as Tuple2>::_1:
            IntoIterator<Item = usize> + LabeledSuccessors,
        for<'next> <Lend<'next, I::Lender> as Tuple2>::_1: LabeledSuccessors<Label = L>,
    {
        for_iter! { (node, succ) in iter_nodes =>
            self.add_node(node);
            for (v, l) in succ.labeled() {
                self.add_arc_with_label(node, v, l);
            }
        }
        self
    }

    /// Add the nodes, arcs, and labels in a graph to a [`VecGraph`].
    pub fn add_labeled_graph<S: LabeledSequentialGraph<Label = L>>(
        &mut self,
        graph: &S,
    ) -> &mut Self
    where
        for<'next> <S as SequentialGraph>::Successors<'next>: LabeledSuccessors<Label = L>,
    {
        self.add_labeled_node_iter::<<S as SequentialGraph>::Iterator<'_>>(graph.iter())
    }*/
}

impl VecGraph {
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

    /// Convert an iterator on nodes and successors in a [`VecGraph`].
    pub fn from_node_iter<L>(iter_nodes: L) -> Self
    where
        L: IntoLender + for<'next> NodeLabelsLending<'next, Item = usize>,
        for<'next> Lend<'next, L::Lender>: Tuple2<_0 = usize>,
        for<'next> <Lend<'next, L::Lender> as Tuple2>::_1: IntoIterator<Item = usize>,
    {
        let x = iter_nodes.into_lender().next().unwrap();

        let mut g = Self::new();
        g.add_node_iter(iter_nodes);
        g
    }

    /// Add the nodes and successors from an iterator to a [`VecGraph`].
    pub fn add_node_iter<L>(&mut self, iter_nodes: L) -> &mut Self
    where
        L: IntoLender + for<'next> NodeLabelsLending<'next, Item = usize>,
        for<'next> Lend<'next, L::Lender>: Tuple2<_0 = usize>,
        for<'next> <Lend<'next, L::Lender> as Tuple2>::_1: IntoIterator<Item = usize>,
    {
        for_iter! { (node, succ) in iter_nodes =>
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
}

/*impl<'lend, 'a> Lending<'lend> for &'a VecGraph<()> {
    type Lend = (usize, Successors<'lend, ()>);
}*/

impl<'a> IntoLender for &'a VecGraph {
    type Lender = <VecGraph as SequentialLabelling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}
/*
impl
 Labeled for VecGraph {
    type Label = L;
}
*/

impl RandomAccessLabelling for VecGraph {
    type Successors<'a> = Successors<'a> where Self: 'a;

    #[inline(always)]
    fn num_arcs(&self) -> usize {
        self.number_of_arcs
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.succ[node].len()
    }

    #[inline(always)]
    fn successors(&self, node: usize) -> <Self as RandomAccessLabelling>::Successors<'_> {
        Successors(self.succ[node].iter())
    }
}

impl RandomAccessGraph for VecGraph {}

impl SequentialLabelling for VecGraph {
    type Label = usize;
    type Iterator<'a> = IteratorImpl<'a, Self>;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.succ.len()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        Some(self.num_arcs())
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        IteratorImpl {
            labelling: self,
            nodes: (from..self.num_nodes()),
        }
    }
}

impl SequentialGraph for VecGraph {}

#[repr(transparent)]
pub struct Successors<'a>(std::collections::btree_set::Iter<'a, usize>);

impl<'a> Iterator for Successors<'a> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().copied()
    }
}

unsafe impl<'a> SortedSuccessors for Successors<'a> {}

impl<'a> ExactSizeIterator for Successors<'a> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}
