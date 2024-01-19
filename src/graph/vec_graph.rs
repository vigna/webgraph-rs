/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

use lender::prelude::*;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
struct Successor<L: Copy + 'static>(usize, L);

impl<L: Copy + 'static> PartialEq for Successor<L> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<L: Copy + 'static> Eq for Successor<L> {}

impl<L: Copy + 'static> PartialOrd for Successor<L> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl<L: Copy + 'static> Ord for Successor<L> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

/// A vector-based mutable [`Graph`]/[`LabeledGraph`] implementation.
///
/// Successors are represented using a [`BTreeSet`]. Choosing `()`
/// as the label type will result in a [`Graph`] implementation.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VecGraph<L: Copy + 'static = ()> {
    /// The number of arcs in the graph.
    number_of_arcs: usize,
    /// For each node, its list of successors.
    succ: Vec<BTreeSet<Successor<L>>>,
}

impl<L: Copy + 'static> core::default::Default for VecGraph<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Copy + 'static> VecGraph<L> {
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

    /// Add a node to the graph without successors and return if it was a new
    /// one or not.
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| BTreeSet::new()));
        len <= node
    }

    /// Add an arc to the graph and return if it was a new one or not.
    /// `true` => already exist, `false` => new arc.
    pub fn add_labelled_arc(&mut self, u: usize, v: usize, l: L) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            self.succ.resize(max + 1, BTreeSet::new());
        }
        let result = self.succ[u].insert(Successor(v, l));
        self.number_of_arcs += result as usize;
        result
    }

    /// Remove an arc from the graph and return if it was present or not.
    /// Return Nones if the either nodes (`u` or `v`) do not exist.
    pub fn remove_labelled_arc(&mut self, u: usize, v: usize, l: L) -> Option<bool> {
        if u >= self.succ.len() || v >= self.succ.len() {
            return None;
        }
        let result = self.succ[u].remove(&Successor(v, l));
        self.number_of_arcs -= result as usize;
        Some(result)
    }

    /// Add the nodes and sucessors from the `iter_nodes` iterator of a graph
    pub fn add_labelled_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabels<'next, Label = (usize, L)>,
    {
        for_!( (node, succ) in iter_nodes {
            self.add_node(node);
            for (v, l) in succ {
                self.add_labelled_arc(node, v, l);
            }
        });
        self
    }

    /// Convert the `iter_nodes` iterator of a graph into a [`VecGraph`].
    pub fn from_labelled_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabels<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_labelled_lender(iter_nodes);
        g
    }

    /// Add an arc to the graph and return a reference to self to allow a
    /// builder-like usage.
    pub fn add_labelled_arc_list(
        &mut self,
        arcs: impl IntoIterator<Item = (usize, usize, L)>,
    ) -> &mut Self {
        for (u, v, l) in arcs {
            self.add_labelled_arc(u, v, l);
        }
        self
    }

    /// Convert a COO arc list into a graph by sorting and deduplicating.
    pub fn from_labelled_arc_list(arcs: impl IntoIterator<Item = (usize, usize, L)>) -> Self {
        let mut g = Self::new();
        g.add_labelled_arc_list(arcs);
        g
    }
}

impl VecGraph<()> {
    /// Add an arc to the graph and return if it was a new one or not.
    /// `true` => already exist, `false` => new arc.
    pub fn add_arc(&mut self, u: usize, v: usize) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            self.succ.resize(max + 1, BTreeSet::new());
        }
        let result = self.succ[u].insert(Successor(v, ()));
        self.number_of_arcs += result as usize;
        result
    }

    /// Remove an arc from the graph and return if it was present or not.
    /// Return Nones if the either nodes (`u` or `v`) do not exist.
    pub fn remove_arc(&mut self, u: usize, v: usize) -> Option<bool> {
        if u >= self.succ.len() || v >= self.succ.len() {
            return None;
        }
        let result = self.succ[u].remove(&Successor(v, ()));
        self.number_of_arcs -= result as usize;
        Some(result)
    }

    /// Add the nodes and sucessors from the `iter_nodes` iterator of a graph
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabels<'next, Label = usize>,
    {
        for_!( (node, succ) in iter_nodes {
            self.add_node(node);
            for v in succ {
                self.add_arc(node, v);
            }
        });
        self
    }

    /// Convert the `iter_nodes` iterator of a graph into a [`VecGraph`].
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabels<'next, Label = usize>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
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

    /// Convert a COO arc list into a graph by sorting and deduplicating.
    pub fn from_arc_list(arcs: &[(usize, usize)]) -> Self {
        let mut g = Self::new();
        g.add_arc_list(arcs);
        g
    }
}

impl<'a, L: Copy + 'static> IntoLender for &'a VecGraph<L> {
    type Lender = <VecGraph<L> as SequentialLabelling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: Copy + 'static> SequentialLabelling for VecGraph<L> {
    type Label = (usize, L);
    type Iterator<'a> = IteratorImpl<'a, Self> where Self: 'a;

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

impl<L: Copy + 'static> LabelledSequentialGraph<L> for VecGraph<L> {}

impl<L: Copy + 'static> RandomAccessLabelling for VecGraph<L> {
    type Successors<'succ> = Successors<'succ, L> where L: 'succ;
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

impl<L: Copy + 'static> LabelledRandomAccessGraph<L> for VecGraph<L> {}

#[repr(transparent)]
pub struct Successors<'a, L: Copy + 'static>(std::collections::btree_set::Iter<'a, Successor<L>>);

impl<'a, L: Copy + 'static> Iterator for Successors<'a, L> {
    type Item = (usize, L);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().copied().map(|x| (x.0, x.1))
    }
}

unsafe impl<'a, L: Copy + 'static> SortedSuccessors for Successors<'a, L> {}

impl<'a, L: Copy + 'static> ExactSizeIterator for Successors<'a, L> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}
