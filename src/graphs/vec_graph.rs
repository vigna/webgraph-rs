/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

use lender::prelude::*;
use std::iter::Iterator;
use std::{collections::BTreeSet, mem::MaybeUninit};

#[doc(hidden)]
/// A struct containing a successor.
///
/// By implementing equality and order on the first coordinate only, we
/// can store the successors of a node and their labels as a
/// [`BTreeSet`] of pairs `(usize, L)`.
#[derive(Clone, Copy, Debug)]
struct Successor<L: Clone + 'static>(usize, L);

impl<L: Clone + 'static> PartialEq for Successor<L> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<L: Clone + 'static> Eq for Successor<L> {}

impl<L: Clone + 'static> PartialOrd for Successor<L> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl<L: Clone + 'static> Ord for Successor<L> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of [`BTreeSet`].
///
/// Choosing [`()`](https://doc.rust-lang.org/std/primitive.unit.html)
/// as the label type will result in a [`RandomAccessGraph`] implementation.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VecGraph<L: Clone + 'static = ()> {
    /// The number of arcs in the graph.
    number_of_arcs: u64,
    /// For each node, its list of successors.
    succ: Vec<BTreeSet<Successor<L>>>,
}

impl<L: Clone + 'static> core::default::Default for VecGraph<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Clone + 'static> VecGraph<L> {
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

    /// Add an isolated node to the graph and return true if is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| BTreeSet::new()));
        len <= node
    }

    /// Add an arc to the graph and return whether it is a new one.
    pub fn add_labeled_arc(&mut self, u: usize, v: usize, l: L) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            panic!(
                "Node {} does not exist (the graph has {} nodes)",
                max,
                self.succ.len(),
            );
        }
        let result = self.succ[u].insert(Successor(v, l));
        self.number_of_arcs += result as u64;
        result
    }

    /// Remove an arc from the graph and return whether it was present or not.
    pub fn remove_arc(&mut self, u: usize, v: usize) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            panic!(
                "Node {} does not exist (the graph has {} nodes)",
                max,
                self.succ.len(),
            );
        }
        // SAFETY: the label is not used by Eq/Ord.
        let result = self.succ[u].remove(&Successor(v, unsafe {
            #[allow(clippy::uninit_assumed_init)]
            MaybeUninit::<L>::uninit().assume_init()
        }));
        self.number_of_arcs -= result as u64;
        result
    }

    /// Add nodes and labeled successors from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn add_labeled_lender<I: IntoLender>(&mut self, iter_nodes: I)
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        for_!( (node, succ) in iter_nodes {
            self.add_node(node);
            for (v, l) in succ {
                self.add_node(v);
                self.add_labeled_arc(node, v, l);
            }
        });
    }

    /// Creates a new graph from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn from_labeled_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_labeled_lender(iter_nodes);
        g
    }

    /// Add labeled arcs from an [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying
    /// an arc and its label.
    ///
    /// Note that new nodes will be added as needed.
    pub fn add_labeled_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize, L)>) {
        for (u, v, l) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_labeled_arc(u, v, l);
        }
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying
    /// an arc and its label.
    pub fn from_labeled_arc_list(arcs: impl IntoIterator<Item = (usize, usize, L)>) -> Self {
        let mut g = Self::new();
        g.add_labeled_arcs(arcs);
        g
    }
}

impl VecGraph<()> {
    /// Add an arc to the graph and return whether it is a new one.
    pub fn add_arc(&mut self, u: usize, v: usize) -> bool {
        self.add_labeled_arc(u, v, ())
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        for_!( (node, succ) in iter_nodes {
            self.add_node(node);
            for v in succ {
                self.add_node(v);
                self.add_arc(node, v);
            }
        });
        self
    }

    /// Creates a new graph from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Add arcs from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    pub fn add_arc_list(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        for (u, v) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_arc(u, v);
        }
    }

    /// Creates a new graph from  an [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize)` specifying
    /// an arc.
    pub fn from_arc_list(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_arc_list(arcs);
        g
    }
}

impl<'a, L: Clone + 'static> IntoLender for &'a VecGraph<L> {
    type Lender = <VecGraph<L> as SequentialLabeling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: Clone + 'static> SequentialLabeling for VecGraph<L> {
    type Label = (usize, L);
    type Iterator<'a> = IteratorImpl<'a, Self> where Self: 'a;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.succ.len()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(self.num_arcs())
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        IteratorImpl {
            labeling: self,
            nodes: (from..self.num_nodes()),
        }
    }
}

impl<L: Clone + 'static> LabeledSequentialGraph<L> for VecGraph<L> {}

impl<L: Clone + 'static> RandomAccessLabeling for VecGraph<L> {
    type Labels<'succ> = Successors<'succ, L> where L: 'succ;
    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.number_of_arcs
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.succ[node].len()
    }

    #[inline(always)]
    fn labels(&self, node: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        Successors(self.succ[node].iter())
    }
}

impl<L: Clone + 'static> LabeledRandomAccessGraph<L> for VecGraph<L> {}

#[doc(hidden)]
#[repr(transparent)]
pub struct Successors<'a, L: Clone + 'static>(std::collections::btree_set::Iter<'a, Successor<L>>);

impl<'a, L: Clone + 'static> Iterator for Successors<'a, L> {
    type Item = (usize, L);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().cloned().map(|x| (x.0, x.1))
    }
}

unsafe impl<'a, L: Clone + 'static> SortedLabels for Successors<'a, L> {}

impl<'a, L: Clone + 'static> ExactSizeIterator for Successors<'a, L> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[test]
fn test_remove() {
    let mut g = VecGraph::<_>::from_labeled_arc_list([(0, 1, 1), (0, 2, 2), (1, 2, 3)]);
    assert!(g.remove_arc(0, 2));
    assert!(!g.remove_arc(0, 2));
}
