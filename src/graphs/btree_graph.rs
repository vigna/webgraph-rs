/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

use lender::prelude::*;
use std::{collections::BTreeSet, mem::MaybeUninit};

/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of [`BTreeSet`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledBTreeGraph<L: Clone + 'static = ()> {
    /// The number of arcs in the graph.
    number_of_arcs: u64,
    /// For each node, its list of successors.
    succ: Vec<BTreeSet<Successor<L>>>,
}

impl<L: Clone + 'static> core::default::Default for LabeledBTreeGraph<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Clone + 'static> LabeledBTreeGraph<L> {
    /// Creates a new empty graph.
    pub fn new() -> Self {
        Self {
            number_of_arcs: 0,
            succ: vec![],
        }
    }

    /// Creates a new empty graph with `n` nodes.
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
    pub fn add_arc(&mut self, u: usize, v: usize, l: L) -> bool {
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
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I)
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        for_!( (node, succ) in iter_nodes {
            self.add_node(node);
            for (v, l) in succ {
                self.add_node(v);
                self.add_arc(node, v, l);
            }
        });
    }

    /// Creates a new graph from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Add labeled arcs from an [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying
    /// an arc and its label.
    ///
    /// Note that new nodes will be added as needed.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize, L)>) {
        for (u, v, l) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_arc(u, v, l);
        }
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying
    /// an arc and its label.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize, L)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }
}

impl<L: Clone + 'static> SequentialLabeling for LabeledBTreeGraph<L> {
    type Label = (usize, L);
    type Lender<'a>
        = IteratorImpl<'a, Self>
    where
        Self: 'a;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.succ.len()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(self.num_arcs())
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        IteratorImpl {
            labeling: self,
            nodes: (from..self.num_nodes()),
        }
    }
}

impl<'a, L: Clone + 'static> IntoLender for &'a LabeledBTreeGraph<L> {
    type Lender = <LabeledBTreeGraph<L> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: Clone + 'static> LabeledSequentialGraph<L> for LabeledBTreeGraph<L> {}

impl<L: Clone + 'static> RandomAccessLabeling for LabeledBTreeGraph<L> {
    type Labels<'succ>
        = Successors<'succ, L>
    where
        L: 'succ;
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

impl<L: Clone + 'static> LabeledRandomAccessGraph<L> for LabeledBTreeGraph<L> {}

/// A mutable [`RandomAccessGraph`] implementation based on a vector of
/// [`BTreeSet`].
///
/// # Implementation Notes
///
/// This is just a newtype for a [`LabeledBTreeGraph`] with
/// [`()`](https://doc.rust-lang.org/std/primitive.unit.html) labels.
/// All mutation methods are delegated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BTreeGraph(LabeledBTreeGraph<()>);

impl core::default::Default for BTreeGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeGraph {
    /// Creates a new empty graph.
    pub fn new() -> Self {
        Self(LabeledBTreeGraph::new())
    }

    /// Creates a new empty graph with `n` nodes.
    pub fn empty(n: usize) -> Self {
        LabeledBTreeGraph::empty(n).into()
    }

    /// Add an isolated node to the graph and return true if is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        self.0.add_node(node)
    }

    /// Add an arc to the graph and return whether it is a new one.
    fn add_arc(&mut self, u: usize, v: usize) -> bool {
        self.0.add_arc(u, v, ())
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        self.0.add_lender(UnitLender(iter_nodes.into_lender()));
        self
    }

    /// Add arcs from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    fn add_arc_list(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        self.0.add_arcs(arcs.into_iter().map(|(u, v)| (u, v, ())));
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

    /// Creates a new graph from  an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_arc_list(arcs);
        g
    }
}

impl<'a> IntoLender for &'a BTreeGraph {
    type Lender = <BTreeGraph as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl SequentialLabeling for BTreeGraph {
    type Label = usize;
    type Lender<'a>
        = IteratorImpl<'a, Self>
    where
        Self: 'a;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.0.succ.len()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(self.num_arcs())
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        IteratorImpl {
            labeling: self,
            nodes: (from..self.num_nodes()),
        }
    }
}

impl SequentialGraph for BTreeGraph {}

impl RandomAccessLabeling for BTreeGraph {
    type Labels<'succ> = std::iter::Map<
        std::collections::btree_set::Iter<'succ, Successor<()>>,
        fn(&Successor<()>) -> usize,
    >;

    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.0.number_of_arcs
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.0.succ[node].len()
    }

    #[inline(always)]
    fn labels(&self, node: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        self.0.succ[node].iter().map(|x| x.0)
    }
}

impl RandomAccessGraph for BTreeGraph {}

impl From<LabeledBTreeGraph<()>> for BTreeGraph {
    fn from(g: LabeledBTreeGraph<()>) -> Self {
        BTreeGraph(g)
    }
}

#[doc(hidden)]
/// A struct containing a successor.
///
/// By implementing equality and order on the first coordinate only, we
/// can store the successors of a node and their labels as a
/// [`BTreeSet`] of pairs `(usize, L)`.
#[derive(Clone, Copy, Debug)]
pub struct Successor<L: Clone + 'static>(usize, L);

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

#[doc(hidden)]
#[repr(transparent)]
pub struct Successors<'a, L: Clone + 'static>(std::collections::btree_set::Iter<'a, Successor<L>>);

unsafe impl<L: Clone + 'static> SortedIterator for Successors<'_, L> {}

impl<L: Clone + 'static> Iterator for Successors<'_, L> {
    type Item = (usize, L);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().cloned().map(|x| (x.0, x.1))
    }
}

impl<L: Clone + 'static> ExactSizeIterator for Successors<'_, L> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[test]
fn test_remove() {
    let mut g = LabeledBTreeGraph::<_>::from_arcs([(0, 1, 1), (0, 2, 2), (1, 2, 3)]);
    assert!(g.remove_arc(0, 2));
    assert!(!g.remove_arc(0, 2));
}
