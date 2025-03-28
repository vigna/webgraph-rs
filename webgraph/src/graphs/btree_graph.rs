/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

use lender::prelude::*;
use std::{collections::BTreeSet, mem::MaybeUninit};

/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of
/// [`BTreeSet`].
///
/// This implementation is slower and uses more resources than a
/// [`LabeledVecGraph`](crate::graphs::vec_graph::LabeledVecGraph),
/// but it is more flexible as arcs can be added in any order.
///
/// By setting the feature `serde`, this struct can be serialized and
/// deserialized using [serde](https://crates.io/crates/serde).
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
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

/// Manual implementation of [`PartialEq`]. This implementation is necessary
/// because the private struct [`Successor`] that we use to store in a
/// [`BTreeSet`] the tuple `(usize, Label)` implements [`PartialEq`] ignoring
/// the label so to enforce the absence of duplicate arcs. This implies that the
/// derived implementation of [`PartialEq`] would not check labels, so the same
/// graph with different labels would be equal, and this is not the intended
/// semantics.
impl<L: Clone + 'static + PartialEq> PartialEq for LabeledBTreeGraph<L> {
    fn eq(&self, other: &Self) -> bool {
        if self.number_of_arcs != other.number_of_arcs {
            return false;
        }
        if self.succ.len() != other.succ.len() {
            return false;
        }
        for (s, o) in self.succ.iter().zip(other.succ.iter()) {
            if s.len() != o.len() {
                return false;
            }
            let s_iter = s.iter().map(|x| (x.0, &x.1));
            let o_iter = o.iter().map(|x| (x.0, &x.1));
            for (v1, v2) in s_iter.zip(o_iter) {
                if v1 != v2 {
                    return false;
                }
            }
        }
        true
    }
}
impl<L: Clone + 'static + Eq> Eq for LabeledBTreeGraph<L> {}

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

    /// Add an isolated node to the graph and return true if it is a new node.
    ///
    /// # Panics
    ///
    /// This method will panic if one of the given nodes is greater or equal
    /// than the number of nodes in the graph.
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| BTreeSet::new()));
        len <= node
    }

    /// Add a labeled arc to the graph and return whether it is a new one.
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

    /// Add nodes and labeled successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
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

    /// Creates a new graph from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Add labeled arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying an
    /// arc and its label.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize, L)>) {
        for (u, v, l) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_arc(u, v, l);
        }
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying an
    /// arc and its label.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize, L)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Shrink the capacity of the graph to fit its current size.
    ///
    /// # Implementation Notes
    ///
    /// This method just shrinks the capacity of the successor vector, as
    /// [`BTreeSet`] does not have a `shrink_to_fit` method.
    pub fn shrink_to_fit(&mut self) {
        self.succ.shrink_to_fit();
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
/// This implementation is slower and uses more resources than a [`VecGraph`],
/// but it is more flexible as arcs can be added in any order.
///
/// By setting the feature `serde`, this struct can be serialized and
/// deserialized using [serde](https://crates.io/crates/serde).
///
/// # Implementation Notes
///
/// This is just a newtype for a [`LabeledBTreeGraph`] with
/// [`()`](https://doc.rust-lang.org/std/primitive.unit.html) labels. All
/// mutation methods are delegated.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BTreeGraph(LabeledBTreeGraph<()>);

impl BTreeGraph {
    /// Creates a new empty graph.
    pub fn new() -> Self {
        Self(LabeledBTreeGraph::new())
    }

    /// Creates a new empty graph with `n` nodes.
    pub fn empty(n: usize) -> Self {
        LabeledBTreeGraph::empty(n).into()
    }

    /// Add an isolated node to the graph and return true if it is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        self.0.add_node(node)
    }

    /// Add an arc to the graph and return whether it is a new one.
    pub fn add_arc(&mut self, u: usize, v: usize) -> bool {
        self.0.add_arc(u, v, ())
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        self.0.add_lender(UnitLender(iter_nodes.into_lender()));
        self
    }

    /// Creates a new graph from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Add arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying an arc.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        self.0.add_arcs(arcs.into_iter().map(|(u, v)| (u, v, ())));
    }

    /// Creates a new graph from  an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Shrink the capacity of the graph to fit its current size.
    ///
    /// # Implementation Notes
    ///
    /// This method just shrinks the capacity of the successor vector, as
    /// [`BTreeSet`] does not have a `shrink_to_fit` method.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_btree_graph() {
        let mut arcs = vec![
            (0, 1, Some(1.0)),
            (0, 2, None),
            (1, 2, Some(2.0)),
            (2, 4, Some(f64::INFINITY)),
            (3, 4, Some(f64::NEG_INFINITY)),
            (1, 3, Some(f64::NAN)),
        ];
        let g = LabeledBTreeGraph::<_>::from_arcs(arcs.iter().copied());
        assert_ne!(g, g, "The label contains a NaN which is not equal to itself so the graph must be not equal to itself");

        arcs.pop();
        let g = LabeledBTreeGraph::<_>::from_arcs(arcs);
        assert_eq!(g, g, "Without NaN the graph should be equal to itself");
    }
}
