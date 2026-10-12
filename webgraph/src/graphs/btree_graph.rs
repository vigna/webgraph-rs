/*
 * SPDX-FileCopyrightText: 2023-2025 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::graphs::{assert_nodes_exist, impl_into_lender, impl_seq_from_ra, impl_split};
use crate::prelude::*;

use lender::prelude::*;
use std::{collections::BTreeMap, iter::FusedIterator};

/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of
/// [`BTreeMap`].
///
/// This implementation is slower and uses more resources than a
/// [`LabeledVecGraph`], but it is more flexible as arcs can be added in any
/// order.
///
/// By setting the feature `serde`, this struct can be serialized and
/// deserialized using [serde].
///
/// [`LabeledVecGraph`]: crate::graphs::vec_graph::LabeledVecGraph
/// [serde]: https://crates.io/crates/serde
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledBTreeGraph<L: Clone + 'static = ()> {
    /// The number of arcs in the graph.
    num_arcs: u64,
    /// For each node, its list of successors.
    succ: Vec<BTreeMap<usize, L>>,
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
            num_arcs: 0,
            succ: vec![],
        }
    }

    /// Creates a new empty graph with `n` nodes.
    pub fn empty(n: usize) -> Self {
        Self {
            num_arcs: 0,
            succ: Vec::from_iter((0..n).map(|_| BTreeMap::new())),
        }
    }

    /// Adds an isolated node to the graph and returns true if it is a new node.
    ///
    /// If the node index is greater than the current number of nodes,
    /// the missing nodes will be added (as isolated nodes).
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| BTreeMap::new()));
        len <= node
    }

    /// Adds a labeled arc to the graph and returns whether it is a new one.
    ///
    /// # Panics
    ///
    /// Panics if either endpoint is not a node of the graph; use
    /// [`add_node`](Self::add_node) or the bulk methods to grow the graph.
    pub fn add_arc(&mut self, u: usize, v: usize, l: L) -> bool {
        assert_nodes_exist(self.succ.len(), u, v);
        let is_new_arc = self.succ[u].insert(v, l).is_none();
        self.num_arcs += is_new_arc as u64;
        is_new_arc
    }

    /// Removes an arc from the graph and returns whether it was present or not.
    ///
    /// # Panics
    ///
    /// Panics if either endpoint is not a node of the graph.
    pub fn remove_arc(&mut self, u: usize, v: usize) -> bool {
        assert_nodes_exist(self.succ.len(), u, v);
        let arc_existed = self.succ[u].remove(&v).is_some();
        self.num_arcs -= arc_existed as u64;
        arc_existed
    }

    /// Adds nodes and labeled successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
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
        self
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

    /// Adds labeled arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be labeled pairs of the form `((usize, usize), l)` specifying an
    /// arc and its label.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = ((usize, usize), L)>) {
        for ((u, v), l) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_arc(u, v, l);
        }
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be labeled pairs of the form `((usize, usize), l)` specifying an
    /// arc and its label.
    pub fn from_arcs(arcs: impl IntoIterator<Item = ((usize, usize), L)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Shrinks the capacity of the graph to fit its current size.
    ///
    /// # Implementation Notes
    ///
    /// This method just shrinks the capacity of the successor vector, as
    /// [`BTreeMap`] does not have a `shrink_to_fit` method.
    pub fn shrink_to_fit(&mut self) {
        self.succ.shrink_to_fit();
    }
}

impl_seq_from_ra!([L: Clone + 'static] LabeledBTreeGraph<L>, (usize, L), |g| g.succ.len());
impl_into_lender!([L: Clone + 'static] LabeledBTreeGraph<L>);

impl<L: Clone + 'static> LabeledSequentialGraph<L> for LabeledBTreeGraph<L> {}

impl<L: Clone + 'static> RandomAccessLabeling for LabeledBTreeGraph<L> {
    type Labels<'succ>
        = LabeledSucc<'succ, L>
    where
        L: 'succ;
    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.num_arcs
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.succ[node].len()
    }

    #[inline(always)]
    fn labels(&self, node: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        LabeledSucc(self.succ[node].iter())
    }
}

impl<L: Clone + 'static> LabeledRandomAccessGraph<L> for LabeledBTreeGraph<L> {}

impl_split!(ra [L: Clone + Sync] LabeledBTreeGraph<L> [] []);

/// A mutable [`RandomAccessGraph`] implementation based on a vector of
/// [`BTreeMap`].
///
/// This implementation is slower and uses more resources than a [`VecGraph`],
/// but it is more flexible as arcs can be added in any order.
///
/// By setting the feature `serde`, this struct can be serialized and
/// deserialized using [serde].
///
/// # Implementation Notes
///
/// This is just a newtype for a [`LabeledBTreeGraph`] with [`()`] labels.
/// All mutation methods are delegated.
///
/// [serde]: https://crates.io/crates/serde
/// [`()`]: https://doc.rust-lang.org/std/primitive.unit.html
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

    /// Adds an isolated node to the graph and returns true if it is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        self.0.add_node(node)
    }

    /// Adds an arc to the graph and returns whether it is a new one.
    ///
    /// # Panics
    ///
    /// Panics if either endpoint is not a node of the graph; use
    /// [`add_node`](Self::add_node) or the bulk methods to grow the graph.
    pub fn add_arc(&mut self, u: usize, v: usize) -> bool {
        self.0.add_arc(u, v, ())
    }

    /// Removes an arc from the graph and returns whether it was present or not.
    ///
    /// # Panics
    ///
    /// Panics if either endpoint is not a node of the graph.
    pub fn remove_arc(&mut self, u: usize, v: usize) -> bool {
        self.0.remove_arc(u, v)
    }

    /// Adds nodes and successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        self.0.add_lender(UnitLabelLender(iter_nodes.into_lender()));
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

    /// Adds arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying an arc.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        self.0.add_arcs(arcs.into_iter().map(|pair| (pair, ())));
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Shrinks the capacity of the graph to fit its current size.
    ///
    /// # Implementation Notes
    ///
    /// This method just shrinks the capacity of the successor vector, as
    /// [`BTreeMap`] does not have a `shrink_to_fit` method.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }
}

impl_into_lender!([] BTreeGraph);
impl_seq_from_ra!([] BTreeGraph, usize, |g| g.0.succ.len());

impl SequentialGraph for BTreeGraph {}

impl RandomAccessLabeling for BTreeGraph {
    type Labels<'succ> = Succ<'succ>;

    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.0.succ[node].len()
    }

    #[inline(always)]
    fn labels(&self, node: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        Succ(self.0.succ[node].keys().copied())
    }
}

impl RandomAccessGraph for BTreeGraph {}

impl From<LabeledBTreeGraph<()>> for BTreeGraph {
    fn from(g: LabeledBTreeGraph<()>) -> Self {
        BTreeGraph(g)
    }
}

#[doc(hidden)]
#[repr(transparent)]
pub struct LabeledSucc<'a, L: Clone + 'static>(std::collections::btree_map::Iter<'a, usize, L>);

// SAFETY: successors are stored in a BTreeMap, which iterates in sorted order.
unsafe impl<L: Clone + 'static> SortedIterator for LabeledSucc<'_, L> {}

impl<L: Clone + 'static> Iterator for LabeledSucc<'_, L> {
    type Item = (usize, L);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(succ, labels)| (*succ, labels.clone()))
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.len()
    }
}

impl<L: Clone + 'static> ExactSizeIterator for LabeledSucc<'_, L> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<L: Clone + 'static> FusedIterator for LabeledSucc<'_, L> {}

#[doc(hidden)]
#[repr(transparent)]
pub struct Succ<'succ>(core::iter::Copied<std::collections::btree_map::Keys<'succ, usize, ()>>);

// SAFETY: successors are stored in a BTreeMap, which iterates in sorted order.
unsafe impl SortedIterator for Succ<'_> {}

impl Iterator for Succ<'_> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.len()
    }
}

impl ExactSizeIterator for Succ<'_> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl FusedIterator for Succ<'_> {}

impl_split!(ra [] BTreeGraph [] []);

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_btree_graph() {
        crate::graphs::check_nan_labels(LabeledBTreeGraph::from_arcs);
    }
}
