/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

use lender::prelude::*;
/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of
/// vectors.
///
/// This implementation is faster and uses less resources than a
/// [`LabeledBTreeGraph`](crate::graphs::btree_graph::LabeledBTreeGraph), but it
/// is less flexible as arcs can be added only in increasing successor order.
///
/// By setting the feature `serde`, this struct can be serialized and
/// deserialized using [serde](https://crates.io/crates/serde).
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledVecGraph<L: Clone + 'static> {
    /// The number of arcs in the graph.
    number_of_arcs: u64,
    /// For each node, its list of successors.
    succ: Vec<Vec<(usize, L)>>,
}

impl<L: Clone + 'static> core::default::Default for LabeledVecGraph<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Clone + 'static> LabeledVecGraph<L> {
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
            succ: Vec::from_iter((0..n).map(|_| Vec::new())),
        }
    }

    /// Add an isolated node to the graph and return true if is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| Vec::new()));
        len <= node
    }

    /// Add an arc to the graph, the arcs of each node have to be inserted in
    /// increasing order.
    ///
    /// The label is not taken into account to check if the arc already exists.
    ///
    /// # Panics
    /// - If the given nodes are bigger or equal than the number of nodes in the graph.
    /// - If the inserted arc destination `v` is not bigger than all the sucessors
    ///      of the node `u`.
    pub fn add_arc(&mut self, u: usize, v: usize, l: L) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            panic!(
                "Node {} does not exist (the graph has {} nodes)",
                max,
                self.succ.len(),
            );
        }
        let succ = &mut self.succ[u];
        let biggest_dst = succ.last().map(|x| x.0).unwrap_or(0);
        match v.cmp(&biggest_dst) {
            // arcs have to be inserted in order
            core::cmp::Ordering::Less => panic!(
                "Error adding arc ({u}, {v}) as its insertion is not monotonic. The last arc inserted was ({u}, {})",
                biggest_dst,
            ),
            // no duplicated arcs
            core::cmp::Ordering::Equal => succ.last().is_none(),
            core::cmp::Ordering::Greater => {
                succ.push((v, l));
                self.number_of_arcs += 1;
                true
            }
        }
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut arcs = Vec::new();
        for_!( (node, succ) in iter_nodes {
            self.add_node(node);
            for (v, l) in succ {
                arcs.push((v, l));
                self.add_node(v);
            }
            arcs.sort_by_key(|x| x.0);
            for (v, l) in arcs.drain(..) {
                self.add_arc(node, v, l);
            }
        });
        self
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

    /// Add nodes and successors from an [`IntoLender`] yielding a sorted [`NodeLabelsLender`].
    pub fn add_sorted_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
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

    /// Creates a new graph from a sorted [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn from_sorted_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_sorted_lender(iter_nodes);
        g
    }

    /// Add arcs from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize, L)>) {
        let mut arcs = arcs.into_iter().collect::<Vec<_>>();
        arcs.sort_by_key(|x| (x.0, x.1));
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

    /// Add arcs from a sorted [`IntoIterator`] of paris of nodes.
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    pub fn add_sorted_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize, L)>) {
        for (u, v, l) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_arc(u, v, l);
        }
    }

    /// Creates a new graph from a sorted [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying
    /// an arc and its label.
    pub fn from_sorted_arcs(arcs: impl IntoIterator<Item = (usize, usize, L)>) -> Self {
        let mut g = Self::new();
        g.add_sorted_arcs(arcs);
        g
    }
}

impl<L: Clone + 'static> SequentialLabeling for LabeledVecGraph<L> {
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

impl<'a, L: Clone + 'static> IntoLender for &'a LabeledVecGraph<L> {
    type Lender = <LabeledVecGraph<L> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: Clone + 'static> LabeledSequentialGraph<L> for LabeledVecGraph<L> {}

impl<L: Clone + 'static> RandomAccessLabeling for LabeledVecGraph<L> {
    type Labels<'succ> = core::iter::Cloned<core::slice::Iter<'succ, (usize, L)>>;
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
        self.succ[node].iter().cloned()
    }
}

impl<L: Clone + 'static> LabeledRandomAccessGraph<L> for LabeledVecGraph<L> {}

/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of
/// vectors.
///
/// This implementation is faster and uses less resources than a
/// [`BTreeGraph`](crate::graphs::btree_graph::BTreeGraph), but it
/// is less flexible as arcs can be added only in increasing successor order.
///
/// By setting the feature `serde`, this struct can be serialized and
/// deserialized using [serde](https://crates.io/crates/serde).
///
/// # Implementation Notes
///
/// This is just a newtype for a [`LabeledVecGraph`] with
/// [`()`](https://doc.rust-lang.org/std/primitive.unit.html) labels.
/// All mutation methods are delegated.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VecGraph(LabeledVecGraph<()>);

impl VecGraph {
    /// Creates a new empty graph.
    pub fn new() -> Self {
        Self(LabeledVecGraph::new())
    }

    /// Creates a new empty graph with `n` nodes.
    pub fn empty(n: usize) -> Self {
        Self(LabeledVecGraph::empty(n))
    }

    /// Add an isolated node to the graph and return true if is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        self.0.add_node(node)
    }

    /// Add an arc to the graph, the arcs of each node have to be inserted in
    /// increasing order.
    ///
    /// # Panics
    /// - If the given nodes are bigger or equal than the number of nodes in the graph.
    /// - If the inserted arc destination `v` is not bigger than all the sucessors
    ///      of the node `u`.
    pub fn add_arc(&mut self, u: usize, v: usize) -> bool {
        self.0.add_arc(u, v, ())
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        self.0.add_lender(UnitLender(iter_nodes.into_lender()));
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

    /// Add nodes and successors from an [`IntoLender`] yielding a sorted [`NodeLabelsLender`].
    pub fn add_sorted_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        self.0
            .add_sorted_lender(UnitLender(iter_nodes.into_lender()));
        self
    }

    /// Creates a new graph from a sorted [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn from_sorted_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        let mut g = Self::new();
        g.add_sorted_lender(iter_nodes);
        g
    }

    /// Add arcs from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        self.0.add_arcs(arcs.into_iter().map(|(u, v)| (u, v, ())));
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying
    /// an arc and its label.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Add arcs from a sorted [`IntoIterator`] of paris of nodes.
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    pub fn add_sorted_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        self.0
            .add_sorted_arcs(arcs.into_iter().map(|(u, v)| (u, v, ())));
    }

    /// Creates a new graph from a sorted [`IntoIterator`].
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying
    /// an arc and its label.
    pub fn from_sorted_arcs(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_sorted_arcs(arcs);
        g
    }
}

impl<'a> IntoLender for &'a VecGraph {
    type Lender = <VecGraph as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl SequentialLabeling for VecGraph {
    type Label = usize;
    type Lender<'a>
        = IteratorImpl<'a, Self>
    where
        Self: 'a;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        self.0.num_arcs_hint()
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        IteratorImpl {
            labeling: self,
            nodes: (from..self.num_nodes()),
        }
    }
}

impl SequentialGraph for VecGraph {}

impl RandomAccessLabeling for VecGraph {
    type Labels<'succ> = SortedIter<
        core::iter::Map<
            core::iter::Copied<core::slice::Iter<'succ, (usize, ())>>,
            fn((usize, ())) -> usize,
        >,
    >;
    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.0.outdegree(node)
    }

    #[inline(always)]
    fn labels(&self, node: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        // this is safe as we mantain each vector of successors sorted
        unsafe { SortedIter::new(self.0.succ[node].iter().copied().map(|(x, _)| x)) }
    }
}

impl RandomAccessGraph for VecGraph {}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_vec_graph() {
        let mut arcs = vec![
            (0, 1, Some(1.0)),
            (0, 2, None),
            (1, 2, Some(2.0)),
            (2, 4, Some(f64::INFINITY)),
            (3, 4, Some(f64::NEG_INFINITY)),
            (1, 3, Some(f64::NAN)),
        ];
        let g = LabeledVecGraph::<_>::from_arcs(arcs.iter().copied());
        assert_ne!(g, g, "The label contains a NaN which is not equal to itself so the graph must be not equal to itself");

        arcs.pop();
        let g = LabeledVecGraph::<_>::from_arcs(arcs);
        assert_eq!(g, g, "Without NaN the graph should be equal to itself");
    }
}
