/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use epserde::Epserde;
use lender::prelude::*;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Epserde, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LabelledArc<L>(usize, L);

impl<L> From<(usize, L)> for LabelledArc<L> {
    fn from((v, l): (usize, L)) -> Self {
        Self(v, l)
    }
}

impl<L> From<LabelledArc<L>> for (usize, L) {
    fn from(value: LabelledArc<L>) -> (usize, L) {
        (value.0, value.1)
    }
}

/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of
/// vectors.
///
/// This implementation is faster and uses less resources than a
/// [`LabeledBTreeGraph`](crate::graphs::btree_graph::LabeledBTreeGraph), but it
/// is less flexible as arcs can be added only in increasing successor order.
///
/// This struct can be serialized with
/// [ε-serde](https://crates.io/crates/epserde). By setting the feature `serde`,
/// this struct can be serialized using [serde](https://crates.io/crates/serde),
/// too.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Epserde, Clone, Debug, PartialEq, Eq)]
pub struct LabeledVecGraph<L: Clone + 'static> {
    /// The number of arcs in the graph.
    number_of_arcs: u64,
    /// For each node, its list of successors.
    succ: Vec<Vec<LabelledArc<L>>>,
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

    /// Add an isolated node to the graph and return true if it is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| Vec::new()));
        len <= node
    }

    /// Add an arc to the graph.
    ///
    /// New arcs must be added in increasing successor order, or this method
    /// will panic.
    ///
    /// # Panics
    ///
    /// This method will panic:
    /// - if one of the given nodes is greater or equal than the number of nodes
    ///   in the graph;
    /// - if the successor is lesser than or equal to the current last successor
    ///   of the source node.
    pub fn add_arc(&mut self, u: usize, v: usize, l: L) {
        let max = u.max(v);
        if max >= self.succ.len() {
            panic!(
                "Node {} does not exist (the graph has {} nodes)",
                max,
                self.succ.len(),
            );
        }
        let succ = &mut self.succ[u];

        match succ.last() {
            None => {
                succ.push((v, l).into());
                self.number_of_arcs += 1;
            }
            Some(LabelledArc(last, _label)) => {
                if v <= *last {
                    // arcs have to be inserted in increasing successor order
                    panic!(
                        "Error adding arc ({u}, {v}): successor is not increasing; the last arc inserted was ({u}, {last})"
                    );
                }
                succ.push((v, l).into());
                self.number_of_arcs += 1;
            }
        }
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// If the lender is sorted, consider using
    /// [`add_sorted_lender`](Self::add_sorted_lender), as it does not need to
    /// sort the output of the lender.
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

    /// Creates a new graph from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// If the lender is sorted, consider using
    /// [`from_sorted_lender`](Self::from_sorted_lender), as it does not need to
    /// sort the output of the lender.
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a sorted
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`add_lender`](Self::add_lender) as
    /// it does not need to sort the output of the lender.
    pub fn add_sorted_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator,
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

    /// Creates a new graph from a sorted [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`from_lender`](Self::from_lender) as
    /// it does not need to sort the output of the lender.
    pub fn from_sorted_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator,
    {
        let mut g = Self::new();
        g.add_sorted_lender(iter_nodes);
        g
    }

    /// Add labeled arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be triples of the form `(usize, usize, l)` specifying an
    /// arc and its label.
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
    /// The items must be triples of the form `(usize, usize, l)` specifying an
    /// arc and its label.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize, L)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Shrink the capacity of the graph to fit its current size.
    pub fn shrink_to_fit(&mut self) {
        self.succ.shrink_to_fit();
        for s in self.succ.iter_mut() {
            s.shrink_to_fit();
        }
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
    type Labels<'succ> = core::iter::Map<
        core::iter::Cloned<core::slice::Iter<'succ, LabelledArc<L>>>,
        fn(LabelledArc<L>) -> (usize, L),
    >;
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
        self.succ[node].iter().cloned().map(Into::into)
    }
}

impl<L: Clone + 'static> LabeledRandomAccessGraph<L> for LabeledVecGraph<L> {}

/// A mutable [`RandomAccessGraph`] implementation based on a vector of
/// vectors.
///
/// This implementation is faster and uses less resources than a [`BTreeGraph`],
/// but it is less flexible as arcs can be added only in increasing successor
/// order.
///
/// This struct can be serialized with
/// [ε-serde](https://crates.io/crates/epserde). By setting the feature `serde`,
/// this struct can be serialized using [serde](https://crates.io/crates/serde),
/// too.
///
/// # Implementation Notes
///
/// This is just a newtype for a [`LabeledVecGraph`] with
/// [`()`](https://doc.rust-lang.org/std/primitive.unit.html) labels. All
/// mutation methods are delegated.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Epserde, Clone, Debug, Default, PartialEq, Eq)]
pub struct VecGraph(LabeledVecGraph<()>);

impl VecGraph {
    /// Creates a new empty graph.
    pub fn new() -> Self {
        LabeledVecGraph::new().into()
    }

    /// Creates a new empty graph with `n` nodes.
    pub fn empty(n: usize) -> Self {
        LabeledVecGraph::empty(n).into()
    }

    /// Add an isolated node to the graph and return true if it is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        self.0.add_node(node)
    }

    /// Add an arc to the graph.
    ///
    /// New arcs must be added in increasing successor order, or this method
    /// will panic.
    ///
    /// # Panics
    ///
    /// This method will panic:
    /// - if one of the given nodes is greater or equal than the number of nodes
    ///   in the graph;
    /// - if the successor is lesser than or equal to the current last successor
    ///   of the source node.
    pub fn add_arc(&mut self, u: usize, v: usize) {
        self.0.add_arc(u, v, ())
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// If the lender is sorted, consider using
    /// [`add_sorted_lender`](Self::add_sorted_lender), as it does not need to
    /// sort the output of the lender.
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        self.0.add_lender(UnitLender(iter_nodes.into_lender()));
        self
    }

    /// Creates a new graph from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// If the lender is sorted, consider using
    /// [`from_sorted_lender`](Self::from_sorted_lender), as it does not need to
    /// sort the output of the lender.
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a sorted
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`add_lender`](Self::add_lender) as
    /// it does not need to sort the output of the lender.
    pub fn add_sorted_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator,
    {
        self.0
            .add_sorted_lender(UnitLender(iter_nodes.into_lender()));
        self
    }

    /// Creates a new graph from a sorted [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`from_lender`](Self::from_lender) as
    /// it does not need to sort the output of the lender.
    pub fn from_sorted_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator,
    {
        let mut g = Self::new();
        g.add_sorted_lender(iter_nodes);
        g
    }

    /// Add arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying an arc.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        self.0.add_arcs(arcs.into_iter().map(|(u, v)| (u, v, ())));
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying an arc.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Shrink the capacity of the graph to fit its current size.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
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
            core::iter::Copied<core::slice::Iter<'succ, LabelledArc<()>>>,
            fn(LabelledArc<()>) -> usize,
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
        // this is safe as we maintain each vector of successors sorted
        unsafe { SortedIter::new(self.0.succ[node].iter().copied().map(|LabelledArc(x, _)| x)) }
    }
}

impl RandomAccessGraph for VecGraph {}

impl From<LabeledVecGraph<()>> for VecGraph {
    fn from(g: LabeledVecGraph<()>) -> Self {
        VecGraph(g)
    }
}

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
        assert_ne!(
            g, g,
            "The label contains a NaN which is not equal to itself so the graph must be not equal to itself"
        );

        arcs.pop();
        let g = LabeledVecGraph::<_>::from_arcs(arcs);
        assert_eq!(g, g, "Without NaN the graph should be equal to itself");
    }
}
