/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::graphs::{assert_nodes_exist, impl_into_lender, impl_seq_from_ra, impl_split};
use crate::prelude::*;
use epserde::Epserde;
use lender::prelude::*;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Epserde, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[epserde(deep_copy)]
/// An arc with a label, stored as a pair (target, label).
pub struct LabeledArc<L>(usize, L);

impl<L> From<(usize, L)> for LabeledArc<L> {
    fn from((v, l): (usize, L)) -> Self {
        Self(v, l)
    }
}

impl<L> From<LabeledArc<L>> for (usize, L) {
    fn from(value: LabeledArc<L>) -> (usize, L) {
        (value.0, value.1)
    }
}

/// A mutable [`LabeledRandomAccessGraph`] implementation based on a vector of
/// vectors.
///
/// This implementation is faster and uses less resources than a
/// [`LabeledBTreeGraph`], but it is less flexible as arcs can be added only
/// in increasing successor order.
///
/// This struct can be serialized with [ε-serde]. By setting the feature
/// `serde`, this struct can be serialized using [serde], too.
///
/// [`LabeledBTreeGraph`]: crate::graphs::btree_graph::LabeledBTreeGraph
/// [ε-serde]: https://crates.io/crates/epserde
/// [serde]: https://crates.io/crates/serde
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Epserde, Clone, Debug, PartialEq, Eq)]
pub struct LabeledVecGraph<L: Clone + 'static> {
    /// The number of arcs in the graph.
    num_arcs: u64,
    /// For each node, its list of successors.
    #[epserde(force_full_copy)]
    succ: Vec<Vec<LabeledArc<L>>>,
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
            num_arcs: 0,
            succ: vec![],
        }
    }

    /// Creates a new empty graph with `n` nodes.
    pub fn empty(n: usize) -> Self {
        Self {
            num_arcs: 0,
            succ: Vec::from_iter((0..n).map(|_| Vec::new())),
        }
    }

    /// Adds an isolated node to the graph and returns true if it is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        let len = self.succ.len();
        self.succ.extend((len..=node).map(|_| Vec::new()));
        len <= node
    }

    /// Adds an arc to the graph.
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
    ///   of the source node (in particular, if the arc is a duplicate of the
    ///   last added arc).
    pub fn add_arc(&mut self, u: usize, v: usize, l: L) {
        assert_nodes_exist(self.succ.len(), u, v);
        let succ = &mut self.succ[u];

        match succ.last() {
            None => {
                succ.push((v, l).into());
                self.num_arcs += 1;
            }
            Some(LabeledArc(last, _label)) => {
                if v <= *last {
                    // arcs have to be inserted in increasing successor order
                    panic!(
                        "Error adding arc ({u}, {v}): the successor is not strictly increasing (duplicate arcs are not allowed); the last arc inserted was ({u}, {last})"
                    );
                }
                succ.push((v, l).into());
                self.num_arcs += 1;
            }
        }
    }

    /// Adds nodes and successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// If the lender is sorted, consider using [`add_sorted_lender`], as it
    /// does not need to sort the output of the lender.
    ///
    /// [`add_sorted_lender`]: Self::add_sorted_lender
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
    /// If the lender is sorted, consider using [`from_sorted_lender`], as it
    /// does not need to sort the output of the lender.
    ///
    /// [`from_sorted_lender`]: Self::from_sorted_lender
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Adds nodes and successors from an [`IntoLender`] yielding a sorted
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`add_lender`] as it does not need to sort
    /// the output of the lender.
    ///
    /// [`add_lender`]: Self::add_lender
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
    /// This method is faster than [`from_lender`] as it does not need to
    /// sort the output of the lender.
    ///
    /// [`from_lender`]: Self::from_lender
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

    /// Adds nodes and successors from an [`IntoLender`] yielding a sorted
    /// [`NodeLabelsLender`] whose successors implement [`ExactSizeIterator`].
    ///
    /// This method has a better memory behavior than [`add_sorted_lender`]
    /// as it can allocate the right amount of memory for each node at once.
    ///
    /// [`add_sorted_lender`]: Self::add_sorted_lender
    pub fn add_exact_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator + ExactSizeIterator,
    {
        for_!( (node, succ) in iter_nodes {
            self.add_node(node);
            let succ = succ.into_iter();
            let d = succ.len();
            self.succ[node].reserve_exact(d);
            self.succ[node].extend(succ.map(Into::into));
            self.num_arcs += d as u64;
            // Add the missing successor nodes; since the successors are
            // sorted, it suffices to add the last one.
            if let Some(max_succ) = self.succ[node].last().map(|arc| arc.0) {
                self.add_node(max_succ);
            }
        });
        self
    }

    /// Creates a new graph from a sorted [`IntoLender`] yielding a
    /// [`NodeLabelsLender`] whose successors implement [`ExactSizeIterator`].
    ///
    /// This method has a better memory behavior than [`from_sorted_lender`]
    /// as it can allocate the right amount of memory for each node at once.
    ///
    /// [`from_sorted_lender`]: Self::from_sorted_lender
    pub fn from_exact_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator + ExactSizeIterator,
    {
        let mut g = Self::new();
        g.add_exact_lender(iter_nodes);
        g
    }

    /// Adds labeled arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be labeled pairs of the form `((usize, usize), l)` specifying an
    /// arc and its label. Duplicate arcs are added once, keeping the label of
    /// the last occurrence, as in [`LabeledBTreeGraph`].
    ///
    /// [`LabeledBTreeGraph`]: crate::graphs::btree_graph::LabeledBTreeGraph
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = ((usize, usize), L)>) {
        let mut arcs = arcs.into_iter().collect::<Vec<_>>();
        arcs.sort_by_key(|x| x.0);
        // Keep the label of the last occurrence of each arc: `b` is the
        // element that survives deduplication, `a` the one that is removed.
        arcs.dedup_by(|a, b| {
            let dup = a.0 == b.0;
            if dup {
                core::mem::swap(a, b);
            }
            dup
        });
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
    pub fn shrink_to_fit(&mut self) {
        self.succ.shrink_to_fit();
        for s in self.succ.iter_mut() {
            s.shrink_to_fit();
        }
    }
}

impl_seq_from_ra!([L: Clone + 'static] LabeledVecGraph<L>, (usize, L), |g| g.succ.len());
impl_into_lender!([L: Clone + 'static] LabeledVecGraph<L>);

impl<L: Clone + 'static> LabeledSequentialGraph<L> for LabeledVecGraph<L> {}

impl<L: Clone + 'static> RandomAccessLabeling for LabeledVecGraph<L> {
    type Labels<'succ> = AssumeSortedIterator<
        core::iter::Map<
            core::iter::Cloned<core::slice::Iter<'succ, LabeledArc<L>>>,
            fn(LabeledArc<L>) -> (usize, L),
        >,
    >;
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
        // SAFETY: arcs are added in increasing successor order, so successors
        // are sorted.
        unsafe { AssumeSortedIterator::new(self.succ[node].iter().cloned().map(Into::into)) }
    }
}

impl<L: Clone + 'static> LabeledRandomAccessGraph<L> for LabeledVecGraph<L> {}

impl_split!(ra [L: Clone + Sync] LabeledVecGraph<L> [] []);

/// A mutable [`RandomAccessGraph`] implementation based on a vector of
/// vectors.
///
/// This implementation is faster and uses less resources than a [`BTreeGraph`],
/// but it is less flexible as arcs can be added only in increasing successor
/// order.
///
/// This struct can be serialized with [ε-serde]. By setting the feature
/// `serde`, this struct can be serialized using [serde], too.
///
/// # Implementation Notes
///
/// This is just a newtype for a [`LabeledVecGraph`] with [`()`] labels.
/// All mutation methods are delegated.
///
/// [ε-serde]: https://crates.io/crates/epserde
/// [serde]: https://crates.io/crates/serde
/// [`()`]: https://doc.rust-lang.org/std/primitive.unit.html
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

    /// Adds an isolated node to the graph and returns true if it is a new node.
    pub fn add_node(&mut self, node: usize) -> bool {
        self.0.add_node(node)
    }

    /// Adds an arc to the graph.
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

    /// Adds nodes and successors from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// If the lender is sorted, consider using [`add_sorted_lender`], as it
    /// does not need to sort the output of the lender.
    ///
    /// [`add_sorted_lender`]: Self::add_sorted_lender
    pub fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        self.0.add_lender(UnitLabelLender(iter_nodes.into_lender()));
        self
    }

    /// Creates a new graph from an [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// If the lender is sorted, consider using [`from_sorted_lender`], as it
    /// does not need to sort the output of the lender.
    ///
    /// [`from_sorted_lender`]: Self::from_sorted_lender
    pub fn from_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        let mut g = Self::new();
        g.add_lender(iter_nodes);
        g
    }

    /// Adds nodes and successors from an [`IntoLender`] yielding a sorted
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`add_lender`] as it does not need to sort
    /// the output of the lender.
    ///
    /// [`add_lender`]: Self::add_lender
    pub fn add_sorted_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator,
    {
        self.0
            .add_sorted_lender(UnitLabelLender(iter_nodes.into_lender()));
        self
    }

    /// Creates a new graph from a sorted [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`from_lender`] as it does not need to
    /// sort the output of the lender.
    ///
    /// [`from_lender`]: Self::from_lender
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

    /// Adds nodes and successors from an [`IntoLender`] yielding a sorted
    /// [`NodeLabelsLender`] whose successors implement [`ExactSizeIterator`].
    ///
    /// This method has a better memory behavior than [`add_sorted_lender`]
    /// as it can allocate the right amount of memory for each node at once.
    ///
    /// [`add_sorted_lender`]: Self::add_sorted_lender
    pub fn add_exact_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator + ExactSizeIterator,
    {
        self.0
            .add_exact_lender(UnitLabelLender(iter_nodes.into_lender()));
        self
    }

    /// Creates a new graph from a sorted [`IntoLender`] yielding a
    /// [`NodeLabelsLender`] whose successors implement [`ExactSizeIterator`].
    ///
    /// This method has a better memory behavior than [`from_sorted_lender`]
    /// as it can allocate the right amount of memory for each node at once.
    ///
    /// [`from_sorted_lender`]: Self::from_sorted_lender
    pub fn from_exact_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator + ExactSizeIterator,
    {
        let mut g = Self::new();
        g.add_exact_lender(iter_nodes);
        g
    }

    /// Adds arcs from an [`IntoIterator`], adding new nodes as needed.
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying an
    /// arc. Duplicate arcs are added once.
    pub fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        self.0.add_arcs(arcs.into_iter().map(|pair| (pair, ())));
    }

    /// Creates a new graph from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying an arc.
    pub fn from_arcs(arcs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut g = Self::new();
        g.add_arcs(arcs);
        g
    }

    /// Shrinks the capacity of the graph to fit its current size.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }
}

impl_into_lender!([] VecGraph);
impl_seq_from_ra!([] VecGraph, usize, |g| g.0.num_nodes());

impl SequentialGraph for VecGraph {}

impl RandomAccessLabeling for VecGraph {
    type Labels<'succ> = AssumeSortedIterator<
        core::iter::Map<
            core::iter::Copied<core::slice::Iter<'succ, LabeledArc<()>>>,
            fn(LabeledArc<()>) -> usize,
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
        unsafe {
            AssumeSortedIterator::new(self.0.succ[node].iter().copied().map(|LabeledArc(x, _)| x))
        }
    }
}

impl RandomAccessGraph for VecGraph {}

impl From<LabeledVecGraph<()>> for VecGraph {
    fn from(g: LabeledVecGraph<()>) -> Self {
        VecGraph(g)
    }
}

impl_split!(ra [] VecGraph [] []);

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_vec_graph() {
        crate::graphs::check_nan_labels(LabeledVecGraph::from_arcs);
    }
}
