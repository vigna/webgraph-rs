/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

use lender::prelude::*;
use std::{collections::BTreeSet, mem::MaybeUninit};

/// A mutable [`RandomAccessGraph`] implementation based on a vector of vectors.
/// The arcs of each node has to be inserted monotonically.
///
/// This implementation works for bigger graphs than [`BTreeGraph`] and
/// can be converted to a CSR (Compressed Sparse Row) representation which
/// allows memory mapping through epserde.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VecGraph {
    /// The number of arcs in the graph.
    number_of_arcs: u64,
    /// For each node, its list of successors.
    succ: Vec<Vec<usize>>,
}

impl core::default::Default for VecGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl VecGraph {
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
    /// # Panics
    /// - If the given nodes are bigger or equal than the number of nodes in the graph.
    /// - If the inserted arc destination `v` is not bigger than all the sucessors
    ///      of the node `u`.
    fn add_arc(&mut self, u: usize, v: usize) -> bool {
        let max = u.max(v);
        if max >= self.succ.len() {
            panic!(
                "Node {} does not exist (the graph has {} nodes)",
                max,
                self.succ.len(),
            );
        }
        let succ = &mut self.succ[u];
        let biggest_dst = succ.last().unwrap_or(0);
        match v.cmp(&biggest_dst) {
            // arcs have to be inserted in order
            core::cmp::Ordering::Less => panic!(
                "Error adding arc ({u}, {v}) as its insertion is not monotonic. The last arc inserted was ({u}, {})",
                biggest_dst,
            ),
            // no duplicated arcs
            core::cmp::Ordering::Equal => succ.last().is_none(),
            core::cmp::Ordering::Greater => {
                succ.push(v);
                self.number_of_arcs += 1;
                true
            }
        }
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a [`NodeLabelsLender`].
    fn add_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        let mut arcs = Vec::new();
        for_!( (node, succ) in iter_nodes {
            arcs.clear();
            self.add_node(node);
            for v in succ {
                arcs.push(v);
                self.add_node(v);
            }
            arcs.sort();
            for v in arcs {
                self.add_arc(node, v);
            }
        });
        self
    }

    /// Add nodes and successors from an [`IntoLender`] yielding a sorted [`NodeLabelsLender`].
    fn add_sorted_lender<I: IntoLender>(&mut self, iter_nodes: I) -> &mut Self
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

    /// Add arcs from an [`IntoIterator`].
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    fn add_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        let mut arcs = arcs.collect::<Vec<_>>();
        arcs.sort();
        for (u, v) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_arc(u, v);
        }
    }

    /// Add arcs from a sorted [`IntoIterator`] of paris of nodes.
    ///
    /// The items must be pairs of the form `(usize, usize)` specifying
    /// an arc.
    ///
    /// Note that new nodes will be added as needed.
    fn add_sorted_arcs(&mut self, arcs: impl IntoIterator<Item = (usize, usize)>) {
        for (u, v) in arcs {
            self.add_node(u);
            self.add_node(v);
            self.add_arc(u, v);
        }
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

    /// Creates a new graph from a sorted [`IntoLender`] yielding a [`NodeLabelsLender`].
    pub fn from_sorted_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, L)>,
    {
        let mut g = Self::new();
        g.add_sorted_lender(iter_nodes);
        g
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

impl IntoLender for &'a VecGraph {
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

impl SequentialGraph for VecGraph {}

impl RandomAccessLabeling for VecGraph {
    type Labels<'succ>
        = core::iter::Copied<core::slice::Iter<'succ, usize>>;
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
        self.succ[node].iter().copied()
    }
}

impl RandomAccessGraph for VecGraph {}

#[test]
fn test_remove() {
    let mut g = VecGraph::<_>::from_arcs([(0, 1), (0, 2), (1, 2)]);
    assert!(g.remove_arc(0, 2));
    assert!(!g.remove_arc(0, 2));
}
