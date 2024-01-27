/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Blanket Implementations of [`petgraph`] traits for our [`Graph`].

use crate::prelude::*;

pub struct PetAdapter<G: SequentialGraph>(G);

impl<G> petgraph::visit::GraphBase for PetAdapter<G>
where
    G: SequentialGraph,
{
    type NodeId = usize;
    type EdgeId = usize;
}

impl<G> petgraph::visit::GraphProp for PetAdapter<G>
where
    G: SequentialGraph,
{
    type EdgeType = petgraph::Directed;
}

impl<G> petgraph::visit::Data for PetAdapter<G>
where
    G: SequentialGraph,
{
    type NodeWeight = ();
    type EdgeWeight = ();
}

impl<G> petgraph::visit::NodeCount for PetAdapter<G>
where
    G: SequentialGraph,
{   
    #[inline(always)]
    fn node_count(&self) -> usize {
        self.num_nodes()
    }
}

impl<G> petgraph::visit::NodeIndexable for PetAdapter<G>
where
    G: SequentialGraph,
{   
    #[inline(always)]
    fn node_bound(&self) -> usize {
        self.num_nodes()
    }
    #[inline(always)]
    fn to_index(&self, a: Self::NodeId) -> usize {
        a
    }
    #[inline(always)]
    fn from_index(&self, i: usize) -> Self::NodeId {
        i
    }
}

impl<G> petgraph::visit::NodeCompactIndexable for PetAdapter<G>
where
    G: SequentialGraph,
{}

impl<G> petgraph::visit::EdgeCount for PetAdapter<G>
where
    G: RandomAccessGraph,
{
    #[inline(always)]
    fn edge_count(&self) -> usize {
        self.num_arcs()
    }
}

impl<G> petgraph::visit::EdgeIndexable for PetAdapter<G>
where
    G: RandomAccessGraph,
{
    #[inline(always)]
    fn edge_bound(&self) -> usize {
        self.num_arcs()
    }
    #[inline(always)]
    fn to_index(&self, a: Self::EdgeId) -> usize {
        a
    }
    #[inline(always)]
    fn from_index(&self, i: usize) -> Self::EdgeId {
        i
    }
}

impl<'a, G> petgraph::visit::IntoNodeIdentifiers for &'a PetAdapter<G>
where
    G: SequentialGraph,
{
    type NodeIdentifiers = std::ops::Range<usize>;
    #[inline(always)]
    fn node_identifiers(self) -> Self::NodeIdentifiers {
        0..self.num_nodes()
    }
}


impl<'a, G> petgraph::visit::IntoNeighbors for &'a PetAdapter<G>
where
    G: SequentialGraph,
{
    type Neighbors = LenderIntoIter<'static, G>;
    // Required method
    fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
        self.successors(a)
    }
}

impl<'a, G> petgraph::visit::IntoEdgeReferences for &'a PetAdapter<G>
where
    G: SequentialGraph,
{
    type EdgeRef: Edge;
    type EdgeReferences: Iterator<Item = Self::EdgeRef>;

    // Required method
    fn edge_references(self) -> Self::EdgeReferences {

    }
}

pub struct EdgeRefIter<'a, G: SequentialGraph> {
    edge_id: usize,
    source: usize,
    iter: G::Iterator<'a>,
    succ: LenderIntoIterator<'static, G::Iterator<'a>>,
}

impl <'a, G: SequentialGraph> core::iter::Iterator for EdgeRefIter<'a, G> {
    type Item = Edge<()>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(target) = self.succ.next() {
                let edge_id = self.edge_id;
                self.edge_id += 1;
                return Some(Edge {
                    edge_id,
                    source: self.source,
                    target,
                    weight: (),
                });
            }
            let (source, succ) = self.iter.next()?;
            self.source = source;
            self.succ = succ;
        }
    }
}


#[derive(Clone, Copy, Debug)]
pub struct Edge<W: Copy = ()> {
    pub edge_id: usize,
    pub source: usize,
    pub target: usize,
    pub weight: W,
}

impl<W: Copy> petgraph::prelude::EdgeRef for Edge<W> {
    type NodeId = usize;
    type EdgeId = usize;
    type Weight = W;

    #[inline(always)]
    fn source(&self) -> Self::NodeId {
        self.source
    }
    #[inline(always)]
    fn target(&self) -> Self::NodeId {
        self.target
    }
    #[inline(always)]
    fn weight(&self) -> &Self::Weight {
        &self.weight
    }
    #[inline(always)]
    fn id(&self) -> Self::EdgeId {
        self.edge_id
    }
}

