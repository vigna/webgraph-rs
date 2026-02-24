/*
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// Granularity of parallel tasks, specified transparently by nodes or arcs.
///
/// This enum provides a way to specify the granularity of parallel tasks on
/// graphs. It is used by
/// [`par_apply`](crate::traits::SequentialLabeling::par_apply) and
/// [`par_node_apply`](crate::traits::SequentialLabeling::par_node_apply).
///
/// Some parallel implementations (e.g.,
/// [`par_node_apply`](crate::traits::SequentialLabeling::par_node_apply))
/// express naturally the granularity of their tasks via a number of nodes,
/// whereas others (e.g.,
/// [`par_apply`](crate::traits::SequentialLabeling::par_apply)) via a
/// number of arcs. This enum allows to specify the granularity of parallel
/// tasks in a transparent way, by nodes or arcs. Conversion between the two
/// specifications is done by the methods
/// [`arc_granularity`](Self::arc_granularity) and
/// [`node_granularity`](Self::node_granularity).
#[derive(Debug, Clone, Copy)]
pub enum Granularity {
    /// Node granularity.
    ///
    /// For node-based parallelism, each task will be formed by the specified
    /// number of nodes. For arc-based parallelism, each task will be formed by
    /// a number of nodes that has, tentatively, sum of outdegrees equal to the
    /// average outdegree multiplied by the specified number of nodes.
    Nodes(usize),
    /// Arc granularity.
    ///
    /// For arc-based parallelism, each task will be formed by a number of nodes
    /// that has, tentatively, sum of outdegrees equal to the specified number
    /// of arcs. For node-based parallelism, each task will be formed by a
    /// number of nodes equal to the specified number of arcs divided by the
    /// average outdegree.
    Arcs(u64),
}

impl core::default::Default for Granularity {
    /// Returns a default relative granularity of 1000 nodes.
    fn default() -> Self {
        Self::Nodes(1000)
    }
}

impl Granularity {
    /// Returns a node granularity for a given number of elements and threads.
    ///
    /// For the variant [`Nodes`](Self::Nodes), the specified number of nodes is
    /// returned. For the variant [`Arcs`](Self::Arcs), the number of nodes is
    /// computed as the specified number of arcs divided by the average
    /// outdegree.
    ///
    /// # Panics
    ///
    /// This method will panic if it needs to make a conversion from arc
    /// granularity to node granularity and the number of arcs is not provided.
    pub fn node_granularity(&self, num_nodes: usize, num_arcs: Option<u64>) -> usize {
        match self {
            Self::Nodes(n) => *n,
            Self::Arcs(n) => {
                let average_degree = num_arcs.expect(
                    "You need the number of arcs to convert arc granularity to node granularity",
                ) as f64
                    / num_nodes.max(1) as f64;
                (*n as f64 / average_degree).min(usize::MAX as f64).ceil() as usize
            }
        }
    }

    /// Returns an arc granularity for a given number of nodes and arcs.
    ///
    /// For the [`Arcs`](Self::Arcs) variant, the specified number of arcs is
    /// returned. For the [`Nodes`](Self::Nodes) variant, the number of nodes is
    /// computed as the specified number of arcs divided by the average degree.
    ///
    /// # Panics
    ///
    /// This method will panic if it needs to make a conversion from node
    /// granularity to arc granularity and the number of arcs is not provided.
    pub fn arc_granularity(&self, num_nodes: usize, num_arcs: Option<u64>) -> usize {
        match self {
            Self::Nodes(n) => {
                let average_degree = num_arcs.expect(
                    "You need the number of arcs to convert node granularity to arc granularity",
                ) as f64
                    / num_nodes.max(1) as f64;
                (*n as f64 * average_degree).ceil().max(1.) as usize
            }
            Self::Arcs(n) => *n as usize,
        }
    }
}
