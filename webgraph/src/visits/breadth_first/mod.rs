/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Breadth-first visits.
//!
//! Implementations must accept a callback function with argument
//! [`EventNoPred`], or [`EventPred`] if the visit keeps track of parent nodes.
//! The associated filter argument types are [`FilterArgsNoPred`] and
//! [`FilterArgsPred`], respectively.
//!
//! Note that since [`EventPred`] contains the predecessor of the visited node,
//! all post-initialization visit events can be interpreted as arc events. The
//! only exception is the [`Visit`](EventPred::Visit) event at the root.

mod seq;
pub use seq::*;

mod par_fair;
pub use par_fair::*;

mod par_low_mem;
pub use par_low_mem::*;

/// Types of callback events generated during breadth-first visits
/// keeping track of parent nodes.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum EventPred {
    /// This event should be used to set up state at the start of the visit.
    ///
    /// Note that this event will not happen if the visit is empty, that
    /// is, all of the roots are already visited or filtered.
    Init {},
    /// The node has been encountered for the first time: we are traversing a
    /// new tree arc, unless all node fields are equal to the root.
    Visit {
        /// The current node.
        node: usize,
        /// The parent of [node](`EventPred::Visit::node`) in the visit tree,
        /// or [`node`](`EventPred::Visit::node`) if
        /// [`node`](`EventPred::Visit::node`) is one of the roots.
        pred: usize,
        /// The distance of the current node from the roots.
        distance: usize,
    },
    /// The node has been encountered before: we are traversing a back arc, a
    /// forward arc, or a cross arc.
    ///
    /// Note however that in parallel contexts it might happen that callback
    /// with event [`Visit`](`EventPred::Visit`) has not been called yet by
    /// the thread who discovered the node.
    Revisit {
        /// The current node.
        node: usize,
        /// The predecessor of [node](`EventPred::Revisit::node`).
        pred: usize,
    },
    /// The size of the frontier at a given distance.
    ///
    /// This even will happen with increasing value of
    /// [`distance`](`EventPred::FrontierSize::distance`), starting at 0.
    ///
    /// If the root is formed by a single node, this is the size of the sphere
    /// with center at the root and radius
    /// [`distance`](`EventPred::FrontierSize::distance`).
    ///
    /// This event will happen just before starting to visit nodes at a given
    /// distance or when all nodes at that distance have been visited, depending
    /// on the implementation.
    FrontierSize {
        /// A distance.
        distance: usize,
        /// The number of nodes at
        /// [`distance`](`EventNoPred::FrontierSize::distance`) from the roots.
        size: usize,
    },
    /// The visit has been completed.
    ///
    /// Note that this event will not happen if the visit is empty (that is, if
    /// the root has already been visited) or if the visit is stopped by a
    /// callback returning an error.
    Done {},
}

/// Filter arguments for visits that keep track of predecessors.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FilterArgsPred {
    /// The current node.
    pub node: usize,
    /// The predecessor of [node](`Self::node`).
    pub pred: usize,
    /// The distance of the current node from the roots.
    pub distance: usize,
}

impl super::Event for EventPred {
    type FilterArgs = FilterArgsPred;
}

/// Types of callback events generated during breadth-first visits
/// not keeping track of parent nodes.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum EventNoPred {
    /// This event should be used to set up state at the start of the visit.
    ///
    /// Note that this event will not happen if the visit is empty, that
    /// is, all of the roots are already visited or filtered.
    Init {},
    /// The node has been encountered for the first time: we are traversing a
    /// new tree arc, unless the node is one of the roots.
    Visit {
        /// The current node.
        node: usize,
        /// The distance of the current node from the roots.
        distance: usize,
    },
    /// The node has been encountered before: we are traversing a back arc, a
    /// forward arc, or a cross arc.
    ///
    /// Note however that in parallel contexts it might happen that callback
    /// with event [`Unknown`](`EventNoPred::Visit`) has not been called yet
    /// by the thread who discovered the node.
    Revisit {
        /// The current node.
        node: usize,
    },
    /// The size of the frontier at a given distance.
    ///
    /// This even will happen with increasing value of
    /// [`distance`](`EventPred::FrontierSize::distance`), starting at 0.
    ///
    /// If the root is formed by a single node, this is the size of the sphere
    /// with center at the root and radius
    /// [`distance`](`EventPred::FrontierSize::distance`).
    ///
    /// This event will happen just before starting to visit nodes at a given
    /// distance or when all nodes at that distance have been visited, depending
    /// on the implementation.
    FrontierSize {
        /// A distance.
        distance: usize,
        /// The number of nodes at
        /// [`distance`](`EventNoPred::FrontierSize::distance`) from the roots.
        sizes: usize,
    },
    /// The visit has been completed.
    ///
    /// Note that this event will not happen if the visit is empty (that is, if
    /// the root has already been visited) or if the visit is stopped by a
    /// callback returning an error.
    Done {},
}

/// Filter arguments for visits that do not keep track of predecessors.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FilterArgsNoPred {
    /// The current node.
    pub node: usize,
    /// The distance of the current node from the roots.
    pub distance: usize,
}

impl super::Event for EventNoPred {
    type FilterArgs = FilterArgsNoPred;
}
