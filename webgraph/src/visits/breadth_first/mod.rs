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
//! only exception is the [`Visit`] event at the root.
//!
//! [`Visit`]: EventPred::Visit

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
        /// The parent of [`node`] in the visit tree, or [`node`] if
        /// [`node`] is one of the roots.
        ///
        /// [`node`]: EventPred::Visit::node
        pred: usize,
        /// The distance of the current node from the roots.
        distance: usize,
    },
    /// The node has been encountered before: we are traversing a back arc, a
    /// forward arc, or a cross arc.
    ///
    /// Note however that in parallel contexts it might happen that callback
    /// with event [`Visit`] has not been called yet by the thread who
    /// discovered the node.
    ///
    /// [`Visit`]: EventPred::Visit
    Revisit {
        /// The current node.
        node: usize,
        /// The predecessor of [node].
        ///
        /// [node]: EventPred::Revisit::node
        pred: usize,
    },
    /// The size of the frontier at a given distance.
    ///
    /// This event will happen with increasing value of
    /// [`distance`], starting at 0.
    ///
    /// If the root is formed by a single node, this is the size of the sphere
    /// with center at the root and radius [`distance`].
    ///
    /// This event will happen just before starting to visit nodes at a given
    /// distance or when all nodes at that distance have been visited, depending
    /// on the implementation.
    ///
    /// [`distance`]: EventPred::FrontierSize::distance
    FrontierSize {
        /// A distance.
        distance: usize,
        /// The number of nodes at [`distance`] from the roots.
        ///
        /// [`distance`]: EventPred::FrontierSize::distance
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
    /// The predecessor of [node].
    ///
    /// [node]: Self::node
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
    /// Note however that in parallel contexts it might happen that the callback
    /// with event [`Visit`] has not been called yet by the thread who
    /// discovered the node.
    ///
    /// [`Visit`]: EventNoPred::Visit
    Revisit {
        /// The current node.
        node: usize,
    },
    /// The size of the frontier at a given distance.
    ///
    /// This event will happen with increasing value of
    /// [`distance`], starting at 0.
    ///
    /// If the root is formed by a single node, this is the size of the sphere
    /// with center at the root and radius [`distance`].
    ///
    /// This event will happen just before starting to visit nodes at a given
    /// distance or when all nodes at that distance have been visited, depending
    /// on the implementation.
    ///
    /// [`distance`]: EventNoPred::FrontierSize::distance
    FrontierSize {
        /// A distance.
        distance: usize,
        /// The number of nodes at [`distance`] from the roots.
        ///
        /// [`distance`]: EventNoPred::FrontierSize::distance
        size: usize,
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
