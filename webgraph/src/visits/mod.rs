/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Visits on graphs.
//!
//! Implementation of [sequential](Sequential) and [parallel][Parallel] visits
//! depend on a type parameter `A` implementing the trait [`Event`]; they
//! provide visit methods accepting a callback function with argument `A` and
//! returning a `ControlFlow<E, ()>`, where `E` is a type parameter of the visit
//! method: for example, `E` might be [`StoppedWhenDone`] when completing early,
//! [`Interrupted`] when interrupted or [`Infallible`](std::convert::Infallible)
//! if the visit cannot be interrupted.
//!
//! If a callback returns a [`Break`](ControlFlow::Break), the visit will be
//! interrupted, and the [`Break`](ControlFlow::Break) value will be the return
//! value of the visit method; for uninterruptible visits we suggest to use the
//! [`no-break`](https://crates.io/crates/no-break) crate and its
//! [`continue_value_no_break`](no_break::NoBreak::continue_value_no_break)
//! method on the result to let type inference run smoothly.
//!
//! Note that an interruption does not necessarily denote an error condition
//! (see, e.g., [`StoppedWhenDone`]).
//!
//! [Sequential visits](Sequential) are visits that are executed in a single
//! thread, whereas [parallel visits](Parallel) use multiple threads. The
//! signature of callbacks reflects this difference ([`FnMut`] for the
//! sequential case vs. [`Fn`] + [`Sync`] for the parallel case).
//!
//! In case of interruption sequential visits usually return immediately to the
//! caller, whereas in general parallel visits might need to complete part of
//! their subtasks before returning to the caller.
//!
//! Additionally, implementations might accepts a filter function accepting a
//! [`Event::FilterArgs`] that will be called when a new node is discovered. If
//! the filter returns false, the node will be ignored, that is, not even marked
//! as known. Note that in case of parallel visits the filter might be called
//! multiple times on the same node (and with a different predecessor, if
//! available) due to race conditions.
//!
//! All visits have also methods accepting an `init` item similarly to the
//! [Rayon](rayon) [`map_with`](rayon::iter::ParallelIterator::map_with) method.
//! For parallel visits, the item will be cloned.
//!
//! There is a blanket implementation of the [`Parallel`] trait for all types
//! implementing the [`Sequential`] trait. This approach makes it possible to
//! have structures that can use both sequential and parallel visits.
//!
//! Visit must provide a `reset` method that makes it possible to reuse the
//! visit.
//!
//! # Examples
//!
//! There are examples of visits in
//! [`SeqIter`](crate::visits::depth_first::SeqIter),
//! [`ParFair`](crate::visits::breadth_first::ParFair) and
//! [`ParLowMem`](crate::visits::breadth_first::ParLowMem).

pub mod breadth_first;
pub mod depth_first;

use std::ops::ControlFlow;
use thiserror::Error;

#[derive(Error, Debug)]
/// The visit was interrupted.
#[error("The visit was interrupted")]
pub struct Interrupted;

#[derive(Error, Debug)]
/// The result of the visit was computed without completing the visit; for
/// example, during an acyclicity test a single arc pointing at the visit path
/// is sufficient to compute the result.
#[error("Stopped when done")]
pub struct StoppedWhenDone;

/// Types usable as arguments for the callbacks in visits.
///
/// Arguments are usually enums in which variants represent visit events
/// (previsits, postvisits, etc.). Each variant then contains additional data
/// related to the specific event.
///
/// The associated type [`Event::FilterArgs`] is the type of the arguments
/// passed to the filter associated with the visit. It can be set to `()` if
/// filtering is not supported
pub trait Event {
    /// The type passed as input to the filter.
    type FilterArgs;
}

/// A convenience type alias for the filter arguments of an event.
///
/// It is useful to write match patterns using destructuring syntax.
pub type FilterArgs<A> = <A as Event>::FilterArgs;

/// A sequential visit.
///
/// Implementation of this trait must provide the
/// [`visit_filtered_with`](Sequential::visit_filtered_with) method, which
/// should perform a visit of a graph starting from a given set of nodes. Note
/// that different visits types might interpret the set of nodes differently:
/// for example, a [breadth-first visit](breadth_first) will interpret the set
/// of nodes as the initial queue, whereas a [depth-first visit](depth_first)
/// will interpret the set of nodes as a list of nodes from which to start
/// visits.
pub trait Sequential<A: Event> {
    /// Visits the graph from the specified nodes with an initialization value
    /// and a filter function.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `init`: a value the will be passed to the callback function.
    ///
    /// * `callback`: The callback function.
    ///
    /// * `filter`: The filter function.
    fn visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T,
        E,
        C: FnMut(&mut T, A) -> ControlFlow<E, ()>,
        F: FnMut(&mut T, A::FilterArgs) -> bool,
    >(
        &mut self,
        roots: R,
        init: T,
        callback: C,
        filter: F,
    ) -> ControlFlow<E, ()>;

    /// Visits the graph from the specified nodes with a filter function.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `callback`: The callback function.
    ///
    /// * `filter`: The filter function.
    fn visit_filtered<
        R: IntoIterator<Item = usize>,
        E,
        C: FnMut(A) -> ControlFlow<E, ()>,
        F: FnMut(A::FilterArgs) -> bool,
    >(
        &mut self,
        roots: R,
        mut callback: C,
        mut filter: F,
    ) -> ControlFlow<E, ()> {
        self.visit_filtered_with(roots, (), |(), a| callback(a), |(), a| filter(a))
    }

    /// Visits the graph from the specified nodes with an initialization value.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `init`: a value the will be passed to the callback function.
    ///
    /// * `callback`: The callback function.
    fn visit_with<
        R: IntoIterator<Item = usize>,
        T,
        E,
        C: FnMut(&mut T, A) -> ControlFlow<E, ()>,
    >(
        &mut self,
        roots: R,
        init: T,
        callback: C,
    ) -> ControlFlow<E, ()> {
        self.visit_filtered_with(roots, init, callback, |_, _| true)
    }

    /// Visits the graph from the specified nodes.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `callback`: The callback function.
    fn visit<R: IntoIterator<Item = usize>, E, C: FnMut(A) -> ControlFlow<E, ()>>(
        &mut self,
        roots: R,
        callback: C,
    ) -> ControlFlow<E, ()> {
        self.visit_filtered(roots, callback, |_| true)
    }

    /// Resets the visit status, making it possible to reuse it.
    fn reset(&mut self);
}

/// A parallel visit.
///
/// Implementation of this trait must provide the
/// [`par_visit_filtered_with`](Parallel::par_visit_filtered_with) method, which
/// should perform a parallel visit of a graph starting from a given set of
/// nodes. Note that different visits types might interpret the set of nodes
/// differently: for example, a [breadth-first visit](breadth_first) will
/// interpret the set of nodes as the initial queue, whereas a [depth-first
/// visit](depth_first) will interpret the set of nodes as a list of nodes from
/// which to start visits.
pub trait Parallel<A: Event> {
    /// Visits the graph from the specified nodes with an initialization value
    /// and a filter function.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `init`: a value the will be cloned and passed to the callback
    ///   function.
    ///
    /// * `callback`: The callback function.
    ///
    /// * `filter`: The filter function.
    fn par_visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T: Clone + Send + Sync + Sync,
        E: Send,
        C: Fn(&mut T, A) -> ControlFlow<E, ()> + Sync,
        F: Fn(&mut T, A::FilterArgs) -> bool + Sync,
    >(
        &mut self,
        roots: R,
        init: T,
        callback: C,
        filter: F,
    ) -> ControlFlow<E, ()>;

    /// Visits the graph from the specified nodes with a filter function.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `callback`: The callback function.
    ///
    /// * `filter`: The filter function.
    fn par_visit_filtered<
        R: IntoIterator<Item = usize>,
        E: Send,
        C: Fn(A) -> ControlFlow<E, ()> + Sync,
        F: Fn(A::FilterArgs) -> bool + Sync,
    >(
        &mut self,
        roots: R,
        callback: C,
        filter: F,
    ) -> ControlFlow<E, ()> {
        self.par_visit_filtered_with(
            roots,
            (),
            |(), a| callback(a),
            |(), a| filter(a),
        )
    }

    /// Visits the graph from the specified nodes with an initialization value.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `init`: a value the will be cloned and passed to the callback
    ///   function.
    ///
    /// * `callback`: The callback function.
    fn par_visit_with<
        R: IntoIterator<Item = usize>,
        T: Clone + Send + Sync + Sync,
        E: Send,
        C: Fn(&mut T, A) -> ControlFlow<E, ()> + Sync,
    >(
        &mut self,
        roots: R,
        init: T,
        callback: C,
    ) -> ControlFlow<E, ()> {
        self.par_visit_filtered_with(roots, init, callback, |_, _| true)
    }

    /// Visits the graph from the specified nodes.
    ///
    /// See the [module documentation](crate::visits) for more information on
    /// the return value.
    ///
    /// # Arguments
    ///
    /// * `roots`: The nodes to start the visit from.
    ///
    /// * `callback`: The callback function.
    fn par_visit<R: IntoIterator<Item = usize>, E: Send, C: Fn(A) -> ControlFlow<E, ()> + Sync>(
        &mut self,
        roots: R,
        callback: C,
    ) -> ControlFlow<E, ()> {
        self.par_visit_filtered(roots, callback, |_| true)
    }

    /// Resets the visit status, making it possible to reuse it.
    fn reset(&mut self);
}
