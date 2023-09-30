/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Basic traits to access graphs, both sequentially and
in random-access fashion.

*/

use crate::prelude::*;
use core::{
    ops::Range,
    sync::atomic::{AtomicUsize, Ordering},
};
use dsi_progress_logger::ProgressLogger;
use std::sync::Mutex;

/// A support trait that make it possible to specify separate conditions
/// on the two components of the pairs returned by a
/// [graph iterator](SequentialGraph::Iterator).
///
/// The user should rarely, if ever, interact with this trait. A good
/// example of its use is in
/// [`VecGraph::from_node_iter`](crate::graph::vec_graph::VecGraph::from_node_iter).
///
/// The main purpose of [Tuple2] is to make it possible to write methods
/// accepting a generic [lending iterator](LendingIterator) returning pairs
/// of nodes and successors, and to iterate over such iterators.
pub trait Tuple2 {
    type _0;
    type _1;

    fn into_tuple(self) -> (Self::_0, Self::_1);
}

impl<T, U> Tuple2 for (T, U) {
    type _0 = T;
    type _1 = U;

    fn into_tuple(self) -> (Self::_0, Self::_1) {
        self
    }
}

/// A graph that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or the successors of a node will be returned in ascending order.
/// The marker traits [SortedIterator] and [SortedSuccessors] can be used to
/// force these properties.
///
/// The iterator returned by [iter](SequentialGraph::iter) is [lending](LendingIterator):
/// to access the next pair, you must have finished to use the previous one. You
/// can invoke [`LendingIterator::into_iter`] to get a standard iterator, in general
/// at the cost of some allocation and copying.
pub trait SequentialGraph {
    type Successors<'succ>: IntoIterator<Item = usize>;
    /// The type of the iterator over the successors of a node
    /// returned by [the iterator on the graph](SequentialGraph::Iterator).
    type Iterator<'node>: LendingIterator
        + for<'succ> LendingIteratorItem<'succ, T = (usize, Self::Successors<'succ>)>
    where
        Self: 'node;

    /// Return the number of nodes in the graph.
    fn num_nodes(&self) -> usize;

    /// Return the number of arcs in the graph, if available.
    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    /// Return an iterator over the graph.
    ///
    /// Iterators over the graph return pairs given by a node of the graph
    /// and an [IntoIterator] over its successors.
    fn iter(&self) -> Self::Iterator<'_> {
        self.iter_from(0)
    }

    /// Return an iterator over the nodes of the graph starting at `from`
    /// (included).
    ///
    /// Note that if the graph iterator [is not sorted](SortedIterator),
    /// `from` is not the node id of the first node returned by the iterator,
    /// but just the starting point of the iteration.
    fn iter_from(&self, from: usize) -> Self::Iterator<'_>;

    /// Given a graph, apply `func` to each chunk of nodes of size `granularity`
    /// in parallel, and reduce the results using `reduce`.
    fn par_graph_apply<F, R, T>(
        &self,
        func: F,
        reduce: R,
        thread_pool: &rayon::ThreadPool,
        granularity: usize,
        pr: Option<&mut ProgressLogger>,
    ) -> T
    where
        F: Fn(Range<usize>) -> T + Send + Sync,
        R: Fn(T, T) -> T + Send + Sync,
        T: Send + Default,
    {
        let pr_lock = pr.map(Mutex::new);
        let num_nodes = self.num_nodes();
        let num_cpus = thread_pool
            .current_num_threads()
            .min(num_nodes / granularity)
            .max(1);
        let next_node = AtomicUsize::new(0);

        thread_pool.scope(|scope| {
            let mut res = Vec::with_capacity(num_cpus);
            for _ in 0..num_cpus {
                // create a channel to receive the result
                let (tx, rx) = std::sync::mpsc::channel();
                res.push(rx);

                // create some references so that we can share them across threads
                let pr_lock_ref = &pr_lock;
                let next_node_ref = &next_node;
                let func_ref = &func;
                let reduce_ref = &reduce;

                scope.spawn(move |_| {
                    let mut result = T::default();
                    loop {
                        // compute the next chunk of nodes to process
                        let start_pos = next_node_ref.fetch_add(granularity, Ordering::Relaxed);
                        let end_pos = (start_pos + granularity).min(num_nodes);
                        // exit if done
                        if start_pos >= num_nodes {
                            break;
                        }
                        // apply the function and reduce the result
                        result = reduce_ref(result, func_ref(start_pos..end_pos));
                        // update the progress logger if specified
                        if let Some(pr_lock) = pr_lock_ref {
                            pr_lock
                                .lock()
                                .unwrap()
                                .update_with_count((start_pos..end_pos).len());
                        }
                    }
                    // comunicate back that the thread finished
                    tx.send(result).unwrap();
                });
            }
            // reduce the results
            let mut result = T::default();
            for rx in res {
                result = reduce(result, rx.recv().unwrap());
            }
            result
        })
    }
}

/// Marker trait for [sequential graphs](SequentialGraph) whose [iterator](SequentialGraph::Iterator)
/// returns nodes in ascending order.
///
/// # Safety
/// The first element of the pairs returned by the iterator must go from
/// zero to the [number of nodes](SequentialGraph::num_nodes) of the graph, excluded.
pub unsafe trait SortedIterator: LendingIterator {}

/// Marker trait for [sequential graphs](SequentialGraph) whose [successors](SequentialGraph::Successors)
/// are returned in ascending order.
///
/// # Safety
/// The successors returned by the iterator must be in ascending order.
pub unsafe trait SortedSuccessors: IntoIterator {}

/// A [sequential graph](SequentialGraph) providing, additionally, random access to successor lists.
pub trait RandomAccessGraph: SequentialGraph {
    /// The type of the iterator over the successors of a node
    /// returned by [successors](RandomAccessGraph::successors).
    type Successors<'succ>: IntoIterator<Item = usize>
    where
        Self: 'succ;

    /// Return the number of arcs in the graph.
    fn num_arcs(&self) -> usize;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn successors(&self, node_id: usize) -> <Self as RandomAccessGraph>::Successors<'_>;

    /// Return the number of successors of a node.
    fn outdegree(&self, _node_id: usize) -> usize {
        todo!();
        // self.successors(node_id).count()
    }

    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> bool {
        for neighbour_id in self.successors(src_node_id) {
            // found
            if neighbour_id == dst_node_id {
                return true;
            }
            // early stop
            if neighbour_id > dst_node_id {
                return false;
            }
        }
        false
    }
}

/// A struct used to make it easy to implement [a graph iterator](LendingIterator)
/// for a type that implements [`RandomAccessGraph`].
pub struct IteratorImpl<'node, G: RandomAccessGraph> {
    pub graph: &'node G,
    pub nodes: core::ops::Range<usize>,
}

impl<'node, 'succ, G: RandomAccessGraph> LendingIteratorItem<'succ> for IteratorImpl<'node, G> {
    type T = (usize, <G as RandomAccessGraph>::Successors<'succ>);
}

impl<'node, G: RandomAccessGraph> LendingIterator for IteratorImpl<'node, G> {
    #[inline(always)]
    fn next(&mut self) -> Option<Item<'_, Self>> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.graph.successors(node_id)))
    }
}

/// We iter on the node ids in a range so it is sorted
unsafe impl<'a, G: RandomAccessGraph> SortedIterator for IteratorImpl<'a, G> {}

/// A graph where each arc has a label
pub trait Labeled {
    /// The type of the label on the arcs
    type Label;
}

/// A trait to allow to ask for the label of the current node on a successors
/// iterator
pub trait LabeledSuccessors: Labeled + Iterator<Item = usize> {
    /// Get the label of the current node, this panics if called before the first
    fn label(&self) -> Self::Label;

    /// Wrap the `Self` into a [`LabeledSuccessorsWrapper`] to be able to iter
    /// on `(successor, label)` easily
    #[inline(always)]
    fn labeled(self) -> LabeledSuccessorsWrapper<Self>
    where
        Self: Sized,
    {
        LabeledSuccessorsWrapper(self)
    }
}

/// A trait to constraint the successors iterator to implement [`LabeledSuccessors`]
pub trait LabeledSequentialGraph: SequentialGraph + Labeled
where
    for<'a> Self::Successors<'a>: LabeledSuccessors<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: SequentialGraph + Labeled> LabeledSequentialGraph for G where
    for<'a> Self::Successors<'a>: LabeledSuccessors<Label = Self::Label>
{
}

/// A trait to constraint the successors iterator to implement [`LabeledSuccessors`]
pub trait LabeledRandomAccessGraph: RandomAccessGraph + Labeled
where
    for<'a> <Self as RandomAccessGraph>::Successors<'a>: LabeledSuccessors<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: RandomAccessGraph + Labeled> LabeledRandomAccessGraph for G where
    for<'a> <Self as RandomAccessGraph>::Successors<'a>: LabeledSuccessors<Label = Self::Label>
{
}

/// A graph that can be accessed both sequentially and randomly,
/// and which enumerates nodes and successors in increasing order.
pub trait Graph: SequentialGraph + RandomAccessGraph {}
/// Blanket implementation
impl<G: SequentialGraph + RandomAccessGraph> Graph for G {}

/// The same as [`Graph`], but with a label on each node.
pub trait LabeledGraph: LabeledSequentialGraph + LabeledRandomAccessGraph
where
    for<'a> <Self as SequentialGraph>::Successors<'a>: LabeledSuccessors<Label = Self::Label>,
    for<'a> <Self as RandomAccessGraph>::Successors<'a>: LabeledSuccessors<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: LabeledSequentialGraph + LabeledRandomAccessGraph> LabeledGraph for G
where
    for<'a> <Self as SequentialGraph>::Successors<'a>: LabeledSuccessors<Label = Self::Label>,
    for<'a> <Self as RandomAccessGraph>::Successors<'a>: LabeledSuccessors<Label = Self::Label>,
{
}

#[repr(transparent)]
/// A wrapper around a [`LabeledSuccessors`] to make it implement [`Iterator`]
/// with a tuple of `(successor, label)`
pub struct LabeledSuccessorsWrapper<I: LabeledSuccessors + Iterator<Item = usize>>(I);

impl<I: LabeledSuccessors + Iterator<Item = usize>> Iterator for LabeledSuccessorsWrapper<I> {
    type Item = (usize, I::Label);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|successor| (successor, self.0.label()))
    }
    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<I: LabeledSuccessors + Iterator<Item = usize> + ExactSizeIterator> ExactSizeIterator
    for LabeledSuccessorsWrapper<I>
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// We are transparent regarding the sortedness of the underlying iterator
unsafe impl<I: LabeledSuccessors + Iterator<Item = usize> + SortedSuccessors> SortedSuccessors
    for LabeledSuccessorsWrapper<I>
{
}
