/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Traits for access labelings, both sequentially and
in random-access fashion.

A *labeling* is the basic storage unit for graph data. It associates to
each node of a graph a list of labels. In the [sequential case](SequentialLabeling),
one can obtain a [lender](lender::Lender) that lends pairs given by a node
and an iterator on the associated labels. In the [random-access case](RandomAccessLabeling),
instead, one can get [an iterator on the labels associated with a node](RandomAccessLabeling::successors).
Labelings can be [zipped together](crate::label::Zip), obtaining a
new labeling whose labels are pairs.

The number of nodes *n* of the graph is returned by [`SequentialLabeling::num_nodes`],
and nodes identifier are in the interval [0 . . *n*).



*/

use core::{
    ops::Range,
    sync::atomic::{AtomicUsize, Ordering},
};
use dsi_progress_logger::prelude::*;
use impl_tools::autoimpl;
use lender::*;
use sux::traits::{IndexedDict, Succ};

/// Iteration on nodes and associated labels.
///
/// This trait is a [`Lender`] returning pairs given by a `usize` (a node of the
/// graph) and an [`IntoIterator`], specified by the associated type [`IntoIterator`],
/// over the labels associated with that node,
/// specified by the associated type [`Label`] (which is forced to be identical
/// to the associated type `Item` of the [`IntoIterator`]).
///
/// For those types we provide convenience type aliases [`LenderIntoIterator`],
/// [`LenderIntoIter`], and [`LenderLabel`].
///
/// ## Propagation of implicit bounds
///
/// The definition of this trait emerged from a [discussion on the Rust language
/// forum](https://users.rust-lang.org/t/more-help-for-more-complex-lifetime-situation/103821/10).
/// The purpose of the trait is to propagate the implicit
/// bound appearing in the definition [`Lender`] to the iterator returned
/// by the associated type [`IntoIterator`]. In this way, one can return iterators
/// depending on the internal state of the labeling. Without this additional trait, it
/// would be possible to return iterators whose state depends on the state of
/// the lender, but not on the state of the labeling.
pub trait NodeLabelsLender<'lend, __ImplBound: lender::ImplBound = lender::Ref<'lend, Self>>:
    Lender + Lending<'lend, __ImplBound, Lend = (usize, Self::IntoIterator)>
{
    type Label;
    type IntoIterator: IntoIterator<Item = Self::Label>;
}

/// Convenience type alias for the associated type `Label` of a [`NodeLabelsLender`].
pub type LenderLabel<'lend, L> = <L as NodeLabelsLender<'lend>>::Label;

/// Convenience type alias for the associated type `IntoIterator` of a [`NodeLabelsLender`].
pub type LenderIntoIterator<'lend, L> = <L as NodeLabelsLender<'lend>>::IntoIterator;

/// Convenience type alias for the [`Iterator`] returned by the `IntoIterator`
/// associated type of a [`NodeLabelsLender`].
pub type LenderIntoIter<'lend, L> =
    <<L as NodeLabelsLender<'lend>>::IntoIterator as IntoIterator>::IntoIter;

/// A labeling that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or that the labels of the successors will be returned
/// in any specified order.
///
/// The marker traits [`SortedIterator`] and [`SortedLabels`] can be used to
/// force these properties.
///
/// The iterator returned by [iter](SequentialLabeling::iter) is a [lender](Lender):
/// to access the next pair, you must have finished to use the previous one. You
/// can invoke [`Lender::into_iter`] to get a standard iterator, in general

/// at the cost of some allocation and copying.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S)]
pub trait SequentialLabeling {
    type Label;
    /// The type of the iterator over the successors of a node
    /// returned by [the iterator on the graph](SequentialGraph::Iterator).
    type Iterator<'node>: for<'all> NodeLabelsLender<'all, Label = Self::Label>
    where
        Self: 'node;

    /// Return the number of nodes in the graph.
    fn num_nodes(&self) -> usize;

    /// Return the number of arcs in the graph, if available.
    fn num_arcs_hint(&self) -> Option<u64> {
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

    /// Given a labeling, apply `func` to each chunk of nodes of size `granularity`
    /// in parallel, and reduce the results using `reduce`.
    fn par_node_apply<F, R, T>(
        &self,
        func: F,
        reduce: R,
        thread_pool: &rayon::ThreadPool,
        granularity: usize,
        pl: Option<&mut ProgressLogger>,
    ) -> T
    where
        F: Fn(Range<usize>) -> T + Send + Sync,
        R: Fn(T, T) -> T + Send + Sync,
        T: Send + Default,
    {
        let pl_lock = pl.map(std::sync::Mutex::new);
        let num_nodes = self.num_nodes();
        let num_scoped_threads = thread_pool
            .current_num_threads()
            .min(num_nodes / granularity + 1)
            .max(2)
            - 1;
        let next_node = AtomicUsize::new(0);

        thread_pool.scope(|scope| {
            let mut res = Vec::with_capacity(num_scoped_threads);
            for _ in 0..num_scoped_threads {
                // create a channel to receive the result
                let (tx, rx) = std::sync::mpsc::channel();
                res.push(rx);

                // create some references so that we can share them across threads
                let pl_lock_ref = &pl_lock;
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
                        if let Some(pl_lock) = pl_lock_ref {
                            pl_lock
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

    /// Given a labeling, apply `func` to each chunk of nodes of size `granularity`
    /// in parallel, and reduce the results using `reduce`.
    fn par_apply<F, R, T>(
        &self,
        func: F,
        reduce: R,
        thread_pool: &rayon::ThreadPool,
        granularity: usize,
        _deg_cumul_func: &(impl IndexedDict<Input = usize, Output = usize> + Succ),
        pl: Option<&mut ProgressLogger>,
    ) -> T
    where
        F: Fn(Range<usize>) -> T + Send + Sync,
        R: Fn(T, T) -> T + Send + Sync,
        T: Send + Default,
    {
        let pl_lock = pl.map(std::sync::Mutex::new);
        let num_nodes = self.num_nodes();
        let num_scoped_threads = thread_pool
            .current_num_threads()
            .min(num_nodes / granularity + 1)
            .max(2)
            - 1;
        let next_node = AtomicUsize::new(0);

        thread_pool.scope(|scope| {
            let mut res = Vec::with_capacity(num_scoped_threads);
            for _ in 0..num_scoped_threads {
                // create a channel to receive the result
                let (tx, rx) = std::sync::mpsc::channel();
                res.push(rx);

                // create some references so that we can share them across threads
                let pl_lock_ref = &pl_lock;
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
                        if let Some(pl_lock) = pl_lock_ref {
                            pl_lock
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

/// Marker trait for lenders returned by [`SequentialLabeling::iter`]
/// yielding node ids in ascending order.
///
/// # Safety
/// The first element of the pairs returned by the iterator must go from
/// zero to the [number of nodes](SequentialLabeling::num_nodes) of the graph, excluded.
pub unsafe trait SortedIterator: Lender {}

/// Marker trait for [`IntoIterator`]s yielding labels in the
/// order induced by enumerating the successors in ascending order.
///
/// # Safety
/// The labels returned by the iterator must be in the order in which
/// they would be if successors were returned in ascending order.
pub unsafe trait SortedLabels: IntoIterator {}

/// A [`SequentialLabeling`] providing, additionally, random access to
/// the list of labels associated with a node.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S)]
pub trait RandomAccessLabeling: SequentialLabeling {
    /// The type of the iterator over the labels of a node
    /// returned by [`labels`](RandomAccessLabeling::labels).
    type Labels<'succ>: IntoIterator<Item = <Self as SequentialLabeling>::Label>
    where
        Self: 'succ;

    /// Return the number of arcs in the graph.
    fn num_arcs(&self) -> u64;

    /// Return the labels associated with a node.
    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_>;

    /// Return the number of labels associated with a node.
    fn outdegree(&self, node_id: usize) -> usize;
}

/// A struct used to make it easy to implement sequential access
/// starting from random access.
///
/// Users can implement just random-access primitives and then
/// use this structure to implement sequential access.
pub struct IteratorImpl<'node, G: RandomAccessLabeling> {
    pub labeling: &'node G,
    pub nodes: core::ops::Range<usize>,
}

/// We iter on the node ids in a range so it is sorted
unsafe impl<'a, G: RandomAccessLabeling> SortedIterator for IteratorImpl<'a, G> {}

impl<'node, 'succ, G: RandomAccessLabeling> NodeLabelsLender<'succ> for IteratorImpl<'node, G> {
    type Label = G::Label;
    type IntoIterator = <G as RandomAccessLabeling>::Labels<'succ>;
}

impl<'node, 'succ, G: RandomAccessLabeling> Lending<'succ> for IteratorImpl<'node, G> {
    type Lend = (usize, <G as RandomAccessLabeling>::Labels<'succ>);
}

impl<'node, G: RandomAccessLabeling> Lender for IteratorImpl<'node, G> {
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.labeling.labels(node_id)))
    }
}
