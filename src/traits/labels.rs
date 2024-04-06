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
Labelings can be [zipped together](crate::labels::Zip), obtaining a
new labeling whose labels are pairs.

The number of nodes *n* of the graph is returned by [`SequentialLabeling::num_nodes`],
and nodes identifier are in the interval [0 . . *n*).

*/

use super::NodeLabelsLender;

use core::{
    ops::Range,
    sync::atomic::{AtomicUsize, Ordering},
};
use dsi_progress_logger::prelude::*;
use impl_tools::autoimpl;
use lender::*;
use sux::traits::Succ;
use mem_dbg::{MemDbg, MemSize};
use epserde::Epserde;

/// A labeling that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or that the labels of the successors will be returned in
/// any specified order.
///
/// The marker traits [`SortedIterator`] and [`SortedLabels`] can be used to
/// force these properties.
///
/// The iterator returned by [iter](SequentialLabeling::iter) is a
/// [lender](NodeLabelsLender): to access the next pair, you must have finished
/// to use the previous one. You can invoke [`Lender::into_iter`] to get a
/// standard iterator, in general at the cost of some allocation and copying.
///
/// This trait provides two default methods,
/// [`par_apply`](SequentialLabeling::par_apply) and
/// [`par_node_apply`](SequentialLabeling::par_node_apply), that make it easy to
/// process in parallel the nodes of the labeling.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S)]
pub trait SequentialLabeling {
    type Label;
    /// The type of [`Lender`] over the successors of a node
    /// returned by [`iter`](SequentialLabeling::iter).
    type Lender<'node>: for<'next> NodeLabelsLender<'next, Label = Self::Label>
    where
        Self: 'node;

    /// Returns the number of nodes in the graph.
    fn num_nodes(&self) -> usize;

    /// Returns the number of arcs in the graph, if available.
    fn num_arcs_hint(&self) -> Option<u64> {
        None
    }

    /// Returns an iterator over the labeling.
    ///
    /// Iterators over the labeling return pairs given by a node of the graph
    /// and an [`IntoIterator`] over the labels.
    fn iter(&self) -> Self::Lender<'_> {
        self.iter_from(0)
    }

    /// Returns an iterator over the labeling starting at `from` (included).
    ///
    /// Note that if the iterator [is not sorted](SortedIterator), `from` is not
    /// the node id of the first node returned by the iterator, but just the
    /// starting point of the iteration
    fn iter_from(&self, from: usize) -> Self::Lender<'_>;

    /// Applies `func` to each chunk of nodes of size `node_granularity` in
    /// parallel, and folds the results using `fold`.
    ///
    /// # Arguments
    ///
    /// * `func` - The function to apply to each chunk of nodes.
    /// * `fold` - The function to fold the results obtained from each chunk. It
    ///    will be passed to the [`Iterator::fold`].
    /// * `node_granularity` - The number of nodes to process in each chunk.
    /// * `thread_pool` - The thread pool to use. The maximum level of
    ///   parallelism is given by the number of threads in the pool.
    /// * `pl` - An optional mutable reference to a progress logger.

    fn par_node_apply<F, R, T, A>(
        &self,
        func: F,
        fold: R,
        node_granularity: usize,
        thread_pool: &rayon::ThreadPool,
        pl: Option<&mut ProgressLogger>,
    ) -> A
    where
        F: Fn(Range<usize>) -> T + Send + Sync,
        R: Fn(A, T) -> A + Send + Sync,
        T: Send,
        A: Default + Send,
    {
        let pl_lock = pl.map(std::sync::Mutex::new);
        let num_nodes = self.num_nodes();
        let num_scoped_threads = thread_pool
            .current_num_threads()
            .min(num_nodes / node_granularity)
            .max(1);

        let next_node = AtomicUsize::new(0);

        // create a channel to receive the result
        let (tx, rx) = std::sync::mpsc::channel();
        thread_pool.in_place_scope(|scope| {
            for _ in 0..num_scoped_threads {
                // create some references so that we can share them across threads
                let pl_lock = &pl_lock;
                let next_node = &next_node;
                let func = &func;
                let tx = tx.clone();

                scope.spawn(move |_| {
                    loop {
                        // compute the next chunk of nodes to process
                        let start_pos = next_node.fetch_add(node_granularity, Ordering::Relaxed);
                        let end_pos = (start_pos + node_granularity).min(num_nodes);
                        // exit if done
                        if start_pos >= num_nodes {
                            break;
                        }
                        // apply the function and send the result
                        tx.send(func(start_pos..end_pos)).unwrap();

                        // update the progress logger if specified
                        if let Some(pl_lock) = pl_lock {
                            pl_lock
                                .lock()
                                .unwrap()
                                .update_with_count((start_pos..end_pos).len());
                        }
                    }
                });
            }
            drop(tx);

            rx.iter().fold(A::default(), fold)
        })
    }

    /// Applies `func` to each chunk of nodes containing approximately
    /// `arc_granularity` arcs in parallel, and folds the results using `fold`.
    /// You have to provide the degree cumulative function of the graph (i.e.,
    /// the sequence 0, *d*₀, *d*₀ + *d*₁, ..., *a*, where *a* is the number of
    /// arcs in the graph) in a form that makes it possible to compute
    /// successors (for example, using the suitable `webgraph build` command).
    ///
    /// # Arguments
    ///
    /// * `func` - The function to apply to each chunk of nodes.
    /// * `fold` - The function to fold the results obtained from each chunk.
    ///   It will be passed to the [`Iterator::fold`].
    /// * `arc_granularity` - The tentative number of arcs to process in each
    ///   chunk.
    /// * `deg_cumul_func` - The degree cumulative function of the graph.
    /// * `thread_pool` - The thread pool to use. The maximum level of
    ///   parallelism is given by the number of threads in the pool.
    /// * `pl` - An optional mutable reference to a progress logger.

    fn par_apply<F, R, T, A>(
        &self,
        func: F,
        fold: R,
        arc_granularity: usize,
        deg_cumul: &(impl Succ<Input = usize, Output = usize> + Send + Sync),
        thread_pool: &rayon::ThreadPool,
        pl: Option<&mut ProgressLogger>,
    ) -> A
    where
        F: Fn(Range<usize>) -> T + Send + Sync,
        R: Fn(A, T) -> A + Send + Sync,
        T: Send,
        A: Default + Send,
    {
        let pl_lock = pl.map(std::sync::Mutex::new);
        let num_nodes = self.num_nodes();
        let num_arcs = self.num_arcs_hint().unwrap();
        let num_scoped_threads = thread_pool
            .current_num_threads()
            .min((num_arcs / arc_granularity as u64) as usize)
            .max(1);
        let next_node_next_arc = std::sync::Mutex::new((0, 0));
        let num_arcs = deg_cumul.get(num_nodes);
        if let Some(num_arcs_hint) = self.num_arcs_hint() {
            assert_eq!(num_arcs_hint, num_arcs as u64);
        }

        thread_pool.in_place_scope(|scope| {
            // create a channel to receive the result
            let (tx, rx) = std::sync::mpsc::channel();

            for _ in 0..num_scoped_threads {
                // create some references so that we can share them across threads
                let pl_lock = &pl_lock;
                let next_node_next_arc = &next_node_next_arc;
                let func = &func;
                let tx = tx.clone();

                scope.spawn(move |_| {
                    loop {
                        let (start_pos, end_pos);
                        {
                            let mut next_node_next_arc = next_node_next_arc.lock().unwrap();
                            let (mut next_node, mut next_arc) = *next_node_next_arc;

                            if next_node >= num_nodes {
                                break;
                            }

                            start_pos = next_node;
                            let target = next_arc + arc_granularity;
                            if target >= num_arcs {
                                next_node = num_nodes;
                            } else {
                                (next_node, next_arc) = deg_cumul.succ(&target).unwrap();
                            }
                            end_pos = next_node;
                            *next_node_next_arc = (next_node, next_arc);
                        }

                        // apply the function and send the result
                        tx.send(func(start_pos..end_pos)).unwrap();

                        // update the progress logger if specified
                        if let Some(pl_lock) = pl_lock {
                            pl_lock
                                .lock()
                                .unwrap()
                                .update_with_count((start_pos..end_pos).len());
                        }
                    }
                });
            }
            drop(tx);

            rx.iter().fold(A::default(), fold)
        })
    }
}

/// Convenience type alias for the iterator over the labels of a node
/// returned by the [`iter_from`](SequentialLabeling::iter_from) method.
pub type Labels<'succ, 'node, S> =
    <<S as SequentialLabeling>::Lender<'node> as NodeLabelsLender<'succ>>::IntoIterator;

/// Marker trait for lenders returned by [`SequentialLabeling::iter`] yielding
/// node ids in ascending order.
///
/// # Safety
///
/// The first element of the pairs returned by the iterator must go from zero to
/// the [number of nodes](SequentialLabeling::num_nodes) of the graph, excluded.
pub unsafe trait SortedLender: Lender {}

/// Marker trait for [`Iterator`]s yielding labels in the order induced by
/// enumerating the successors in ascending order.
///
/// # Safety
///
/// The labels returned by the iterator must be in the order in which they would
/// be if successors were returned in ascending order.
pub unsafe trait SortedIterator: Iterator {}

/// A [`SequentialLabeling`] providing, additionally, random access to
/// the list of labels associated with a node.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S)]
pub trait RandomAccessLabeling: SequentialLabeling {
    /// The type of the iterator over the labels of a node
    /// returned by [`labels`](RandomAccessLabeling::labels).
    type Labels<'succ>: IntoIterator<Item = <Self as SequentialLabeling>::Label>
    where
        Self: 'succ;

    /// Returns the number of arcs in the graph.
    fn num_arcs(&self) -> u64;

    /// Returns the labels associated with a node.
    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_>;

    /// Returns the number of labels associated with a node.
    fn outdegree(&self, node_id: usize) -> usize;
}

/// A struct used to make it easy to implement sequential access
/// starting from random access.
///
/// Users can implement just random-access primitives and then
/// use this structure to implement sequential access.
#[derive(Clone, Debug, PartialEq, Eq, MemDbg, MemSize, Epserde)]
pub struct IteratorImpl<'node, G: RandomAccessLabeling> {
    pub labeling: &'node G,
    pub nodes: core::ops::Range<usize>,
}

unsafe impl<'a, G: RandomAccessLabeling> SortedLender for IteratorImpl<'a, G> {}

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
