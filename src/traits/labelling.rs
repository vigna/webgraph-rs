/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Traits for access labellings, both sequentially and
in random-access fashion.

A *labelling* is the basic storage unit for graph data. It associates to
each node of a graph a list of labels. In the [sequential case](SequentialLabelling),
one can obtain a [lender](lender::Lender) that lends pairs given by a node
and an iterator on the associated labels. In the [random-access case](RandomAccessLabelling),
instead, one can get [an iterator on the labels associated with a node](RandomAccessLabelling::successors).

The number of nodes *n* of the graph is returned by [`SequentialLabelling::num_nodes`],
and nodes identifier are in the interval [0 . . *n*).

Labellings can be [zipped together](crate::utils::Zip), obtaining a
new labelling whose labels are pairs.

*/

use core::{
    ops::Range,
    sync::atomic::{AtomicUsize, Ordering},
};
use dsi_progress_logger::*;
use lender::*;
use std::sync::Mutex;

use crate::Tuple2;

pub trait NodeLabelsLending<'lend, __ImplBound: lender::ImplBound = lender::Ref<'lend, Self>>:
    Lending<
    'lend,
    __ImplBound,
    Lend = (
        usize,
        <Self as NodeLabelsLending<'lend, __ImplBound>>::IntoIterator,
    ),
>
where
    <Self as Lending<'lend, __ImplBound>>::Lend: Tuple2,
{
    type Item;
    type IntoIterator: IntoIterator<Item = Self::Item>;
}

pub type LendingItem<'lend, L> = <L as NodeLabelsLending<'lend>>::Item;
pub type LendingIntoIterator<'lend, L> = <L as NodeLabelsLending<'lend>>::IntoIterator;
pub type LendingIntoIter<'lend, L> =
    <<L as NodeLabelsLending<'lend>>::IntoIterator as IntoIterator>::IntoIter;

pub type Labels<'succ, 'node, S> =
    <<S as SequentialLabelling>::Iterator<'node> as NodeLabelsLending<'succ>>::IntoIterator;

/// A labelling that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or that the labels of the successors will be returned
/// in any specified order.
///
/// TODO
/// The marker traits [SortedIterator] and [SortedSuccessors] can be used to
/// force these properties.
///
/// The iterator returned by [iter](SequentialGraph::iter) is a [lender](Lender):
/// to access the next pair, you must have finished to use the previous one. You
/// can invoke [`Lender::into_iter`] to get a standard iterator, in general
/// at the cost of some allocation and copying.
pub trait SequentialLabelling {
    type Label;
    /// The type of the iterator over the successors of a node
    /// returned by [the iterator on the graph](SequentialGraph::Iterator).
    type Iterator<'node>: Lender + for<'all> NodeLabelsLending<'all, Item = Self::Label>
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
        pl: Option<&mut ProgressLogger>,
    ) -> T
    where
        F: Fn(Range<usize>) -> T + Send + Sync,
        R: Fn(T, T) -> T + Send + Sync,
        T: Send + Default,
    {
        let pl_lock = pl.map(Mutex::new);
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

/// A [sequential graph](SequentialGraph) providing, additionally, random access to successor lists.
pub trait RandomAccessLabelling: SequentialLabelling {
    /// The type of the iterator over the successors of a node
    /// returned by [successors](RandomAccessGraph::successors).
    type Successors<'succ>: IntoIterator<Item = <Self as SequentialLabelling>::Label>
    where
        Self: 'succ;

    /// Return the number of arcs in the graph.
    fn num_arcs(&self) -> usize;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Successors<'_>;

    /// Return the number of successors of a node.
    fn outdegree(&self, node_id: usize) -> usize;
}

/// A struct used to make it easy to implement [a graph iterator](Lender)
/// for a type that implements [`RandomAccessGraph`].
pub struct IteratorImpl<'node, G: RandomAccessLabelling> {
    pub labelling: &'node G,
    pub nodes: core::ops::Range<usize>,
}

impl<'node, 'succ, G: RandomAccessLabelling> Lending<'succ> for IteratorImpl<'node, G> {
    type Lend = (usize, <G as RandomAccessLabelling>::Successors<'succ>);
}

impl<'node, 'succ, G: RandomAccessLabelling> NodeLabelsLending<'succ> for IteratorImpl<'node, G> {
    type Item = G::Label;
    type IntoIterator = <G as RandomAccessLabelling>::Successors<'succ>;
}

impl<'node, G: RandomAccessLabelling> Lender for IteratorImpl<'node, G> {
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.labelling.successors(node_id)))
    }
}
