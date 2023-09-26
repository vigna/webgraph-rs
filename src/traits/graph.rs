/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Basic traits to access graphs, both sequentially and randomly.

*/

use core::{
    ops::Range,
    sync::atomic::{AtomicUsize, Ordering},
};
use std::sync::Mutex;

use dsi_progress_logger::ProgressLogger;
use gat_lending_iterator::LendingIterator;

pub trait GraphIterator {
    type Successors<'a>: IntoIterator<Item = usize> + 'a
    where
        Self: 'a;

    fn next_inner(&mut self) -> Option<(usize, Self::Successors<'_>)>;
}

struct Adapter<I: GraphIterator>(I);

impl<I: GraphIterator> LendingIterator for Adapter<I> {
    type Item<'a> = (usize, <I as GraphIterator>::Successors<'a>)
    where Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.0.next_inner()
    }
}

/// A graph that can be accessed sequentially
pub trait SequentialGraph {
    /// Iterator over the nodes of the graph
    type Iterator<'a>: GraphIterator
    where
        Self: 'a;

    /// Get the number of nodes in the graph
    fn num_nodes(&self) -> usize;

    /// Get the number of arcs in the graph if available
    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    /// Get an iterator over the nodes of the graph
    fn iter_nodes(&self) -> Self::Iterator<'_> {
        self.iter_nodes_from(0)
    }

    /// Get an iterator over the nodes of the graph starting at `start_node`
    /// (included)
    fn iter_nodes_from(&self, from: usize) -> Self::Iterator<'_> {
        self.iter_nodes_from_inner(from)
    }

    /// Get an iterator over the nodes of the graph starting at `start_node`
    /// (included)
    fn iter_nodes_from_inner(&self, from: usize) -> Self::Iterator<'_>;

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

/// Marker trait for [graphs](Graph) whose [graph iterators](GraphIterator) returns
/// their nodes in sorted order.
///
/// # Safety
/// The first element of the pairs returned  by the iterator must be sorted.
pub unsafe trait SortedIterator: GraphIterator {}

/// Marker trait for for [graphs](Graph) whose successor iterators
/// are in stored order.
///
/// # Safety
/// The iterator on successors must be sorted.
pub unsafe trait SortedSuccessors: Iterator<Item = usize> {}

/// A graph providing random access.
pub trait RandomAccessGraph: SequentialGraph {
    /// Iterator over the successors of a node
    type Successors<'a>: IntoIterator<Item = usize> + 'a
    where
        Self: 'a;

    /// Get the number of arcs in the graph
    fn num_arcs(&self) -> usize;

    /// Get a sorted iterator over the neighbours node_id
    fn successors(&self, node_id: usize) -> Self::Successors<'_>;

    /// Get the number of outgoing edges of a node
    fn outdegree(&self, node_id: usize) -> usize {
        todo!();
        // self.successors(node_id).count()
    }

    /// Return if the given edge `src_node_id -> dst_node_id` exists or not
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

/// A struct used to implement [`GraphIterator`] trait for a struct that
/// implements [`RandomAccessGraph`].
pub struct GraphIteratorImpl<'a, G: RandomAccessGraph> {
    pub graph: &'a G,
    pub nodes: core::ops::Range<usize>,
}

impl<'a, G: RandomAccessGraph> GraphIterator for GraphIteratorImpl<'a, G> {
    type Successors<'b> = G::Successors<'b>
    where Self: 'b;

    #[inline(always)]
    fn next_inner(&mut self) -> Option<(usize, Self::Successors<'_>)> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.graph.successors(node_id)))
    }
}

/// We iter on the node ids in a range so it is sorted
unsafe impl<'a, G: RandomAccessGraph> SortedIterator for GraphIteratorImpl<'a, G> {}

/// A graph where each arc has a label
pub trait Labelled {
    /// The type of the label on the arcs
    type Label;
}
/* TODO
/// A trait to allow to ask for the label of the current node on a successors
/// iterator
pub trait LabelledIterator: Labelled + Iterator<Item = usize> {
    /// Get the label of the current node, this panics if called before the first
    fn label(&self) -> Self::Label;

    /// Wrap the `Self` into a [`LabelledIteratorWrapper`] to be able to iter
    /// on `(successor, label)` easily
    #[inline(always)]
    fn labelled(self) -> LabelledIteratorWrapper<Self>
    where
        Self: Sized,
    {
        LabelledIteratorWrapper(self)
    }
}

/// A trait to constraint the successors iterator to implement [`LabelledIterator`]
pub trait LabelledSequentialGraph: SequentialGraph + Labelled
where
    for<'a> Self::Iterator<'a>: LabelledIterator<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: SequentialGraph + Labelled> LabelledSequentialGraph for G where
    for<'a> Self::Iterator<'a>: LabelledIterator<Label = Self::Label>
{
}

/// A trait to constraint the successors iterator to implement [`LabelledIterator`]
pub trait LabelledRandomAccessGraph: RandomAccessGraph + Labelled
where
    for<'a> Self::Successors<'a>: LabelledIterator<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: RandomAccessGraph + Labelled> LabelledRandomAccessGraph for G where
    for<'a> Self::Successors<'a>: LabelledIterator<Label = Self::Label>
{
}

/// A graph that can be accessed both sequentially and randomly,
/// and which enumerates nodes and successors in increasing order.
pub trait Graph: SequentialGraph + RandomAccessGraph {}
/// Blanket implementation
impl<G: SequentialGraph + RandomAccessGraph> Graph for G {}

/// The same as [`Graph`], but with a label on each node.
pub trait LabelledGraph: LabelledSequentialGraph + LabelledRandomAccessGraph
where
    for<'a> Self::Iterator<'a>: LabelledIterator<Label = Self::Label>,
    for<'a> Self::Successors<'a>: LabelledIterator<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: LabelledSequentialGraph + LabelledRandomAccessGraph> LabelledGraph for G
where
    for<'a> Self::Iterator<'a>: LabelledIterator<Label = Self::Label>,
    for<'a> Self::Successors<'a>: LabelledIterator<Label = Self::Label>,
{
}

#[repr(transparent)]
/// A wrapper around a [`LabelledIterator`] to make it implement [`Iterator`]
/// with a tuple of `(successor, label)`
pub struct LabelledIteratorWrapper<I: LabelledIterator + Iterator<Item = usize>>(I);

impl<I: LabelledIterator + Iterator<Item = usize>> Iterator for LabelledIteratorWrapper<I> {
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

impl<I: LabelledIterator + Iterator<Item = usize> + ExactSizeIterator> ExactSizeIterator
    for LabelledIteratorWrapper<I>
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

*/
/*
/// We are transparent regarding the sortedness of the underlying iterator
unsafe impl<I: LabelledIterator + Iterator<Item = usize> + SortedIterator> SortedIterator
    for LabelledIteratorWrapper<I>
{
}
*/
