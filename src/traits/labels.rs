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
one can obtain a [lender](SequentialLabeling::iter) that lends pairs given by a node
and an iterator on the associated labels. In the [random-access case](RandomAccessLabeling),
instead, one can get [an iterator on the labels associated with a node](RandomAccessLabeling::labels).
Labelings can be [zipped together](crate::labels::Zip), obtaining a
new labeling whose labels are pairs.

The number of nodes *n* of the graph is returned by [`SequentialLabeling::num_nodes`],
and nodes identifier are in the interval [0 . . *n*).

*/

use super::{LenderLabel, NodeLabelsLender, ParMapFold};

use core::ops::Range;
use dsi_progress_logger::prelude::*;
use impl_tools::autoimpl;
use lender::*;
use rayon::ThreadPool;
use std::rc::Rc;

use sux::{traits::Succ, utils::FairChunks};

/// A labeling that can be accessed sequentially.
///
/// The iterator returned by [iter](SequentialLabeling::iter) is a
/// [lender](NodeLabelsLender): to access the next pair, you must have finished
/// to use the previous one. You can invoke [`Lender::copied`] to get a standard
/// iterator, at the cost of some allocation and copying.
///
/// Note that there is no guarantee that the lender will return nodes in
/// ascending order, or that the iterators on labels will return them in any
/// specified order.
///
/// The marker traits [`SortedLender`] and [`SortedIterator`] can be used to
/// force these properties. Note that [`SortedIterator`] implies that successors
/// are returned in ascending order, and labels are returned in the same order.
///
/// This trait provides two default methods,
/// [`par_apply`](SequentialLabeling::par_apply) and
/// [`par_node_apply`](SequentialLabeling::par_node_apply), that make it easy to
/// process in parallel the nodes of the labeling.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
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

    /// Return an iterator over the labeling.
    ///
    /// Iterators over the labeling return pairs given by a node of the graph
    /// and an [`IntoIterator`] over the labels.
    fn iter(&self) -> Self::Lender<'_> {
        self.iter_from(0)
    }

    /// Return an iterator over the labeling starting at `from` (included).
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
    fn par_node_apply<
        A: Default + Send,
        F: Fn(Range<usize>) -> A + Sync,
        R: Fn(A, A) -> A + Sync,
    >(
        &self,
        func: F,
        fold: R,
        node_granularity: usize,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> A {
        let num_nodes = self.num_nodes();
        (0..num_nodes.div_ceil(node_granularity))
            .map(|i| i * node_granularity..num_nodes.min((i + 1) * node_granularity))
            .par_map_fold_with(
                pl.clone(),
                |pl, range| {
                    let len = range.len();
                    let res = func(range);
                    pl.update_with_count(len);
                    res
                },
                fold,
                thread_pool,
            )
    }

    /// Apply `func` to each chunk of nodes containing approximately
    /// `arc_granularity` arcs in parallel and folds the results using `fold`.
    ///
    /// You have to provide the degree cumulative function of the graph (i.e.,
    /// the sequence 0, *d*₀, *d*₀ + *d*₁, ..., *a*, where *a* is the number of
    /// arcs in the graph) in a form that makes it possible to compute
    /// successors (for example, using the suitable `webgraph build` command).
    ///
    /// # Arguments
    ///
    /// * `func` - The function to apply to each chunk of nodes.
    ///
    /// * `fold` - The function to fold the results obtained from each chunk.
    ///   It will be passed to the [`Iterator::fold`].
    ///
    /// * `arc_granularity` - The tentative number of arcs to process in each
    ///   chunk; usually computed using [`crate::utils::Granularity`].
    ///
    /// * `deg_cumul_func` - The degree cumulative function of the graph.
    ///
    /// * `thread_pool` - The thread pool to use. The maximum level of
    ///   parallelism is given by the number of threads in the pool.
    ///
    /// * `pl` - A mutable reference to a concurrent progress logger.
    fn par_apply<
        F: Fn(Range<usize>) -> A + Sync,
        A: Default + Send,
        R: Fn(A, A) -> A + Sync,
        D: Succ<Input = usize, Output = usize>,
    >(
        &self,
        func: F,
        fold: R,
        arc_granularity: usize,
        deg_cumul: &D,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> A {
        FairChunks::new(arc_granularity, deg_cumul).par_map_fold_with(
            pl.clone(),
            |pl, range| {
                let len = range.len();
                let res = func(range);
                pl.update_with_count(len);
                res
            },
            fold,
            thread_pool,
        )
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

/// A transparent wrapper for a [`NodeLabelsLender`] unsafely implementing
/// [`SortedLender`].
///
/// This wrapper is useful when the underlying lender is known to return nodes
/// in ascending order, but the trait is not implemented, and it is not possible
/// to implement it directly because of the orphan rule.
pub struct AssumeSortedLender<L> {
    lender: L,
}

impl<L> AssumeSortedLender<L> {
    /// # Safety
    ///
    /// The argument must return nodes in ascending order.
    pub unsafe fn new(lender: L) -> Self {
        Self { lender }
    }
}

unsafe impl<L: Lender> SortedLender for AssumeSortedLender<L> {}

impl<'succ, L: Lender> Lending<'succ> for AssumeSortedLender<L> {
    type Lend = <L as Lending<'succ>>::Lend;
}

impl<L: Lender> Lender for AssumeSortedLender<L> {
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.lender.next()
    }
}

impl<L: ExactSizeLender> ExactSizeLender for AssumeSortedLender<L> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.lender.len()
    }
}

impl<'lend, L> NodeLabelsLender<'lend> for AssumeSortedLender<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

/// Marker trait for [`Iterator`]s yielding labels in the order induced by
/// enumerating the successors in ascending order.
///
/// # Safety
///
/// The labels returned by the iterator must be in the order in which they would
/// be if successors were returned in ascending order.
pub unsafe trait SortedIterator: Iterator {}

/// A wrapper to attach `SortedIterator` to an iterator. This is needed when
/// the iterator is not directly a `SortedIterator`, but it is known that it
/// returns elements in sorted order, e.g. like iterating on a vector that was
/// sorted.
pub struct SortedIter<I> {
    iter: I,
}

impl<I> SortedIter<I> {
    /// # Safety
    /// This is unsafe as the propose of this struct is to attach an unsafe
    /// trait to a struct that does not implement it.
    pub unsafe fn new(iter: I) -> Self {
        Self { iter }
    }
}

unsafe impl<I: Iterator> SortedIterator for SortedIter<I> {}

impl<I: Iterator> Iterator for SortedIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for SortedIter<I> {
    fn len(&self) -> usize {
        self.iter.len()
    }
}

/// A [`SequentialLabeling`] providing, additionally, random access to
/// the list of labels associated with a node.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
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
pub struct IteratorImpl<'node, G: RandomAccessLabeling> {
    pub labeling: &'node G,
    pub nodes: core::ops::Range<usize>,
}

unsafe impl<G: RandomAccessLabeling> SortedLender for IteratorImpl<'_, G> {}

impl<'succ, G: RandomAccessLabeling> NodeLabelsLender<'succ> for IteratorImpl<'_, G> {
    type Label = G::Label;
    type IntoIterator = <G as RandomAccessLabeling>::Labels<'succ>;
}

impl<'succ, G: RandomAccessLabeling> Lending<'succ> for IteratorImpl<'_, G> {
    type Lend = (usize, <G as RandomAccessLabeling>::Labels<'succ>);
}

impl<G: RandomAccessLabeling> Lender for IteratorImpl<'_, G> {
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.labeling.labels(node_id)))
    }
}
