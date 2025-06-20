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

use crate::{traits::LenderIntoIter, utils::Granularity};

use super::{LenderLabel, NodeLabelsLender, ParMapFold};

use core::ops::Range;
use dsi_progress_logger::prelude::*;
use impl_tools::autoimpl;
use lender::*;
use rayon::ThreadPool;
use std::rc::Rc;
use thiserror::Error;

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
///
/// The function [`eq_sorted`] can be used to check whether two
/// sorted labelings are equal.
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
    ///
    /// * `fold` - The function to fold the results obtained from each chunk. It
    ///   will be passed to the [`Iterator::fold`].
    ///
    /// * `granularity` - The granularity of parallel tasks.
    ///
    /// * `thread_pool` - The thread pool to use. The maximum level of
    ///   parallelism is given by the number of threads in the pool.
    ///
    /// * `pl` - An optional mutable reference to a progress logger.
    ///
    /// # Panics
    ///
    /// This method will panic if [`Granularity::node_granularity`] does.
    fn par_node_apply<
        A: Default + Send,
        F: Fn(Range<usize>) -> A + Sync,
        R: Fn(A, A) -> A + Sync,
    >(
        &self,
        func: F,
        fold: R,
        granularity: Granularity,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> A {
        let num_nodes = self.num_nodes();
        let node_granularity = granularity.node_granularity(num_nodes, self.num_arcs_hint());
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
    /// * `granularity` - The granularity of parallel tests.
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
        granularity: Granularity,
        deg_cumul: &D,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> A {
        FairChunks::new(
            granularity.arc_granularity(
                self.num_nodes(),
                Some(deg_cumul.get(deg_cumul.len() - 1) as u64),
            ),
            deg_cumul,
        )
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
}

/// Error types that can occur during graph equality checking.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum EqError {
    /// The graphs have different numbers of nodes.
    #[error("Different number of nodes: {first} != {second}")]
    NumNodes { first: usize, second: usize },

    /// The graphs have different numbers of arcs.
    #[error("Different number of arcs: {first} !={second}")]
    NumArcs { first: u64, second: u64 },

    /// The graphs have different successors for a specific node.
    #[error("Different successors for node {node}: at index {index} {first} != {second}")]
    Successors {
        node: usize,
        index: usize,
        first: String,
        second: String,
    },

    /// The graphs have different outdegrees for a specific node.
    #[error("Different outdegree for node {node}: {first} != {second}")]
    Outdegree {
        node: usize,
        first: usize,
        second: usize,
    },
}

#[doc(hidden)]
/// Checks whether two sorted successors lists are identical,
/// returning an appropriate error.
pub fn eq_succs<L: PartialEq + std::fmt::Debug>(
    node: usize,
    succ0: impl IntoIterator<Item = L>,
    succ1: impl IntoIterator<Item = L>,
) -> Result<(), EqError> {
    let mut succ0 = succ0.into_iter();
    let mut succ1 = succ1.into_iter();
    let mut index = 0;
    loop {
        match (succ0.next(), succ1.next()) {
            (None, None) => return Ok(()),
            (Some(s0), Some(s1)) => {
                if s0 != s1 {
                    return Err(EqError::Successors {
                        node,
                        index,
                        first: format!("{:?}", s0),
                        second: format!("{:?}", s1),
                    });
                }
            }
            (None, Some(_)) => {
                return Err(EqError::Outdegree {
                    node,
                    first: index,
                    second: index + 1 + succ1.count(),
                });
            }
            (Some(_), None) => {
                return Err(EqError::Outdegree {
                    node,
                    first: index + 1 + succ0.count(),
                    second: index,
                });
            }
        }
        index += 1;
    }
}

/// Checks if the two provided sorted labelings are equal.
///
/// Since graphs are labelings, this function can also be used to check whether
/// sorted graphs are equal. If the graphs are different, an [`EqError`] is
/// returned describing the first difference found.
pub fn eq_sorted<L0: SequentialLabeling, L1: SequentialLabeling<Label = L0::Label>>(
    l0: &L0,
    l1: &L1,
) -> Result<(), EqError>
where
    for<'a> L0::Lender<'a>: SortedLender,
    for<'a> L1::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'b, L0::Lender<'a>>: SortedIterator,
    for<'a, 'b> LenderIntoIter<'b, L0::Lender<'a>>: SortedIterator,
    L0::Label: PartialEq + std::fmt::Debug,
{
    if l0.num_nodes() != l1.num_nodes() {
        return Err(EqError::NumNodes {
            first: l0.num_nodes(),
            second: l1.num_nodes(),
        });
    }
    for_!(((node0, succ0), (node1, succ1)) in l0.iter().zip(l1.iter()) {
        debug_assert_eq!(node0, node1);
        eq_succs(node0, succ0, succ1)?;
    });
    Ok(())
}

/// Convenience type alias for the iterator over the labels of a node
/// returned by the [`iter_from`](SequentialLabeling::iter_from) method.
pub type Labels<'succ, 'node, S> =
    <<S as SequentialLabeling>::Lender<'node> as NodeLabelsLender<'succ>>::IntoIterator;

/// Marker trait for lenders returned by [`SequentialLabeling::iter`] yielding
/// node ids in ascending order.
///
/// The [`AssumeSortedLender`] type can be used to wrap a lender and
/// unsafely implement this trait.
///
/// # Safety
///
/// The first element of the pairs returned by the iterator must go from zero to
/// the [number of nodes](SequentialLabeling::num_nodes) of the graph, excluded.
///
/// # Examples
///
/// To bind the lender returned by [`SequentialLabeling::iter`] to implement this
/// trait, you must use higher-rank trait bounds:
/// ```rust
/// use webgraph::traits::*;
///
/// fn takes_graph_with_sorted_lender<G>(g: G) where
///     G: SequentialLabeling,
///     for<'a> G::Lender<'a>: SortedLender,
/// {
///     // ...
/// }
/// ```
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
/// The [`AssumeSortedIterator`] type can be used to wrap an iterator and
/// unsafely implement this trait.
///
/// # Safety
///
/// The labels returned by the iterator must be in the order in which they would
/// be if successors were returned in ascending order.
///
/// # Examples
///
/// To bind the iterators returned by the lender returned by
/// [`SequentialLabeling::iter`] to implement this trait, you must use
/// higher-rank trait bounds:
/// ```rust
/// use webgraph::traits::*;
///
/// fn takes_graph_with_sorted_iterators<G>(g: G) where
///     G: SequentialLabeling,
///     for<'a','b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
/// {
///     // ...
/// }
/// ```
pub unsafe trait SortedIterator: Iterator {}

/// A transparent wrapper for an [`Iterator`] unsafely implementing
/// [`SortedIterator`].
///
/// This wrapper is useful when an iterator is known to return labels in sorted
/// order, but the trait is not implemented, and it is not possible to implement
/// it directly because of the orphan rule.
pub struct AssumeSortedIterator<I> {
    iter: I,
}

impl<I> AssumeSortedIterator<I> {
    /// # Safety
    /// This is unsafe as the propose of this struct is to attach an unsafe
    /// trait to a struct that does not implement it.
    pub unsafe fn new(iter: I) -> Self {
        Self { iter }
    }
}

unsafe impl<I: Iterator> SortedIterator for AssumeSortedIterator<I> {}

impl<I: Iterator> Iterator for AssumeSortedIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for AssumeSortedIterator<I> {
    fn len(&self) -> usize {
        self.iter.len()
    }
}

/// A [`SequentialLabeling`] providing, additionally, random access to
/// the list of labels associated with a node.
///
/// The function [`check_impl`] can be used to check whether the
/// sequential and random-access implementations of a labeling are consistent.
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

/// Error types that can occur during checking the implementation of a random
/// access labeling.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CheckImplError {
    /// The number of nodes returned by [`iter`](SequentialLabeling::iter)
    /// is different from the number returned by
    /// [`num_nodes`](SequentialLabeling::num_nodes).
    #[error("Different number of nodes: {iter} (iter) != {method} (num_nodes)")]
    NumNodes { iter: usize, method: usize },

    /// The number of successors returned by [`iter`](SequentialLabeling::iter)
    /// is different from the number returned by
    /// [`num_arcs`](RandomAccessLabeling::num_arcs).
    #[error("Different number of nodes: {iter} (iter) != {method} (num_arcs)")]
    NumArcs { iter: u64, method: u64 },

    /// The two implementations return different labels for a specific node.
    #[error("Different successors for node {node}: at index {index} {sequential} (sequential) != {random_access} (random access)")]
    Successors {
        node: usize,
        index: usize,
        sequential: String,
        random_access: String,
    },

    /// The graphs have different outdegrees for a specific node.
    #[error("Different outdegree for node {node}: {sequential} (sequential) != {random_access} (random access)")]
    Outdegree {
        node: usize,
        sequential: usize,
        random_access: usize,
    },
}

/// Checks the sequential vs. random-access implementation of a sorted
/// random-access labeling.
///
/// Note that this method will check that the sequential and random-access
/// iterators on labels of each node are identical, and that the number of
/// nodes returned by the sequential iterator is the same as the number of
/// nodes returned by [`num_nodes`](SequentialLabeling::num_nodes).
pub fn check_impl<L: RandomAccessLabeling>(l: L) -> Result<(), CheckImplError>
where
    L::Label: PartialEq + std::fmt::Debug,
{
    let mut num_nodes = 0;
    let mut num_arcs: u64 = 0;
    for_!((node, succ_iter) in l.iter() {
        num_nodes += 1;
        let mut succ_iter = succ_iter.into_iter();
        let mut succ = l.labels(node).into_iter();
        let mut index = 0;
        loop {
            match (succ_iter.next(), succ.next()) {
                (None, None) => break,
                (Some(s0), Some(s1)) => {
                    if s0 != s1 {
                        return Err(CheckImplError::Successors {
                            node,
                            index,
                            sequential: format!("{:?}", s0),
                            random_access: format!("{:?}", s1),
                        });
                    }
                }
                (None, Some(_)) => {
                    return Err(CheckImplError::Outdegree {
                        node,
                        sequential: index,
                        random_access: index + 1 + succ.count(),
                    });
                }
                (Some(_), None) => {
                    return Err(CheckImplError::Outdegree {
                        node,
                        sequential: index + 1 + succ_iter.count(),
                        random_access: index,
                    });
                }
            }
            index += 1;
        }
        num_arcs += index as u64;
    });

    if num_nodes != l.num_nodes() {
        Err(CheckImplError::NumNodes {
            method: l.num_nodes(),
            iter: num_nodes,
        })
    } else if num_arcs != l.num_arcs() {
        Err(CheckImplError::NumArcs {
            method: l.num_arcs(),
            iter: num_arcs,
        })
    } else {
        Ok(())
    }
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
