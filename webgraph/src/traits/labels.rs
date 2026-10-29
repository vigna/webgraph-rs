/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Traits for accessing labelings, both sequentially and
//! in random-access fashion.
//!
//! A *labeling* is the basic storage unit for graph data. It associates to
//! each node of a graph a list of labels. In the [sequential case], one can
//! obtain a [lender] that lends pairs given by a node and an iterator on the
//! associated labels. In the [random-access case], instead, one can get
//! [an iterator on the labels associated with a node]. Labelings can be
//! [zipped together], obtaining a new labeling whose labels are pairs.
//!
//! The number of nodes *n* of the graph is returned by [`SequentialLabeling::num_nodes`],
//! and nodes identifier are in the interval [0 . . *n*).
//!
//!
//! [sequential case]: SequentialLabeling
//! [lender]: SequentialLabeling::iter
//! [random-access case]: RandomAccessLabeling
//! [an iterator on the labels associated with a node]: RandomAccessLabeling::labels
//! [zipped together]: crate::labels::Zip

use crate::{graphs::bvgraph::DCF, traits::LenderIntoIter, utils::Granularity};

use super::{LenderLabel, NodeLabelsLender, ParMapFold};

use core::ops::Range;
use dsi_progress_logger::prelude::*;
use impl_tools::autoimpl;
use lender::*;
use std::rc::Rc;
use sux::{
    dict::EliasFanoBuilder,
    traits::{IndexedSeq, Succ},
    utils::FairChunks,
};
use thiserror::Error;

/// A labeling that can be accessed sequentially.
///
/// The iterator returned by [`iter`] is a [lender]: to access the next
/// pair, you must have finished to use the previous one. You can invoke
/// [`Lender::copied`] to get a standard iterator, at the cost of some
/// allocation and copying.
///
/// It is suggested that all implementors of this trait also implement
/// [`IntoLender`] on a reference, returning the same lender as [`iter`].
/// This makes it possible to use the [`for_!`] macro to iterate over the
/// labeling (see the [module documentation] for an example).
///
/// Note that there is no guarantee that the lender will return nodes in
/// ascending order, or that the iterators on labels will return them in any
/// specified order.
///
/// The marker traits [`SortedLender`] and [`SortedIterator`] can be used to
/// force these properties. Note that [`SortedIterator`] implies that successors
/// are returned in ascending order, and labels are returned in the same order.
///
/// This trait provides two default methods, [`par_apply`] and
/// [`par_node_apply`], that make it easy to process in parallel the nodes
/// of the labeling.
///
/// The function [`eq_sorted`] can be used to check whether two
/// sorted labelings are equal.
///
/// [`iter`]: SequentialLabeling::iter
/// [lender]: NodeLabelsLender
/// [`for_!`]: lender::for_
/// [module documentation]: crate
/// [`par_apply`]: SequentialLabeling::par_apply
/// [`par_node_apply`]: SequentialLabeling::par_node_apply
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
pub trait SequentialLabeling {
    type Label;
    /// The type of [`Lender`] over the successors of a node returned by
    /// [`iter`].
    ///
    /// [`iter`]: SequentialLabeling::iter
    type Lender<'node>: for<'next> NodeLabelsLender<'next, Label = Self::Label>
    where
        Self: 'node;

    /// Returns the number of nodes in the graph.
    fn num_nodes(&self) -> usize;

    /// Returns the number of arcs in the graph, if available.
    fn get_num_arcs(&self) -> Option<u64> {
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
    /// Note that if the iterator [is not sorted], `from` is not the node id
    /// of the first node returned by the iterator, but just the starting
    /// point of the iteration
    ///
    /// [is not sorted]: SortedIterator
    fn iter_from(&self, from: usize) -> Self::Lender<'_>;

    /// Builds the degree cumulative function for this labeling.
    ///
    /// The degree cumulative function is the sequence 0, d₀, d₀ + d₁, … ,
    /// *m*, where *dₖ* is the number of arcs/labels of node *k* and *m* is the
    /// number of arcs/labels.
    ///
    /// # Panics
    ///
    /// Panics if [`get_num_arcs`] returns `None`.
    ///
    /// [`get_num_arcs`]: SequentialLabeling::get_num_arcs
    fn build_dcf(&self) -> DCF {
        let n = self.num_nodes();
        let num_arcs = self
            .get_num_arcs()
            .expect("build_dcf requires get_num_arcs()");
        let mut efb = EliasFanoBuilder::new(n + 1, num_arcs);
        efb.push(0);
        let mut cumul = 0u64;
        let mut lender = self.iter();
        while let Some((_node, succs)) = lender.next() {
            cumul += succs.into_iter().count() as u64;
            efb.push(cumul);
        }
        crate::graphs::bvgraph::dcf_from_builder(efb).unwrap()
    }

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
        pl: &mut impl ConcurrentProgressLog,
    ) -> A {
        let num_nodes = self.num_nodes();
        let node_granularity = granularity.node_granularity(num_nodes, self.get_num_arcs());
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
            )
    }

    /// Applies `func` to each chunk of nodes containing approximately
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
    /// * `granularity` - The granularity of parallel tasks.
    ///
    /// * `deg_cumul_func` - The degree cumulative function of the graph.
    ///
    /// * `pl` - A mutable reference to a concurrent progress logger.
    fn par_apply<
        F: Fn(Range<usize>) -> A + Sync,
        A: Default + Send,
        R: Fn(A, A) -> A + Sync,
        D: for<'a> Succ<Input = u64, Output<'a> = u64> + IndexedSeq,
    >(
        &self,
        func: F,
        fold: R,
        granularity: Granularity,
        deg_cumul: &D,
        pl: &mut impl ConcurrentProgressLog,
    ) -> A {
        FairChunks::new(
            granularity.arc_granularity(self.num_nodes(), Some(deg_cumul.get(deg_cumul.len() - 1)))
                as u64,
            deg_cumul,
        )
        .par_map_fold_with(
            pl.clone(),
            |pl, range: Range<usize>| {
                let len = range.len();
                let res = func(range);
                pl.update_with_count(len);
                res
            },
            fold,
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
    #[error("Different number of arcs: {first} != {second}")]
    NumArcs { first: u64, second: u64 },

    /// Iteration on the two graphs returned a different node.
    #[error("Different node: at index {index} {first} != {second}")]
    Node {
        index: usize,
        first: usize,
        second: usize,
    },

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

/// The first difference between two sequences.
enum Mismatch<T> {
    /// The sequences have different elements at position `index`.
    Element { index: usize, first: T, second: T },
    /// The sequences have different lengths.
    Length { first: usize, second: usize },
}

/// Compares two sequences, returning their length if they are equal, or
/// their first difference.
fn compare<T: PartialEq>(
    first: impl IntoIterator<Item = T>,
    second: impl IntoIterator<Item = T>,
) -> Result<usize, Mismatch<T>> {
    let mut first = first.into_iter();
    let mut second = second.into_iter();
    let mut index = 0;
    loop {
        match (first.next(), second.next()) {
            (None, None) => return Ok(index),
            (Some(x), Some(y)) => {
                if x != y {
                    return Err(Mismatch::Element {
                        index,
                        first: x,
                        second: y,
                    });
                }
            }
            (None, Some(_)) => {
                return Err(Mismatch::Length {
                    first: index,
                    second: index + 1 + second.count(),
                });
            }
            (Some(_), None) => {
                return Err(Mismatch::Length {
                    first: index + 1 + first.count(),
                    second: index,
                });
            }
        }
        index += 1;
    }
}

#[doc(hidden)]
/// Checks whether two sorted successors lists are identical,
/// returning an appropriate error.
pub fn eq_succs<L: PartialEq + std::fmt::Debug>(
    node: usize,
    succ0: impl IntoIterator<Item = L>,
    succ1: impl IntoIterator<Item = L>,
) -> Result<(), EqError> {
    match compare(succ0, succ1) {
        Ok(_) => Ok(()),
        Err(Mismatch::Element {
            index,
            first,
            second,
        }) => Err(EqError::Successors {
            node,
            index,
            first: format!("{first:?}"),
            second: format!("{second:?}"),
        }),
        Err(Mismatch::Length { first, second }) => Err(EqError::Outdegree {
            node,
            first,
            second,
        }),
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
    for<'a, 'b> LenderIntoIter<'b, L1::Lender<'a>>: SortedIterator,
    L0::Label: PartialEq + std::fmt::Debug,
{
    if l0.num_nodes() != l1.num_nodes() {
        return Err(EqError::NumNodes {
            first: l0.num_nodes(),
            second: l1.num_nodes(),
        });
    }
    for_!((index, ((node0, succ0), (node1, succ1))) in l0.iter().zip(l1.iter()).enumerate() {
        if node0 != node1 {
            return Err(EqError::Node {
                index,
                first: node0,
                second: node1,
            });
        }
        eq_succs(node0, succ0, succ1)?;
    });
    Ok(())
}

/// Convenience type alias for the iterator over the labels of a node
/// returned by the [`iter_from`] method.
///
/// [`iter_from`]: SequentialLabeling::iter_from
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
/// the [number of nodes] of the graph, excluded.
///
/// # Examples
///
/// To bind the lender returned by [`SequentialLabeling::iter`] to implement this
/// trait, you must use higher-rank trait bounds:
/// ```rust
/// use webgraph::traits::*;
///
/// fn takes_labeling_with_sorted_lender<G>(g: G) where
///     G: SequentialLabeling,
///     for<'a> G::Lender<'a>: SortedLender,
/// {
///     // ...
/// }
/// ```
///
/// [number of nodes]: SequentialLabeling::num_nodes
pub unsafe trait SortedLender: Lender {}

// SAFETY: taking a prefix of a sorted lender is still sorted.
unsafe impl<L: SortedLender> SortedLender for lender::Take<L> {}

/// A transparent wrapper for a [`NodeLabelsLender`] unsafely implementing
/// [`SortedLender`].
///
/// This wrapper is useful when the underlying lender is known to return nodes
/// in ascending order, but the trait is not implemented, and it is not possible
/// to implement it directly because of the orphan rule.
#[derive(Debug, Clone)]
#[repr(transparent)]
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

// SAFETY: the caller guarantees that the underlying lender yields items in
// sorted order.
unsafe impl<L: Lender> SortedLender for AssumeSortedLender<L> {}

impl<'succ, L: Lender> Lending<'succ> for AssumeSortedLender<L> {
    type Lend = <L as Lending<'succ>>::Lend;
}

impl<L: Lender> Lender for AssumeSortedLender<L> {
    // SAFETY: the lend is covariant as it directly delegates to the underlying
    // covariant lender L.
    unsafe_assume_covariance!();

    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.lender.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.lender.size_hint()
    }
}

impl<L: ExactSizeLender> ExactSizeLender for AssumeSortedLender<L> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.lender.len()
    }
}

impl<L: FusedLender> FusedLender for AssumeSortedLender<L> {}

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
/// fn takes_labeling_with_sorted_iterators<G>(g: G) where
///     G: SequentialLabeling,
///     for<'a, 'b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
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
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct AssumeSortedIterator<I> {
    iter: I,
}

impl<I> AssumeSortedIterator<I> {
    /// # Safety
    /// This is unsafe as the purpose of this struct is to attach an unsafe
    /// trait to a struct that does not implement it.
    pub unsafe fn new(iter: I) -> Self {
        Self { iter }
    }
}

// SAFETY: the caller guarantees that the underlying iterator yields items in
// sorted order.
unsafe impl<I: Iterator> SortedIterator for AssumeSortedIterator<I> {}

impl<I: Iterator> Iterator for AssumeSortedIterator<I> {
    type Item = I::Item;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.iter.count()
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for AssumeSortedIterator<I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<I: Iterator + core::iter::FusedIterator> core::iter::FusedIterator
    for AssumeSortedIterator<I>
{
}

/// A way to merge the labels of repeated arcs into a single label.
///
/// When [sorting labeled pairs], the same arc may appear several times with
/// different labels. A merger turns the labels of each arc into a single
/// label, possibly of a different type, using a *state*: labels are added to
/// states by [`AddLabel::add`], states are merged by [`merge`], and a fully
/// merged state is turned into the label of the arc by [`finish`].
///
/// Sorting happens in two stages, which use different parts of this
/// interface:
///
/// - **Batch stage**: pairs are sorted in memory in batches, which are then
///   written to disk. For each run of equal arcs in a batch, [`init`] is
///   called once on a state, and then [`add`] is called once for each label
///   of the run, including the first one; the state is then written to disk.
///   A single state is reused for all the runs of a batch.
/// - **Merge stage**: the batches are merged. For each arc, the state read
///   from the first batch containing the arc is merged with the states read
///   from the other batches containing the arc, and then [`finish`] is called
///   exactly once, producing the label of the arc.
///
/// In particular, [`finish`] is never called in the batch stage, and it is
/// called for every arc, repeated or not; [`init`] is not called in the
/// merge stage.
///
/// Mergers must satisfy the following laws:
///
/// - the state set by [`init`] is an identity for [`merge`];
/// - [`merge`] is associative;
/// - adding a label to a state gives the same result as merging the state
///   with the state obtained by adding the label to a state set by [`init`].
///
/// The order of the labels of an arc and the order in which states are
/// merged are unspecified. [`add`] and [`merge`] may depend on these orders
/// as long as [`finish`] removes the dependency: for example, a merger
/// collecting labels can push them in [`add`], append states in [`merge`],
/// and sort the result once in [`finish`].
///
/// [`merge`] and [`finish`] take states by value, so they can move or reuse
/// their content. All methods take `&self`, as a single merger is shared by
/// all sorting threads.
///
/// All methods except [`init`] receive the arc whose labels are being merged,
/// so that, for example, they can report it in log or panic messages.
///
/// Adding labels is the responsibility of the separate trait [`AddLabel`],
/// as the merge stage never sees the type of the labels being sorted.
///
/// # Examples
///
/// A merger counting the occurrences of each arc (see also
/// [`CountRepeats`]), applied by hand to an arc appearing twice in a batch
/// and once in another:
///
/// ```rust
/// use webgraph::traits::{AddLabel, MergeLabels};
///
/// struct Count;
///
/// impl MergeLabels for Count {
///     type State = u64;
///     type Output = u64;
///
///     fn merge(&self, _arc: (usize, usize), state: &mut u64, other: u64) {
///         *state += other;
///     }
///
///     fn finish(&self, _arc: (usize, usize), state: u64) -> u64 {
///         state
///     }
/// }
///
/// impl<L> AddLabel<L> for Count {
///     fn add(&self, _arc: (usize, usize), state: &mut u64, _label: L) {
///         *state += 1;
///     }
/// }
///
/// let arc = (0, 1);
/// // Batch stage
/// let mut first = 0;
/// Count.init(&mut first);
/// Count.add(arc, &mut first, 'a');
/// Count.add(arc, &mut first, 'b');
/// let mut second = 0;
/// Count.init(&mut second);
/// Count.add(arc, &mut second, 'c');
/// // Merge stage
/// Count.merge(arc, &mut first, second);
/// assert_eq!(Count.finish(arc, first), 3);
/// ```
///
/// A merger collecting the labels of each arc into a sorted boxed slice,
/// reusing the allocation of the state in the batch stage:
///
/// ```rust
/// use webgraph::traits::{AddLabel, MergeLabels};
///
/// struct Collect;
///
/// impl MergeLabels for Collect {
///     type State = Vec<u32>;
///     type Output = Box<[u32]>;
///
///     fn init(&self, state: &mut Vec<u32>) {
///         state.clear();
///     }
///
///     fn merge(&self, _arc: (usize, usize), state: &mut Vec<u32>, mut other: Vec<u32>) {
///         state.append(&mut other);
///     }
///
///     fn finish(&self, _arc: (usize, usize), mut state: Vec<u32>) -> Box<[u32]> {
///         state.sort_unstable();
///         state.into_boxed_slice()
///     }
/// }
///
/// impl AddLabel<u32> for Collect {
///     fn add(&self, _arc: (usize, usize), state: &mut Vec<u32>, label: u32) {
///         state.push(label);
///     }
/// }
///
/// let arc = (0, 1);
/// // Batch stage
/// let mut first = Vec::new();
/// Collect.init(&mut first);
/// Collect.add(arc, &mut first, 3);
/// Collect.add(arc, &mut first, 1);
/// let mut second = Vec::new();
/// Collect.init(&mut second);
/// Collect.add(arc, &mut second, 2);
/// // Merge stage
/// Collect.merge(arc, &mut first, second);
/// assert_eq!(&*Collect.finish(arc, first), &[1, 2, 3]);
/// ```
///
/// [sorting labeled pairs]: crate::graphs::par_sorted_graph::ParSortedLabeledGraphConf::merge_labels
/// [`init`]: Self::init
/// [`add`]: AddLabel::add
/// [`merge`]: Self::merge
/// [`finish`]: Self::finish
pub trait MergeLabels {
    /// The type of states.
    type State: Default;
    /// The type of the merged labels.
    type Output;

    /// Resets `state` to the empty state.
    ///
    /// The default implementation assigns `Self::State::default()`.
    /// Override it to reuse resources: for example, a merger collecting
    /// labels into a `Vec` can call `state.clear()`, keeping the allocation
    /// across runs of equal arcs.
    fn init(&self, state: &mut Self::State) {
        *state = Self::State::default();
    }

    /// Merges into `state`, a state of `arc`, another state `other` of
    /// `arc`.
    fn merge(&self, arc: (usize, usize), state: &mut Self::State, other: Self::State);

    /// Turns a fully merged state of `arc` into the label of `arc`.
    fn finish(&self, arc: (usize, usize), state: Self::State) -> Self::Output;
}

/// Adds labels of type `L` to the states of a [`MergeLabels`].
///
/// See [`MergeLabels`] for the semantics and the examples.
pub trait AddLabel<L>: MergeLabels {
    /// Adds `label`, a label of `arc`, to `state`, a state of `arc`.
    fn add(&self, arc: (usize, usize), state: &mut Self::State, label: L);
}

/// A merger keeping one of the labels of each arc.
///
/// Which label is kept is unspecified, as the order of the labels of an arc
/// is unspecified. The state and the output are the label type, which must
/// implement [`Default`]. [Deduplication] while sorting keeps one label
/// without this requirement.
///
/// # Examples
///
/// ```rust
/// use webgraph::traits::{AddLabel, KeepOne, MergeLabels};
///
/// let keep = KeepOne::new();
/// let mut state = 0_u32;
/// keep.init(&mut state);
/// keep.add((0, 1), &mut state, 7);
/// assert_eq!(keep.finish((0, 1), state), 7);
/// ```
///
/// [Deduplication]: crate::graphs::par_sorted_graph::ParSortedLabeledGraphConf::dedup
pub struct KeepOne<L>(core::marker::PhantomData<fn() -> L>);

impl<L> KeepOne<L> {
    /// Creates a new merger keeping one label.
    pub const fn new() -> Self {
        Self(core::marker::PhantomData)
    }
}

impl<L> Default for KeepOne<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L> Clone for KeepOne<L> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<L> Copy for KeepOne<L> {}

impl<L> core::fmt::Debug for KeepOne<L> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("KeepOne")
    }
}

impl<L: Default> MergeLabels for KeepOne<L> {
    type State = L;
    type Output = L;

    #[inline(always)]
    fn merge(&self, _arc: (usize, usize), _state: &mut L, _other: L) {}

    #[inline(always)]
    fn finish(&self, _arc: (usize, usize), state: L) -> L {
        state
    }
}

impl<L: Default> AddLabel<L> for KeepOne<L> {
    #[inline(always)]
    fn add(&self, _arc: (usize, usize), state: &mut L, label: L) {
        *state = label;
    }
}

/// A merger ignoring labels and counting the occurrences of each arc.
///
/// The state and the output are `u64`.
///
/// # Examples
///
/// ```rust
/// use webgraph::traits::{AddLabel, CountRepeats, MergeLabels};
///
/// let mut state = 0;
/// CountRepeats.init(&mut state);
/// CountRepeats.add((0, 1), &mut state, 'x');
/// CountRepeats.add((0, 1), &mut state, 'y');
/// CountRepeats.merge((0, 1), &mut state, 3);
/// assert_eq!(CountRepeats.finish((0, 1), state), 5);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct CountRepeats;

impl MergeLabels for CountRepeats {
    type State = u64;
    type Output = u64;

    #[inline(always)]
    fn merge(&self, _arc: (usize, usize), state: &mut u64, other: u64) {
        *state += other;
    }

    #[inline(always)]
    fn finish(&self, _arc: (usize, usize), state: u64) -> u64 {
        state
    }
}

impl<L> AddLabel<L> for CountRepeats {
    #[inline(always)]
    fn add(&self, _arc: (usize, usize), state: &mut u64, _label: L) {
        *state += 1;
    }
}

/// A [`SequentialLabeling`] providing, additionally, random access to
/// the list of labels associated with a node.
///
/// The function [`check_impl`] can be used to check whether the
/// sequential and random-access implementations of a labeling are consistent.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
pub trait RandomAccessLabeling: SequentialLabeling {
    /// The type of the iterator over the labels of a node returned by
    /// [`labels`].
    ///
    /// [`labels`]: RandomAccessLabeling::labels
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

/// A trait providing parallel iteration over intervals of nodes that can be
/// efficient when used in parallel.
///
/// The purpose of this trait is to make available internal parallelism of a
/// graph representation. While a [`SplitLabeling`] makes it possible to split a
/// graph at will, but it does not provide guarantees on the efficiency of the
/// process, an [`IntoParLenders`] provides in constant time a sequence of
/// lenders that can be used in parallel computations, and that are expected to
/// be efficient when used in parallel.
///
/// [Random-access labelings] can usually implement this trait easily, and
/// [split labelings] can just delegate to their splitting methods. However,
/// there are cases, such as graphs resulting from [sorting], in
/// which the parallelism is inherent and not under control of the caller, albeit
/// in general there is a way to control such parallelism at construction time.
///
/// Compressors should provide methods that accept an [`IntoParLenders`] and use
/// the lenders returned by [`into_par_iters`] to process the graph in parallel.
///
/// There are two typical implementations of this trait:
///
/// - on a type: for one-off graphs (e.g., [`ParSortedGraph`]) that are
///   consumed once and then discarded;
///
/// - on a reference: for graphs that can be iterated over multiple times.
///
/// Some types have both implementations, with the same intended semantics of
/// the two implementations of [`IntoIterator`] on vectors and on references to
/// vectors.
///
/// [`into_par_iters`]: IntoParLenders::into_par_lenders
/// [`SplitLabeling`]: crate::traits::SplitLabeling
/// [`Random-access labelings`]: RandomAccessLabeling
/// [split labelings]: crate::traits::SplitLabeling
/// [sorting]: crate::graphs::par_sorted_graph::ParSortedGraph
/// [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
pub trait IntoParLenders {
    /// The type of [`Lender`] over the successors of a node returned by
    /// [`into_par_lenders`].
    ///
    /// [`into_par_lenders`]: Self::into_par_lenders
    type ParLender: for<'next> NodeLabelsLender<'next> + Send + Sync;

    /// Returns in constant time a sequence of lenders and cutpoints that can be
    /// used in parallel computations.
    ///
    /// The cutpoints are a non-decreasing sequence of node ids with at least
    /// two elements. They define `n` − 1 segments, where `n` is the number of
    /// cutpoints, and the `i`-th segment covers nodes in [`cutpoints[i]` . .
    /// `cutpoints[i + 1]`). There are as many lenders as segments, and the
    /// `i`-th lender yields nodes from the `i`-th segment.
    fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>);
}

/// Implements [`IntoParLenders`] for a type that already implements
/// [`SplitLabeling`], using the split lenders as the parallel lenders.
///
/// The macro requires that the split lenders satisfies the additional bounds
/// `ExactSizeLender + FusedLender`.
///
/// # Usage
///
/// ```ignore
/// impl_parallel_from_split!(
///     [F: RandomAccessDecoderFactory]
///     BvGraph<F>
///     [for<'a> <F as RandomAccessDecoderFactory>::Decoder<'a>: Send + Sync]
/// );
/// ```
///
/// The three arguments are:
/// 1. Generics (in brackets), or `[]` for none.
/// 2. The type to implement `IntoParLenders` for.
/// 3. Where clauses (in brackets), or `[]` for none.
///
/// [`SplitLabeling`]: crate::traits::SplitLabeling
#[macro_export]
macro_rules! impl_parallel_from_split {
    (
        [$($gen:tt)*]
        $ty:ty
        [$($where_clause:tt)*]
    ) => {
        impl<'__graph, $($gen)*> $crate::traits::IntoParLenders for &'__graph $ty
        where $($where_clause)*
        {
            type ParLender
                = <Self as $crate::traits::SplitLabeling>::SplitLender<'__graph>;

            fn into_par_lenders(self) -> (Box<[Self::ParLender]>, Box<[usize]>) {
                use $crate::traits::{SequentialLabeling, SplitLabeling};
                let cutpoints = $crate::traits::split::uniform_cutpoints(
                    self.num_nodes(),
                    rayon::current_num_threads(),
                );
                let lenders: Box<[_]> = self.split_iter_at(cutpoints.clone()).into_iter().collect();
                (lenders, cutpoints.collect())
            }
        }
    };
}

/// Error types that can occur during checking the implementation of a random
/// access labeling.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CheckImplError {
    /// The number of nodes returned by [`iter`] is different from the
    /// number returned by [`num_nodes`].
    ///
    /// [`iter`]: SequentialLabeling::iter
    /// [`num_nodes`]: SequentialLabeling::num_nodes
    #[error("Different number of nodes: {iter} (iter) != {method} (num_nodes)")]
    NumNodes { iter: usize, method: usize },

    /// The number of successors returned by [`iter`] is different from the
    /// number returned by [`num_arcs`].
    ///
    /// [`iter`]: SequentialLabeling::iter
    /// [`num_arcs`]: RandomAccessLabeling::num_arcs
    #[error("Different number of arcs: {iter} (iter) != {method} (num_arcs)")]
    NumArcs { iter: u64, method: u64 },

    /// The two implementations return different labels for a specific node.
    #[error(
        "Different successors for node {node}: at index {index} {sequential} (sequential) != {random_access} (random access)"
    )]
    Successors {
        node: usize,
        index: usize,
        sequential: String,
        random_access: String,
    },

    /// The graphs have different outdegrees for a specific node.
    #[error(
        "Different outdegree for node {node}: {sequential} (sequential) != {random_access} (random access)"
    )]
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
/// nodes returned by [`num_nodes`].
///
/// [`num_nodes`]: SequentialLabeling::num_nodes
pub fn check_impl<L: RandomAccessLabeling>(l: L) -> Result<(), CheckImplError>
where
    L::Label: PartialEq + std::fmt::Debug,
{
    let mut num_nodes = 0;
    let mut num_arcs: u64 = 0;
    for_!((node, succ_iter) in l.iter() {
        num_nodes += 1;
        num_arcs += compare(succ_iter, l.labels(node)).map_err(|mismatch| match mismatch {
            Mismatch::Element {
                index,
                first,
                second,
            } => CheckImplError::Successors {
                node,
                index,
                sequential: format!("{first:?}"),
                random_access: format!("{second:?}"),
            },
            Mismatch::Length { first, second } => CheckImplError::Outdegree {
                node,
                sequential: first,
                random_access: second,
            },
        })? as u64;
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
/// Users can implement just random-access primitives and then use this
/// structure to implement the lender returned by [`iter_from`] by just
/// returning an instance of this struct.
///
/// [`iter_from`]: SequentialLabeling::iter_from
pub struct LenderImpl<'node, G: RandomAccessLabeling> {
    /// The underlying random-access labeling.
    pub labeling: &'node G,
    /// The range of nodes to iterate over.
    pub nodes: core::ops::Range<usize>,
}

// SAFETY: RandomAccessLabeling::labels returns a sorted iterator.
unsafe impl<G: RandomAccessLabeling> SortedLender for LenderImpl<'_, G> {}

impl<'succ, G: RandomAccessLabeling> NodeLabelsLender<'succ> for LenderImpl<'_, G> {
    type Label = G::Label;
    type IntoIterator = <G as RandomAccessLabeling>::Labels<'succ>;
}

impl<'succ, G: RandomAccessLabeling> Lending<'succ> for LenderImpl<'_, G> {
    type Lend = (usize, <G as RandomAccessLabeling>::Labels<'succ>);
}

impl<G: RandomAccessLabeling> Lender for LenderImpl<'_, G> {
    // SAFETY: the lend is covariant as it returns labels from a
    // RandomAccessLabeling, which are expected to be covariant in the lifetime.
    unsafe_assume_covariance!();

    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.labeling.labels(node_id)))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.nodes.len(), Some(self.nodes.len()))
    }
}

impl<G: RandomAccessLabeling> ExactSizeLender for LenderImpl<'_, G> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl<G: RandomAccessLabeling> FusedLender for LenderImpl<'_, G> {}
