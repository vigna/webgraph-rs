/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Traits and basic implementations to support parallel iteration by splitting
//! the [iterator] of a labeling into multiple iterators.
//!
//! [iterator]: SequentialLabeling::Lender

use std::rc::Rc;

use impl_tools::autoimpl;

use super::{labels::SequentialLabeling, lenders::NodeLabelsLender};
use sux::{
    traits::{IndexedSeq, SuccUnchecked},
    utils::FairChunks,
};

/// A trait providing methods to split the labeling [iterator] into multiple
/// thread-safe parts.
///
/// The main method is [`split_iter_at`], which takes a sequence of cutpoints
/// and splits the iteration at those points. Each cutpoint is a node id; the
/// sequence must be non-decreasing and contain at least two elements.
///
/// The convenience method [`split_iter`] provides a default implementation
/// that splits the iteration into `n` approximately equal parts. It is
/// implemented in terms of [`split_iter_at`], so implementors only need to
/// provide the latter.
///
/// Note that the parts are required to be [`Send`] and [`Sync`], so that they
/// can be safely shared among threads.
///
/// Due to some limitations of the current Rust type system, we cannot provide
/// blanket implementations for this trait. However, we provide ready-made
/// implementations for the [sequential] and [random-access] cases. To use
/// them, you must implement the trait by specifying the associated types
/// `SplitLender` and `IntoIterator`, and then just return a [`seq::Iter`] or
/// [`ra::Iter`] structure from [`split_iter_at`].
///
/// [iterator]: SequentialLabeling::Lender
/// [`split_iter_at`]: SplitLabeling::split_iter_at
/// [`split_iter`]: SplitLabeling::split_iter
/// [sequential]: seq
/// [random-access]: ra
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
pub trait SplitLabeling: SequentialLabeling {
    type SplitLender<'a>: for<'next> NodeLabelsLender<'next, Label = <Self as SequentialLabeling>::Label>
        + Send
        + Sync
    where
        Self: 'a;

    type IntoIterator<'a>: IntoIterator<Item = Self::SplitLender<'a>>
    where
        Self: 'a;

    /// Splits the labeling iterator at the given cutpoints.
    ///
    /// The cutpoints are a non-decreasing sequence of node ids with at least
    /// two elements. They define `n` − 1 segments, where `n` is the number of
    /// cutpoints, and the `i`-th segment covers nodes in [`cutpoints[i]` . . `cutpoints[i + 1]`).
    fn split_iter_at(&self, cutpoints: impl IntoIterator<Item = usize>) -> Self::IntoIterator<'_>;

    /// Splits the labeling iterator into `n` approximately equal parts.
    ///
    /// This is a convenience method implemented in terms of
    /// [`split_iter_at`].
    ///
    /// [`split_iter_at`]: SplitLabeling::split_iter_at
    fn split_iter(&self, n: usize) -> Self::IntoIterator<'_> {
        self.split_iter_at(uniform_cutpoints(self.num_nodes(), n))
    }
}

/// Returns `n` + 1 cutpoints splitting `num_nodes` nodes into `n` parts of
/// approximately the same size.
///
/// These are the cutpoints used by [`SplitLabeling::split_iter`].
///
/// # Panics
///
/// Panics if `n` is zero.
pub fn uniform_cutpoints(num_nodes: usize, n: usize) -> impl Iterator<Item = usize> + Clone {
    let step = num_nodes.div_ceil(n);
    (0..=n).map(move |i| i.saturating_mul(step).min(num_nodes))
}

/// Returns cutpoints splitting the nodes of a graph into at most `n` parts
/// containing approximately the same number of arcs.
///
/// The cutpoints are computed by [`FairChunks`] from the degree cumulative
/// function (DCF) of the graph, which must contain one more element than the
/// number of nodes. The result contains at least two cutpoints, the first one
/// is zero, and the last one is the number of nodes.
///
/// # Panics
///
/// Panics if `n` is zero or if the DCF is empty.
pub fn dcf_cutpoints<D>(dcf: D, n: usize) -> Box<[usize]>
where
    D: for<'b> SuccUnchecked<Input = u64, Output<'b> = u64>
        + for<'b> IndexedSeq<Input = u64, Output<'b> = u64>,
{
    assert!(n > 0, "the number of parts must be positive");
    assert!(!dcf.is_empty(), "the DCF must not be empty");
    let num_nodes = dcf.len() - 1;
    let target = dcf.get(num_nodes).div_ceil(n as u64);
    let mut cutpoints: Vec<usize> = std::iter::once(0)
        .chain(FairChunks::new(target, dcf).map(|r| r.end))
        .collect();
    // FairChunks yields no range for a graph without arcs, and its last
    // range might end before trailing nodes without successors; the
    // cutpoints must nonetheless cover all nodes.
    if cutpoints.len() < 2 || *cutpoints.last().unwrap() != num_nodes {
        cutpoints.push(num_nodes);
    }
    cutpoints.into()
}

/// An iterator over the segments defined by a sequence of cutpoints.
///
/// The `i`-th segment is the range [`cutpoints[i]` . . `cutpoints[i + 1]`).
#[derive(Debug, Clone)]
pub(crate) struct Segments {
    cutpoints: Vec<usize>,
    i: usize,
}

impl Segments {
    /// Creates a new iterator over the segments defined by `cutpoints`.
    ///
    /// # Panics
    ///
    /// Panics if there are fewer than two cutpoints, if they are not
    /// non-decreasing, or if `num_nodes` is not `None` and the last cutpoint
    /// is greater than `num_nodes`.
    pub(crate) fn new(
        cutpoints: impl core::iter::IntoIterator<Item = usize>,
        num_nodes: Option<usize>,
    ) -> Self {
        let cutpoints: Vec<usize> = cutpoints.into_iter().collect();
        assert!(
            cutpoints.len() >= 2,
            "cutpoints must have at least 2 elements"
        );
        assert!(
            cutpoints.windows(2).all(|w| w[0] <= w[1]),
            "cutpoints must be non-decreasing"
        );
        if let Some(num_nodes) = num_nodes {
            assert!(
                *cutpoints.last().unwrap() <= num_nodes,
                "last cutpoint ({}) must be <= num_nodes ({})",
                cutpoints.last().unwrap(),
                num_nodes
            );
        }
        Self { cutpoints, i: 0 }
    }
}

impl Iterator for Segments {
    type Item = core::ops::Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i + 1 >= self.cutpoints.len() {
            return None;
        }
        let segment = self.cutpoints[self.i]..self.cutpoints[self.i + 1];
        self.i += 1;
        Some(segment)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }

    fn count(self) -> usize {
        self.len()
    }
}

impl ExactSizeIterator for Segments {
    fn len(&self) -> usize {
        self.cutpoints.len() - 1 - self.i
    }
}

impl core::iter::FusedIterator for Segments {}

/// Ready-made implementation for the sequential case.
///
/// This implementation walks through the iterator of a labeling and clones it
/// at the cutpoints, using [`advance_by`] to skip nodes between cutpoints. It
/// is designed for labelings whose [`iter_from`] is not more efficient than
/// sequential iteration (e.g., compressed graphs without an index); if
/// `iter_from` can seek efficiently, use [`ra::Iter`] instead.
///
/// To use it, you have to implement the trait by specifying the associated
/// types `SplitLender` and `IntoIterator` using the [`seq::Lender`] and
/// [`seq::IntoIterator`] type aliases, and then return a [`seq::Iter`]
/// structure.
///
/// # Examples
///
/// The code for [`BvGraphSeq`] is:
/// ```ignore
/// impl<F: SequentialDecoderFactory> SplitLabeling for BvGraphSeq<F>
/// where
///     for<'a> <F as SequentialDecoderFactory>::Decoder<'a>: Clone + Send + Sync,
/// {
///     type SplitLender<'a> = split::seq::Lender<'a, BvGraphSeq<F>> where Self: 'a;
///     type IntoIterator<'a> = split::seq::IntoIterator<'a, BvGraphSeq<F>> where Self: 'a;
///
///     fn split_iter_at(&self, cutpoints: impl IntoIterator<Item = usize>) -> Self::IntoIterator<'_> {
///         split::seq::Iter::new(self.iter(), cutpoints)
///     }
/// }
/// ```
///
/// [`advance_by`]: lender::Lender::advance_by
/// [`iter_from`]: SequentialLabeling::iter_from
/// [`BvGraphSeq`]: crate::graphs::bvgraph::sequential::BvGraphSeq
pub mod seq {
    use super::Segments;
    use crate::prelude::SequentialLabeling;

    /// An iterator over segments of a sequential labeling defined by cutpoints.
    pub struct Iter<L> {
        lender: L,
        /// The position of `lender`.
        pos: usize,
        segments: Segments,
    }

    impl<L: lender::Lender> Iter<L> {
        /// Creates a new iterator from a lender and a sequence of cutpoints.
        ///
        /// The cutpoints must be a non-decreasing sequence with at least 2
        /// elements, and the last cutpoint must be at most the number of nodes
        /// in the labeling.
        pub fn new(lender: L, cutpoints: impl core::iter::IntoIterator<Item = usize>) -> Self {
            Self {
                lender,
                pos: 0,
                segments: Segments::new(cutpoints, None),
            }
        }
    }

    impl<L: lender::Lender + Clone> Iterator for Iter<L> {
        type Item = lender::Take<L>;

        fn next(&mut self) -> Option<Self::Item> {
            let segment = self.segments.next()?;
            // The lender is positioned at the start of the previous segment,
            // or at node 0 before the first segment is returned; advance to
            // the start of the current segment.
            let advance = segment.start - self.pos;
            if advance > 0 {
                self.lender.advance_by(advance).ok()?;
            }
            self.pos = segment.start;
            Some(self.lender.clone().take(segment.len()))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.segments.size_hint()
        }

        fn count(self) -> usize {
            self.segments.count()
        }
    }

    impl<L: lender::Lender + Clone> ExactSizeIterator for Iter<L> {
        fn len(&self) -> usize {
            self.segments.len()
        }
    }
    impl<L: lender::Lender + Clone> core::iter::FusedIterator for Iter<L> {}

    pub type Lender<'a, S> = lender::Take<<S as SequentialLabeling>::Lender<'a>>;
    pub type IntoIterator<'a, S> = Iter<<S as SequentialLabeling>::Lender<'a>>;
}

/// Ready-made implementation for the random-access case.
///
/// This implementation calls [`iter_from`] at each cutpoint, seeking directly
/// to the desired position. It is designed for labelings with an efficient
/// `iter_from` (e.g., compressed graphs with an index such as an `.ef` file).
/// If `iter_from` is no faster than sequential iteration (e.g.,
/// [`ArcListGraph`]), use [`seq::Iter`] instead.
///
/// The bound is [`RandomAccessLabeling`] rather than [`SequentialLabeling`]
/// even though only `iter_from` is used: the stronger bound ensures that
/// `iter_from` is efficient, preventing silent quadratic slowdowns.
///
/// To use it, you have to implement the trait by specifying the associated
/// types `SplitLender` and `IntoIterator` using the [`ra::Lender`] and
/// [`ra::IntoIterator`] type aliases, and then return a [`ra::Iter`]
/// structure.
///
/// # Examples
///
/// The code for [`BvGraph`] is
/// ```ignore
/// impl<F: RandomAccessDecoderFactory> SplitLabeling for BvGraph<F>
/// where
///     for<'a> <F as RandomAccessDecoderFactory>::Decoder<'a>: Send + Sync,
/// {
///     type SplitLender<'a> = split::ra::Lender<'a, BvGraph<F>> where Self: 'a;
///     type IntoIterator<'a> = split::ra::IntoIterator<'a, BvGraph<F>> where Self: 'a;
///
///     fn split_iter_at(&self, cutpoints: impl IntoIterator<Item = usize>) -> Self::IntoIterator<'_> {
///         split::ra::Iter::new(self, cutpoints)
///     }
/// }
/// ```
///
/// [`iter_from`]: SequentialLabeling::iter_from
/// [`ArcListGraph`]: crate::graphs::arc_list_graph::ArcListGraph
/// [`RandomAccessLabeling`]: crate::traits::RandomAccessLabeling
/// [`BvGraph`]: crate::graphs::bvgraph::random_access::BvGraph
pub mod ra {
    use super::Segments;
    use crate::prelude::{RandomAccessLabeling, SequentialLabeling};

    /// An iterator over segments of a random-access labeling defined by
    /// cutpoints.
    pub struct Iter<'a, R: RandomAccessLabeling> {
        labeling: &'a R,
        segments: Segments,
    }

    impl<'a, R: RandomAccessLabeling> Iter<'a, R> {
        /// Creates a new iterator from a labeling and a sequence of cutpoints.
        ///
        /// The cutpoints must be a non-decreasing sequence with at least 2
        /// elements, and the last cutpoint must be at most [`num_nodes()`].
        ///
        /// [`num_nodes()`]: SequentialLabeling::num_nodes
        pub fn new(
            labeling: &'a R,
            cutpoints: impl core::iter::IntoIterator<Item = usize>,
        ) -> Self {
            Self {
                labeling,
                segments: Segments::new(cutpoints, Some(labeling.num_nodes())),
            }
        }
    }

    impl<'a, R: RandomAccessLabeling> Iterator for Iter<'a, R> {
        type Item = Lender<'a, R>;

        fn next(&mut self) -> Option<Self::Item> {
            use lender::Lender;

            let segment = self.segments.next()?;
            Some(self.labeling.iter_from(segment.start).take(segment.len()))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.segments.size_hint()
        }

        fn count(self) -> usize {
            self.segments.count()
        }
    }

    impl<R: RandomAccessLabeling> ExactSizeIterator for Iter<'_, R> {
        fn len(&self) -> usize {
            self.segments.len()
        }
    }
    impl<R: RandomAccessLabeling> core::iter::FusedIterator for Iter<'_, R> {}

    pub type Lender<'a, R> = lender::Take<<R as SequentialLabeling>::Lender<'a>>;
    pub type IntoIterator<'a, R> = Iter<'a, R>;
}
