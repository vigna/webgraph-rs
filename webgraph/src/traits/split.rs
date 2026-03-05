/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits and basic implementations to support parallel completion by splitting
//! the [iterator](SequentialLabeling::Lender) of a labeling into multiple
//! iterators.

use std::rc::Rc;

use impl_tools::autoimpl;

use super::{labels::SequentialLabeling, lenders::NodeLabelsLender};

/// A trait providing methods to split the labeling
/// [iterator](SequentialLabeling::Lender) into multiple thread-safe parts.
///
/// The main method is [`split_iter_at`](SplitLabeling::split_iter_at), which
/// takes a sequence of cutpoints and splits the iteration at those points.
/// Each cutpoint is a node id; the sequence must be non-decreasing and contain
/// at least two elements.
///
/// The convenience method [`split_iter`](SplitLabeling::split_iter) provides a
/// default implementation that splits the iteration into `n` approximately
/// equal parts. It is implemented in terms of
/// [`split_iter_at`](SplitLabeling::split_iter_at), so implementors only need
/// to provide the latter.
///
/// Note that the parts are required to be [`Send`] and [`Sync`], so that they
/// can be safely shared among threads.
///
/// Due to some limitations of the current Rust type system, we cannot provide
/// blanket implementations for this trait. However, we provide ready-made
/// implementations for the [sequential](seq) and [random-access](ra) cases. To
/// use them, you must implement the trait by specifying the associated types
/// `SplitLender` and `IntoIterator`, and then just return a [`seq::Iter`] or
/// [`ra::Iter`] structure from [`split_iter_at`](SplitLabeling::split_iter_at).
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
    /// [`split_iter_at`](SplitLabeling::split_iter_at).
    fn split_iter(&self, n: usize) -> Self::IntoIterator<'_> {
        let step = self.num_nodes().div_ceil(n);
        let num_nodes = self.num_nodes();
        self.split_iter_at((0..n + 1).map(move |i| (i * step).min(num_nodes)))
    }
}

/// Ready-made implementation for the sequential case.
///
/// This implementation walks through the iterator of a labeling and
/// clones it at the cutpoints. To use it, you have to implement the
/// trait by specifying the associated types `SplitLender` and `IntoIterator`
/// using the [`seq::Lender`] and [`seq::IntoIterator`] type aliases,
/// and then return a [`seq::Iter`] structure.
///
/// # Examples
///
/// The code for [`BvGraphSeq`](crate::graphs::bvgraph::sequential::BvGraphSeq) is:
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
pub mod seq {
    use crate::prelude::SequentialLabeling;

    /// An iterator over segments of a sequential labeling defined by cutpoints.
    pub struct Iter<L> {
        lender: L,
        cutpoints: Vec<usize>,
        i: usize,
    }

    impl<L: lender::Lender> Iter<L> {
        /// Creates a new iterator from a lender and a sequence of cutpoints.
        ///
        /// The cutpoints must be a non-decreasing sequence with at least 2
        /// elements.
        pub fn new(lender: L, cutpoints: impl core::iter::IntoIterator<Item = usize>) -> Self {
            let cutpoints: Vec<usize> = cutpoints.into_iter().collect();
            assert!(
                cutpoints.len() >= 2,
                "cutpoints must have at least 2 elements"
            );
            assert!(
                cutpoints.windows(2).all(|w| w[0] <= w[1]),
                "cutpoints must be non-decreasing"
            );
            Self {
                lender,
                cutpoints,
                i: 0,
            }
        }
    }

    impl<L: lender::Lender + Clone> Iterator for Iter<L> {
        type Item = lender::Take<L>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.i + 1 >= self.cutpoints.len() {
                return None;
            }
            if self.i > 0 {
                let advance = self.cutpoints[self.i] - self.cutpoints[self.i - 1];
                self.lender.advance_by(advance).ok()?;
            }
            let len = self.cutpoints[self.i + 1] - self.cutpoints[self.i];
            self.i += 1;
            Some(self.lender.clone().take(len))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let remaining = self.cutpoints.len() - 1 - self.i;
            (remaining, Some(remaining))
        }

        fn count(self) -> usize {
            self.cutpoints.len() - 1 - self.i
        }
    }

    impl<L: lender::Lender + Clone> core::iter::FusedIterator for Iter<L> {}

    pub type Lender<'a, S> = lender::Take<<S as SequentialLabeling>::Lender<'a>>;
    pub type IntoIterator<'a, S> = Iter<<S as SequentialLabeling>::Lender<'a>>;
}

/// Ready-made implementation for the random-access case.
///
/// This implementation uses [`iter_from`](SequentialLabeling::iter_from) at
/// each cutpoint. To use it, you have to implement the trait by specifying
/// the associated types `SplitLender` and `IntoIterator` using the
/// [`ra::Lender`] and [`ra::IntoIterator`] type aliases, and then return a
/// [`ra::Iter`] structure.
///
/// # Examples
///
/// The code for [`BvGraph`](crate::graphs::bvgraph::random_access::BvGraph) is
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
pub mod ra {
    use crate::prelude::{RandomAccessLabeling, SequentialLabeling};

    /// An iterator over segments of a random-access labeling defined by
    /// cutpoints.
    pub struct Iter<'a, R: RandomAccessLabeling> {
        labeling: &'a R,
        cutpoints: Vec<usize>,
        i: usize,
    }

    impl<'a, R: RandomAccessLabeling> Iter<'a, R> {
        /// Creates a new iterator from a labeling and a sequence of cutpoints.
        ///
        /// The cutpoints must be a non-decreasing sequence with at least 2
        /// elements.
        pub fn new(
            labeling: &'a R,
            cutpoints: impl core::iter::IntoIterator<Item = usize>,
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
            Self {
                labeling,
                cutpoints,
                i: 0,
            }
        }
    }

    impl<'a, R: RandomAccessLabeling> Iterator for Iter<'a, R> {
        type Item = Lender<'a, R>;

        fn next(&mut self) -> Option<Self::Item> {
            use lender::Lender;

            if self.i + 1 >= self.cutpoints.len() {
                return None;
            }
            let start = self.cutpoints[self.i];
            let end = self.cutpoints[self.i + 1];
            self.i += 1;
            Some(self.labeling.iter_from(start).take(end - start))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let remaining = self.cutpoints.len() - 1 - self.i;
            (remaining, Some(remaining))
        }

        fn count(self) -> usize {
            self.cutpoints.len() - 1 - self.i
        }
    }

    impl<R: RandomAccessLabeling> core::iter::FusedIterator for Iter<'_, R> {}

    pub type Lender<'a, R> = lender::Take<<R as SequentialLabeling>::Lender<'a>>;
    pub type IntoIterator<'a, R> = Iter<'a, R>;
}
