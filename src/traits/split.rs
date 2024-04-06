/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits and basic implementations to support parallel completion by splitting
//! the [iterator](SequentialLabeling::Iterator) of a labeling into multiple
//! iterators.

use super::{labels::SequentialLabeling, lenders::NodeLabelsLender};

/// A trait with a single method that splits a labeling into `n` parts which are
/// thread safe.
///
/// Labeling implementing this trait can be analyzed in parallel by calling
/// [`split_iter`](SplitLabeling::split_iter) to split the labeling
/// [iterator](SequentialLabeling::Iterator) into `n` parts.
///
/// Note that the parts are required to be [`Send`] and [`Sync`], so that they
/// can be safely shared among threads.
///
/// Due to some limitations of the current Rust type system, we cannot provide
/// blanket implementations for this trait. However, we provide ready-made
/// implementations for the [sequential](seq) and [random-access](ra) cases. To
/// use them, you must implement the trait by specifying the associated types
/// `Lender` and `IntoIterator`, and then just return a [`seq::Iter`] or
/// [`ra::Iter`] structure.

pub trait SplitLabeling: SequentialLabeling {
    type SplitLender<'a>: for<'next> NodeLabelsLender<'next, Label = <Self as SequentialLabeling>::Label>
        + Send
        + Sync
    where
        Self: 'a;
    type IntoIterator<'a>: IntoIterator<Item = Self::SplitLender<'a>>
    where
        Self: 'a;
    fn split_iter(&self, n: usize) -> Self::IntoIterator<'_>;
}

/// Ready-made implementation for the sequential case.
///
/// This implementation walks through the iterator of a labeling and
/// clones it at regular intervals. To use it, you have to implement the
/// trait by specifying the associated types `Lender` and `IntoIterator`
/// using the [`seq::Lender`] and [`seq::IntoIterator`] types aliases,
/// and then return a [`seq::Iter`] structure.
///
/// # Examples
///
/// The code for [`BVGraphSeq`](crate::graph::bvgraph::BVGraphSeq) is:
/// ```ìgnore
/// impl<F: SequentialDecoderFactory> SplitLabeling for BVGraphSeq<F>
/// where
///     for<'a> <F as SequentialDecoderFactory>::Decoder<'a>: Clone + Send + Sync,
/// {
///     type Lender<'a> = split::seq::Lender<'a, BVGraphSeq<F>> where Self: 'a;
///     type IntoIterator<'a> = split::seq::IntoIterator<'a, BVGraphSeq<F>> where Self: 'a;
///
///     fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
///         split::seq::Iter::new(self.iter(), how_many)
///     }
/// }
/// ```

pub mod seq {
    use mem_dbg::{MemDbg, MemSize};
    use epserde::Epserde;
    use crate::prelude::SequentialLabeling;

    #[derive(Clone, Debug, MemDbg, MemSize, Epserde)]
    pub struct Iter<L> {
        lender: L,
        nodes_per_iter: usize,
        how_many: usize,
        remaining: usize,
    }

    impl<L: lender::ExactSizeLender> Iter<L> {
        pub fn new(lender: L, how_many: usize) -> Self {
            let nodes_per_iter = lender.len().div_ceil(how_many);
            Self {
                lender,
                nodes_per_iter,
                how_many,
                remaining: how_many,
            }
        }
    }

    impl<L: lender::Lender + Clone> Iterator for Iter<L> {
        type Item = lender::Take<L>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.remaining == 0 {
                return None;
            }
            if self.remaining != self.how_many {
                self.lender.advance_by(self.nodes_per_iter).ok()?;
            }
            self.remaining -= 1;
            Some(self.lender.clone().take(self.nodes_per_iter))
        }
    }

    impl<L: lender::Lender + Clone> ExactSizeIterator for Iter<L> {
        fn len(&self) -> usize {
            self.remaining
        }
    }

    pub type Lender<'a, S> = lender::Take<<S as SequentialLabeling>::Lender<'a>>;
    pub type IntoIterator<'a, S> = Iter<<S as SequentialLabeling>::Lender<'a>>;
}

/// Ready-made implementation for the random-access case.
///
/// This implementation uses the [`iter_from`](SequentialLabeling::iter_from) at
/// regular intervals. To use it, you have to implement the trait by specifying
/// the associated types `Lender` and `IntoIterator` using the [`ra::Lender`]
/// and [`ra::IntoIterator`] types aliases, and then return a [`ra::Iter`]
/// structure.
///
/// # Examples
///
/// The code for [`BVGraph`](crate::graph::bvgraph::BVGraph) is
/// ```ìgnore
/// impl<F: RandomAccessDecoderFactory> SplitLabeling for BVGraph<F>
/// where
///     for<'a> <F as RandomAccessDecoderFactory>::Decoder<'a>: Send + Sync,
/// {
///     type Lender<'a> = split::ra::Lender<'a, BVGraph<F>> where Self: 'a;
///     type IntoIterator<'a> = split::ra::IntoIterator<'a, BVGraph<F>> where Self: 'a;
///
///     fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
///         split::ra::Iter::new(self, how_many)
///     }
/// }
/// ```

pub mod ra {
    use mem_dbg::{MemDbg, MemSize};
    use epserde::Epserde;
    use crate::prelude::{RandomAccessLabeling, SequentialLabeling};

    #[derive(Clone, Debug, MemDbg, MemSize, Epserde)]
    pub struct Iter<'a, R: RandomAccessLabeling> {
        labeling: &'a R,
        nodes_per_iter: usize,
        how_many: usize,
        i: usize,
    }

    impl<'a, R: RandomAccessLabeling> Iter<'a, R> {
        pub fn new(labeling: &'a R, how_many: usize) -> Self {
            let nodes_per_iter = labeling.num_nodes().div_ceil(how_many);
            Self {
                labeling,
                nodes_per_iter,
                how_many,
                i: 0,
            }
        }
    }

    impl<'a, R: RandomAccessLabeling> Iterator for Iter<'a, R> {
        type Item = Lender<'a, R>;

        fn next(&mut self) -> Option<Self::Item> {
            use lender::Lender;

            if self.i == self.how_many {
                return None;
            }
            self.i += 1;
            Some(
                self.labeling
                    .iter_from((self.i - 1) * self.nodes_per_iter)
                    .take(self.nodes_per_iter),
            )
        }
    }

    impl<'a, R: RandomAccessLabeling> ExactSizeIterator for Iter<'a, R> {
        fn len(&self) -> usize {
            self.how_many - self.i
        }
    }

    pub type Lender<'a, R> = lender::Take<<R as SequentialLabeling>::Lender<'a>>;
    pub type IntoIterator<'a, R> = Iter<'a, R>;
}
