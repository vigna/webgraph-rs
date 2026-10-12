/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Left and right projections.
//!
//! The two structures in this module, [`Left`] and [`Right`], provide
//! projection of a labeling whose labels are pairs. In particular,
//! [`Left(Zip(g,h))`] is the same labeling as `g` and
//! [`Right(Zip(g,h))`] is the same labeling as `h`.
//!
//! [`Left(Zip(g,h))`]: crate::labels::zip::Zip
//! [`Right(Zip(g,h))`]: crate::labels::zip::Zip

use crate::graphs::{bvgraph::DCF, impl_into_lender};
use crate::impl_parallel_from_split;
use crate::prelude::{
    LenderIntoIterator, LenderLabel, NodeLabelsLender, Pair, RandomAccessGraph,
    RandomAccessLabeling, SequentialGraph, SequentialLabeling, SortedIterator, SortedLender,
};
use crate::traits::SplitLabeling;
use lender::{ExactSizeLender, FusedLender, Lend, Lender, Lending, unsafe_assume_covariance};

/// Generates a projection structure, together with its lender and iterators.
///
/// The arguments are the name of the projection structure (preceded by its
/// attributes), the names of its lender, of its [`IntoIterator`] on labels,
/// and of its iterator on labels, the associated type of [`Pair`] that is
/// projected, and the index of the projected component in the pair.
macro_rules! projection {
    (
        $(#[$attr:meta])*
        $Proj:ident,
        $Iter:ident,
        $IntoSucc:ident,
        $Succ:ident,
        $Component:ident,
        $index:tt
    ) => {
        $(#[$attr])*
        #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
        pub struct $Proj<S: SequentialLabeling>(pub S)
        where
            S::Label: Pair;

        #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
        #[doc(hidden)]
        pub struct $Iter<L>(pub L);

        impl<'succ, L> NodeLabelsLender<'succ> for $Iter<L>
        where
            L: Lender + for<'next> NodeLabelsLender<'next>,
            for<'next> LenderLabel<'next, L>: Pair,
        {
            type Label = <LenderLabel<'succ, L> as Pair>::$Component;
            type IntoIterator = $IntoSucc<<L as NodeLabelsLender<'succ>>::IntoIterator>;
        }

        impl<'succ, L> Lending<'succ> for $Iter<L>
        where
            L: Lender + for<'next> NodeLabelsLender<'next>,
            for<'next> LenderLabel<'next, L>: Pair,
        {
            type Lend = (usize, LenderIntoIterator<'succ, Self>);
        }

        impl<L: ExactSizeLender> ExactSizeLender for $Iter<L>
        where
            L: Lender + for<'next> NodeLabelsLender<'next>,
            for<'next> LenderLabel<'next, L>: Pair,
        {
            #[inline(always)]
            fn len(&self) -> usize {
                self.0.len()
            }

            #[inline(always)]
            fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl<L: FusedLender> FusedLender for $Iter<L>
        where
            L: Lender + for<'next> NodeLabelsLender<'next>,
            for<'next> LenderLabel<'next, L>: Pair,
        {
        }

        #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
        #[doc(hidden)]
        pub struct $IntoSucc<I: IntoIterator>(pub I)
        where
            I::Item: Pair;

        #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
        #[doc(hidden)]
        pub struct $Succ<I: Iterator>(pub I)
        where
            I::Item: Pair;

        impl<I: Iterator> Iterator for $Succ<I>
        where
            I::Item: Pair,
        {
            type Item = <I::Item as Pair>::$Component;

            #[inline(always)]
            fn next(&mut self) -> Option<Self::Item> {
                self.0.next().map(|x| x.into_pair().$index)
            }

            #[inline(always)]
            fn size_hint(&self) -> (usize, Option<usize>) {
                self.0.size_hint()
            }

            #[inline(always)]
            fn count(self) -> usize {
                self.0.count()
            }
        }

        impl<I: ExactSizeIterator> ExactSizeIterator for $Succ<I>
        where
            I::Item: Pair,
        {
            #[inline(always)]
            fn len(&self) -> usize {
                self.0.len()
            }
        }

        impl<I: Iterator + core::iter::FusedIterator> core::iter::FusedIterator for $Succ<I> where
            I::Item: Pair
        {
        }

        impl<I: DoubleEndedIterator> DoubleEndedIterator for $Succ<I>
        where
            I::Item: Pair,
        {
            #[inline(always)]
            fn next_back(&mut self) -> Option<Self::Item> {
                self.0.next_back().map(|x| x.into_pair().$index)
            }

            #[inline(always)]
            fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
                self.0.nth_back(n).map(|x| x.into_pair().$index)
            }
        }

        impl<I: IntoIterator> IntoIterator for $IntoSucc<I>
        where
            I::Item: Pair,
        {
            type Item = <I::Item as Pair>::$Component;
            type IntoIter = $Succ<I::IntoIter>;

            #[inline(always)]
            fn into_iter(self) -> Self::IntoIter {
                $Succ(self.0.into_iter())
            }
        }

        impl<L> Lender for $Iter<L>
        where
            L: Lender + for<'next> NodeLabelsLender<'next>,
            for<'next> LenderLabel<'next, L>: Pair,
        {
            // SAFETY: the lend is covariant as it projects a component from
            // the underlying covariant lender L.
            unsafe_assume_covariance!();

            #[inline(always)]
            fn next(&mut self) -> Option<Lend<'_, Self>> {
                self.0.next().map(|x| {
                    let (node, succ) = x.into_pair();
                    (node, $IntoSucc(succ))
                })
            }

            #[inline(always)]
            fn size_hint(&self) -> (usize, Option<usize>) {
                self.0.size_hint()
            }
        }

        impl_into_lender!([S: SequentialLabeling] $Proj<S> [S::Label: Pair]);

        impl<G> SplitLabeling for $Proj<G>
        where
            G: SequentialLabeling + SplitLabeling,
            G::Label: Pair,
        {
            type SplitLender<'a>
                = $Iter<G::SplitLender<'a>>
            where
                Self: 'a;
            type IntoIterator<'a>
                = core::iter::Map<
                <G::IntoIterator<'a> as IntoIterator>::IntoIter,
                fn(G::SplitLender<'a>) -> Self::SplitLender<'a>,
            >
            where
                Self: 'a;

            fn split_iter_at(
                &self,
                cutpoints: impl IntoIterator<Item = usize>,
            ) -> Self::IntoIterator<'_> {
                self.0.split_iter_at(cutpoints).into_iter().map($Iter)
            }
        }

        impl_parallel_from_split!(
            [G: SequentialLabeling + SplitLabeling]
            $Proj<G>
            [
                G::Label: Pair,
                for<'a> G::SplitLender<'a>: ExactSizeLender + FusedLender
            ]
        );

        impl<S: SequentialLabeling> SequentialLabeling for $Proj<S>
        where
            S::Label: Pair,
        {
            type Label = <S::Label as Pair>::$Component;

            type Lender<'node>
                = $Iter<S::Lender<'node>>
            where
                Self: 'node;

            #[inline(always)]
            fn num_nodes(&self) -> usize {
                self.0.num_nodes()
            }

            #[inline(always)]
            fn get_num_arcs(&self) -> Option<u64> {
                self.0.get_num_arcs()
            }

            #[inline(always)]
            fn iter_from(&self, from: usize) -> Self::Lender<'_> {
                $Iter(self.0.iter_from(from))
            }

            // A projection has the same outdegrees as the underlying labeling
            fn build_dcf(&self) -> DCF {
                self.0.build_dcf()
            }
        }

        impl<R: RandomAccessLabeling> RandomAccessLabeling for $Proj<R>
        where
            R::Label: Pair,
        {
            type Labels<'succ>
                = $IntoSucc<<R as RandomAccessLabeling>::Labels<'succ>>
            where
                Self: 'succ;

            #[inline(always)]
            fn num_arcs(&self) -> u64 {
                self.0.num_arcs()
            }

            #[inline(always)]
            fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
                $IntoSucc(self.0.labels(node_id))
            }

            #[inline(always)]
            fn outdegree(&self, node_id: usize) -> usize {
                self.0.outdegree(node_id)
            }
        }

        impl<S: SequentialLabeling> SequentialGraph for $Proj<S> where
            S::Label: Pair<$Component = usize>
        {
        }

        impl<R: RandomAccessLabeling> RandomAccessGraph for $Proj<R> where
            R::Label: Pair<$Component = usize>
        {
        }

        // SAFETY: projecting a component preserves lender sortedness.
        unsafe impl<L: SortedLender> SortedLender for $Iter<L>
        where
            L: Lender + for<'next> NodeLabelsLender<'next>,
            for<'next> LenderLabel<'next, L>: Pair,
        {
        }

        // SAFETY: projecting a component preserves iterator sortedness.
        unsafe impl<I: SortedIterator> SortedIterator for $Succ<I> where I::Item: Pair {}
    };
}

projection!(
    /// The projection onto the first component of a pair.
    ///
    /// See also [`Right`] and [`Zip`].
    ///
    /// [`Zip`]: crate::labels::Zip
    Left,
    LeftIterator,
    IntoLeftSucc,
    LeftSucc,
    Left,
    0
);

projection!(
    /// The projection onto the second component of a pair.
    ///
    /// See also [`Left`] and [`Zip`].
    ///
    /// [`Zip`]: crate::labels::Zip
    Right,
    RightIterator,
    IntoRightSucc,
    RightSucc,
    Right,
    1
);
