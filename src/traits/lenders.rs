/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::*;
// missing implementations for [Cloned, Copied, Owned] because they don't
// implement Lender but Iterator.

/// Iteration on nodes and associated labels.
///
/// This trait is a [`Lender`] returning pairs given by a `usize` (a node of the
/// graph) and an [`IntoIterator`], specified by the associated type `IntoIterator`,
/// over the labels associated with that node,
/// specified by the associated type `Label` (which is forced to be identical
/// to the associated type `Item` of the [`IntoIterator`]).
///
/// For those types we provide convenience type aliases [`LenderIntoIterator`],
/// [`LenderIntoIter`], and [`LenderLabel`].
///
/// ## Propagation of implicit bounds
///
/// The definition of this trait emerged from a [discussion on the Rust language
/// forum](https://users.rust-lang.org/t/more-help-for-more-complex-lifetime-situation/103821/10).
/// The purpose of the trait is to propagate the implicit
/// bound appearing in the definition [`Lender`] to the iterator returned
/// by the associated type [`IntoIterator`]. In this way, one can return iterators
/// depending on the internal state of the labeling. Without this additional trait, it
/// would be possible to return iterators whose state depends on the state of
/// the lender, but not on the state of the labeling.
pub trait NodeLabelsLender<'lend, __ImplBound: lender::ImplBound = lender::Ref<'lend, Self>>:
    Lender + Lending<'lend, __ImplBound, Lend = (usize, Self::IntoIterator)>
{
    type Label;
    type IntoIterator: IntoIterator<Item = Self::Label>;
}

/// Convenience type alias for the associated type `Label` of a [`NodeLabelsLender`].
pub type LenderLabel<'lend, L> = <L as NodeLabelsLender<'lend>>::Label;

/// Convenience type alias for the associated type `IntoIterator` of a [`NodeLabelsLender`].
pub type LenderIntoIterator<'lend, L> = <L as NodeLabelsLender<'lend>>::IntoIterator;

/// Convenience type alias for the [`Iterator`] returned by the `IntoIterator`
/// associated type of a [`NodeLabelsLender`].
pub type LenderIntoIter<'lend, L> =
    <<L as NodeLabelsLender<'lend>>::IntoIterator as IntoIterator>::IntoIter;

impl<'lend, A, B> NodeLabelsLender<'lend> for lender::Chain<A, B>
where
    A: Lender + for<'next> NodeLabelsLender<'next>,
    B: Lender
        + for<'next> NodeLabelsLender<
            'next,
            Label = <A as NodeLabelsLender<'next>>::Label,
            IntoIterator = <A as NodeLabelsLender<'next>>::IntoIterator,
        >,
{
    type Label = <A as NodeLabelsLender<'lend>>::Label;
    type IntoIterator = <A as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, 's, T> NodeLabelsLender<'lend> for lender::Chunk<'s, T>
where
    T: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = <T as NodeLabelsLender<'lend>>::Label;
    type IntoIterator = <T as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L> NodeLabelsLender<'lend> for lender::Cycle<L>
where
    L: Clone + Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, Label, II, L> NodeLabelsLender<'lend> for lender::Enumerate<L>
where
    L: Lender,
    L: for<'all> Lending<'all, Lend = II>,
    II: IntoIterator<Item = Label>,
{
    type Label = Label;
    type IntoIterator = II;
}

impl<'lend, L, P> NodeLabelsLender<'lend> for lender::Filter<L, P>
where
    P: for<'next> FnMut(&(usize, <L as NodeLabelsLender<'next>>::IntoIterator)) -> bool,
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L, F, II> NodeLabelsLender<'lend> for lender::FilterMap<L, F>
where
    II: IntoIterator + 'static, // TODO!: check if we can avoid this static
    F: for<'all> lender::higher_order::FnMutHKAOpt<'all, Lend<'all, L>, B = (usize, II)>,
    L: Lender,
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<'lend, 'this, LL, L, F> NodeLabelsLender<'lend> for lender::FlatMap<'this, LL, F>
where
    LL: Lender,
    L: Lender + for<'next> NodeLabelsLender<'next> + 'lend,
    F: for<'all> lender::higher_order::FnMutHKA<'all, Lend<'all, LL>, B = L>,
{
    type Label = <L as NodeLabelsLender<'lend>>::Label;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, 'this, LL, L> NodeLabelsLender<'lend> for lender::Flatten<'this, LL>
where
    LL: Lender<Lend = L>,
    L: Lender + for<'next> NodeLabelsLender<'next> + 'lend,
{
    type Label = <L as NodeLabelsLender<'lend>>::Label;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L> NodeLabelsLender<'lend> for lender::Fuse<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L, F> NodeLabelsLender<'lend> for lender::Inspect<L, F>
where
    F: for<'next> FnMut(&Lend<'next, L>),
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L, F, II> NodeLabelsLender<'lend> for lender::Map<L, F>
where
    F: for<'all> lender::higher_order::FnMutHKA<'all, Lend<'all, L>, B = (usize, II)>,
    II: IntoIterator,
    L: Lender,
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<'lend, L, P, II> NodeLabelsLender<'lend> for lender::MapWhile<L, P>
where
    P: for<'all> lender::higher_order::FnMutHKAOpt<'all, Lend<'all, L>, B = (usize, II)>,
    II: IntoIterator + 'static, // TODO!: check if we can avoid this static
    L: Lender,
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<'lend, L, F> NodeLabelsLender<'lend> for lender::Mutate<L, F>
where
    F: FnMut(&mut Lend<'_, L>),
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, 'this, L> NodeLabelsLender<'lend> for lender::Peekable<'this, L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L> NodeLabelsLender<'lend> for lender::Rev<L>
where
    L: Lender + DoubleEndedLender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L, St, F, II> NodeLabelsLender<'lend> for lender::Scan<L, St, F>
where
    for<'all> F:
        lender::higher_order::FnMutHKAOpt<'all, (&'all mut St, Lend<'all, L>), B = (usize, II)>,
    L: Lender,
    II: IntoIterator + 'static, // TODO!: check if we can avoid this static
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<'lend, L> NodeLabelsLender<'lend> for lender::Skip<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L, P> NodeLabelsLender<'lend> for lender::SkipWhile<L, P>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    P: for<'next> FnMut(&Lend<'next, L>) -> bool,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L> NodeLabelsLender<'lend> for lender::StepBy<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L> NodeLabelsLender<'lend> for lender::Take<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, L, P> NodeLabelsLender<'lend> for lender::TakeWhile<L, P>
where
    L: Lender + for<'next> NodeLabelsLender<'next>,
    P: FnMut(&Lend<'_, L>) -> bool,
{
    type Label = LenderLabel<'lend, L>;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, A, B, II> NodeLabelsLender<'lend> for lender::Zip<A, B>
where
    A: Lender + for<'next> Lending<'next, Lend = usize>,
    B: Lender + for<'next> Lending<'next, Lend = II>,
    II: IntoIterator,
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<'lend, I, J> NodeLabelsLender<'lend> for lender::FromIter<I>
where
    I: Iterator<Item = (usize, J)>,
    J: IntoIterator,
{
    type Label = J::Item;
    type IntoIterator = J;
}

impl<'lend, S, F, J> NodeLabelsLender<'lend> for lender::FromFn<S, F>
where
    F: for<'all> lender::higher_order::FnMutHKAOpt<'all, &'all mut S, B = (usize, J)>,
    J: IntoIterator,
{
    type Label = J::Item;
    type IntoIterator = J;
}
