/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! The [main iteration trait](NodeLabelsLender), convenience types and
//! associated implementations.
//!
//! The implementations in this module have the effect that most of the methods
//! of a [`Lender`] (e.g., [`lender::Map`]) will return a [`NodeLabelsLender`]
//! when applied to a [`NodeLabelsLender`]. Without the implementations, one
//! would obtain a normal [`Lender`], which would not be usable as an argument,
//! say, of [`BvComp::extend`](crate::graphs::bvgraph::BvComp::extend).

use lender::{DoubleEndedLender, Lend, Lender, Lending};

use crate::traits::Pair;

/// Iteration on nodes and associated labels.
///
/// This trait is a [`Lender`] returning pairs given by a `usize` (a node of the
/// graph) and an [`IntoIterator`], specified by the associated type
/// `IntoIterator`, over the labels associated with that node, specified by the
/// associated type `Label` (which is forced to be identical to the associated
/// type `Item` of the [`IntoIterator`]).
///
/// For those types we provide convenience type aliases [`LenderIntoIterator`],
/// [`LenderIntoIter`], and [`LenderLabel`].
///
/// # Flattening Facilities
///
/// The methods [`into_pairs`](NodeLabelsLender::into_pairs) and
/// [`into_labeled_pairs`](NodeLabelsLender::into_labeled_pairs) convert a
/// [`NodeLabelsLender`] into an iterator of pairs or triples, respectively.
///
/// # Extension of [`Lender`] Methods
///
/// Methods defined on [`Lender`], such as [`Lender::zip`], normally would
/// return a [`Lender`], but not a [`NodeLabelsLender`]. However, the module
/// [`lenders`](super::lenders) contains implementations that automatically turn
/// such as a [`Lender`] into a [`NodeLabelsLender`] whenever it makes sense.
///
/// Thus, for example, one can take two graphs and merge easily the first half
/// of the first one and the second half of the second one:
/// ```rust
/// use webgraph::prelude::*;
/// use webgraph::graphs::random::ErdosRenyi;
/// use lender::*;
/// use itertools::Itertools;
///
/// // First random graph
/// let g = ErdosRenyi::new(100, 0.1, 0);
/// // Second random graph
/// let h = ErdosRenyi::new(100, 0.1, 1);
/// let mut v = VecGraph::new();
/// // Put first half of the first random graph in v
/// v.add_lender(g.iter().take(50));
/// // Put second half of the second random graph in v
/// v.add_lender(h.iter().skip(50));
///
/// let mut iter = v.iter();
/// for i in 0..50 {
///     itertools::assert_equal(v.successors(i), iter.next().unwrap().1);
/// }
/// let mut iter = h.iter().skip(50);
/// for i in 50..100 {
///     itertools::assert_equal(v.successors(i), iter.next().unwrap().1);
/// }
/// ```
/// [`VecGraph::add_lender`](crate::graphs::vec_graph::VecGraph::add_lender)
/// takes a [`NodeLabelsLender`] as an argument, but the implementations in the
/// module [`lenders`](super::lenders) makes the result of [`Lender::take`] and
/// [`Lender::skip`] a [`NodeLabelsLender`].
///
/// # Propagation of implicit bounds
///
/// The definition of this trait emerged from a [discussion on the Rust language
/// forum](https://users.rust-lang.org/t/more-help-for-more-complex-lifetime-situation/103821/10).
/// The purpose of the trait is to propagate the implicit bound appearing in the
/// definition [`Lender`] to the iterator returned by the associated type
/// [`IntoIterator`]. In this way, one can return iterators depending on the
/// internal state of the labeling. Without this additional trait, it would be
/// possible to return iterators whose state depends on the state of the lender,
/// but not on the state of the labeling.
///
/// [`ArcListGraph`](crate::graphs::arc_list_graph::ArcListGraph) is the main
/// motivation for this trait.
pub trait NodeLabelsLender<'lend, __ImplBound: lender::ImplBound = lender::Ref<'lend, Self>>:
    Lender + Lending<'lend, __ImplBound, Lend = (usize, Self::IntoIterator)>
{
    type Label;
    type IntoIterator: IntoIterator<Item = Self::Label>;

    /// Converts this lender into an iterator of triples, provided
    /// that the label type implements [`Pair`] with `Left = usize`.
    ///
    /// Typically, this method is used to convert a lender on a labeled graph
    /// into an iterator of labeled arcs expressed as triples.
    fn into_labeled_pairs<R>(self) -> IntoLabeledPairs<Self, R>
    where
        Self: Sized + for<'a> NodeLabelsLender<'a, Label: Pair<Left = usize, Right = R>>,
        R: Copy,
    {
        IntoLabeledPairs::new(self)
    }

    /// Converts this lender into an iterator of pairs, provided that the label
    /// type is `usize`.
    ///
    /// Typically, this method is used to convert a lender on a graph into an
    /// iterator of arcs expressed as pairs.
    fn into_pairs(self) -> IntoPairs<Self>
    where
        Self: Sized + for<'a> NodeLabelsLender<'a, Label = usize>,
    {
        IntoPairs::new(self)
    }
}

/// An [`Iterator`] adapter that converts a [`NodeLabelsLender`] into an
/// iterator of triples.
///
/// This struct is created by the
/// [`into_labeled_pairs`](NodeLabelsLender::into_labeled_pairs) method. It
/// converts a lender that yields `(usize, IntoIterator)` pairs into a flat
/// iterator of `(src, dst, label)` triples, where each `(dst, label)`
/// comes from the inner iterator.
pub struct IntoLabeledPairs<L, R> {
    lender: L,
    current_node: usize,
    current_iter: std::vec::IntoIter<(usize, R)>,
}

impl<L, R> IntoLabeledPairs<L, R>
where
    L: for<'a> NodeLabelsLender<'a, Label: Pair<Left = usize, Right = R>>,
    R: Copy,
{
    fn new(lender: L) -> Self {
        Self {
            lender,
            current_node: 0,
            current_iter: Vec::new().into_iter(),
        }
    }
}

impl<L, R> Iterator for IntoLabeledPairs<L, R>
where
    L: for<'a> NodeLabelsLender<'a, Label: Pair<Left = usize, Right = R>>,
    R: Copy,
{
    type Item = (usize, usize, R);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((dst, label)) = self.current_iter.next() {
                return Some((self.current_node, dst, label));
            }

            if let Some((next_node, next_succ)) = self.lender.next() {
                self.current_node = next_node;
                self.current_iter = next_succ
                    .into_iter()
                    .map(|l| l.into_pair())
                    .collect::<Vec<_>>()
                    .into_iter();
            } else {
                return None;
            }
        }
    }
}

/// An [`Iterator`] adapter that converts a [`NodeLabelsLender`] into an
/// iterator of pairs.
///
/// This struct is created by the [`into_pairs`](NodeLabelsLender::into_pairs)
/// method. It converts a lender that yields `(usize, IntoIterator)` pairs into
/// a flat iterator of `(src, dst)` pairs, where each `dst` comes from the
/// inner iterator.
pub struct IntoPairs<L> {
    lender: L,
    current_node: usize,
    current_iter: std::vec::IntoIter<usize>,
}

impl<L> IntoPairs<L>
where
    L: for<'a> NodeLabelsLender<'a, Label = usize>,
{
    fn new(lender: L) -> Self {
        Self {
            lender,
            current_node: 0,
            current_iter: Vec::new().into_iter(),
        }
    }
}

impl<L> Iterator for IntoPairs<L>
where
    L: for<'a> NodeLabelsLender<'a, Label = usize>,
{
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(dst) = self.current_iter.next() {
                return Some((self.current_node, dst));
            }

            if let Some((next_node, next_succ)) = self.lender.next() {
                self.current_node = next_node;
                self.current_iter = next_succ.into_iter().collect::<Vec<_>>().into_iter();
            } else {
                return None;
            }
        }
    }
}

/// Convenience type alias for the associated type `Label` of a [`NodeLabelsLender`].
pub type LenderLabel<'lend, L> = <L as NodeLabelsLender<'lend>>::Label;

/// Convenience type alias for the associated type `IntoIterator` of a [`NodeLabelsLender`].
pub type LenderIntoIterator<'lend, L> = <L as NodeLabelsLender<'lend>>::IntoIterator;

/// Convenience type alias for the [`Iterator`] returned by the `IntoIterator`
/// associated type of a [`NodeLabelsLender`].
pub type LenderIntoIter<'lend, L> =
    <<L as NodeLabelsLender<'lend>>::IntoIterator as IntoIterator>::IntoIter;

// Missing implementations for [Cloned, Copied, Owned] because they don't
// implement Lender but Iterator

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

impl<'lend, T> NodeLabelsLender<'lend> for lender::Chunk<'_, T>
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

impl<Label, II, L> NodeLabelsLender<'_> for lender::Enumerate<L>
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

impl<L, F, II> NodeLabelsLender<'_> for lender::FilterMap<L, F>
where
    II: IntoIterator + 'static, // TODO!: check if we can avoid this static
    F: for<'all> lender::higher_order::FnMutHKAOpt<'all, Lend<'all, L>, B = (usize, II)>,
    L: Lender,
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<'lend, LL, L, F> NodeLabelsLender<'lend> for lender::FlatMap<'_, LL, F>
where
    LL: Lender,
    L: Lender + for<'next> NodeLabelsLender<'next> + 'lend,
    F: for<'all> lender::higher_order::FnMutHKA<'all, Lend<'all, LL>, B = L>,
{
    type Label = <L as NodeLabelsLender<'lend>>::Label;
    type IntoIterator = <L as NodeLabelsLender<'lend>>::IntoIterator;
}

impl<'lend, LL, L> NodeLabelsLender<'lend> for lender::Flatten<'_, LL>
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

impl<L, F, II> NodeLabelsLender<'_> for lender::Map<L, F>
where
    F: for<'all> lender::higher_order::FnMutHKA<'all, Lend<'all, L>, B = (usize, II)>,
    II: IntoIterator,
    L: Lender,
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<L, P, II> NodeLabelsLender<'_> for lender::MapWhile<L, P>
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

impl<'lend, L> NodeLabelsLender<'lend> for lender::Peekable<'_, L>
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

impl<L, St, F, II> NodeLabelsLender<'_> for lender::Scan<L, St, F>
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

impl<A, B, II> NodeLabelsLender<'_> for lender::Zip<A, B>
where
    A: Lender + for<'next> Lending<'next, Lend = usize>,
    B: Lender + for<'next> Lending<'next, Lend = II>,
    II: IntoIterator,
{
    type Label = II::Item;
    type IntoIterator = II;
}

impl<I, J> NodeLabelsLender<'_> for lender::FromIter<I>
where
    I: Iterator<Item = (usize, J)>,
    J: IntoIterator,
{
    type Label = J::Item;
    type IntoIterator = J;
}

impl<S, F, J> NodeLabelsLender<'_> for lender::FromFn<S, F>
where
    F: for<'all> lender::higher_order::FnMutHKAOpt<'all, &'all mut S, B = (usize, J)>,
    J: IntoIterator,
{
    type Label = J::Item;
    type IntoIterator = J;
}