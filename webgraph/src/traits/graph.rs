/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Basic traits to access graphs, both sequentially and
in random-access fashion.

A [sequential graph](SequentialGraph) is simply a
[`SequentialLabeling`] whose associated type [`Label`](SequentialLabeling::Label) is `usize`: labels are interpreted
as successors. Analogously, a [random-access graph](RandomAccessGraph) is simply a
[`RandomAccessLabeling`] extending a [`SequentialLabeling`] whose [`Label`](SequentialLabeling::Label) is `usize`.
To access the successors of a node, however, you must use
[`RandomAccessGraph::successors`], which delegates to [`labels`](RandomAccessLabeling::labels):
the latter method is overridden on purpose make its usage on graphs impossible.

In the same vein, a [sequential graph with labels](LabeledSequentialGraph) of type `L` is a
[`SequentialLabeling`] whose [`Label`](SequentialLabeling::Label) is `(usize, L)`
and a [random-access graph with labels](LabeledRandomAccessGraph) is a
[`RandomAccessLabeling`] extending a [`SequentialLabeling`] whose [`Label`](SequentialLabeling::Label) is `(usize, L)`.
Also in this case, access the successors of a node and their labels, you must use
[`LabeledRandomAccessGraph::successors`].

Finally, the [zipping of a graph and a labeling](Zip) implements the
labeled graph traits (sequential or random-access, depending on the labelings).

Note that most utilities to manipulate graphs manipulate in fact
labeled graphs. To use the same utilities on an unlabeled graph
you just have to wrap it in a [UnitLabelGraph], which
is a zero-cost abstraction assigning to each successor the label `()`.
Usually there is a convenience method doing the wrapping for you.

*/

use std::rc::Rc;

use crate::prelude::{Pair, RandomAccessLabeling, SequentialLabeling};
use impl_tools::autoimpl;
use lender::*;

use super::{
    lenders::{LenderIntoIter, NodeLabelsLender},
    SortedIterator, SortedLender,
};

#[allow(non_camel_case_types)]
struct this_method_cannot_be_called_use_successors_instead;

/// A graph that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or the successors of a node will be returned in ascending
/// order. The marker traits [`SortedLender`] and [`SortedIterator`] can be used
/// to force these properties.
///
/// The function [`eq`](eq) can be used to check whether two
/// graphs are equal.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
pub trait SequentialGraph: SequentialLabeling<Label = usize> {}

/// Returns true if the two provided graphs with sorted lenders are equal.
///
/// This associated function can be used to compare graphs with [sorted
/// lenders](crate::lenders::SortedLender), but whose iterators [are not
/// sorted](crate::lenders::SortedIterator). If the graphs are sorted,
/// [`SequentialLabeling::eq_sorted`] should be used instead.
pub fn eq<G0: SequentialGraph, G1: SequentialGraph>(g0: &G0, g1: &G1) -> bool
where
    for<'a> G0::Lender<'a>: SortedLender,
    for<'a> G1::Lender<'a>: SortedLender,
{
    // In theory we should be able to implement this function using eq_labeled,
    // but due to current limitations of the borrow checker, we would need to
    // make G0 and G1 'static.
    if g0.num_nodes() != g1.num_nodes() {
        return false;
    }
    for_!(((node0, succ0), (node1, succ1)) in g0.iter().zip(g1.iter()) {
        debug_assert_eq!(node0, node1);
        let mut succ0 = succ0.into_iter().collect::<Vec<_>>();
        let mut succ1 = succ1.into_iter().collect::<Vec<_>>();
        succ0.sort();
        succ1.sort();
        if succ0 != succ1 {
            return false;
        }
    });
    true
}

/// Convenience type alias for the iterator over the successors of a node
/// returned by the [`iter_from`](SequentialLabeling::iter_from) method.
pub type Successors<'succ, 'node, S> =
    <<S as SequentialLabeling>::Lender<'node> as NodeLabelsLender<'succ>>::IntoIterator;

/// A [sequential graph](SequentialGraph) providing, additionally, random access
/// to successor lists.
///
/// On such a graph, successors are returned by the
/// [`successors`](RandomAccessGraph::successors) method rather than by the
/// [`labels`](RandomAccessLabeling::labels) method.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
pub trait RandomAccessGraph: RandomAccessLabeling<Label = usize> + SequentialGraph {
    /// Returns the successors of a node.
    ///
    /// Note that this is just a convenience alias of the
    /// [`RandomAccessLabeling::labels`] method, which is overridden in this
    /// trait by an unimplemented, uncallable version.
    /// This approach avoids that users might call `labels` expecting to get
    /// just the labels associated with a node.
    #[inline(always)]
    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        <Self as RandomAccessLabeling>::labels(self, node_id)
    }

    /// Disabling override of the [`RandomAccessLabeling::labels`] method.
    ///
    /// The `where` clause of this override contains an unsatisfiable private trait bound,
    /// which makes calling this method impossible. Use the [`RandomAccessGraph::successors`] method instead.
    #[allow(private_bounds)]
    fn labels(&self, _node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_>
    where
        for<'a> this_method_cannot_be_called_use_successors_instead: Clone,
    {
        // This code is actually impossible to execute due to the unsatisfiable
        // private trait bound.
        unimplemented!("use the `successors` method instead");
    }

    /// Returns whether there is an arc going from `src_node_id` to `dst_node_id`.
    ///
    /// Note that the default implementation performs a linear scan.
    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> bool {
        for succ in self.successors(src_node_id) {
            if succ == dst_node_id {
                return true;
            }
        }
        false
    }
}

/// A labeled sequential graph.
///
/// A labeled sequential graph is a sequential labeling whose labels are pairs
/// `(usize, L)`. The first coordinate is the successor, the second is the
/// label.
///
/// The function [`eq_labeled`](eq_labeled) can be used to check whether two
/// labeled graphs are equal.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
pub trait LabeledSequentialGraph<L>: SequentialLabeling<Label = (usize, L)> {}

/// Returns true if the two provided labeled graphs with sorted lenders are
/// equal.
///
/// This associated function can be used to compare graphs with [sorted
/// lenders](crate::lenders::SortedLender), but whose iterators [are not
/// sorted](crate::lenders::SortedIterator). If the graphs are sorted,
/// [`SequentialLabeling::eq_sorted`] should be used instead.
pub fn eq_labeled<M, G0: LabeledSequentialGraph<M>, G1: LabeledSequentialGraph<M>>(
    g0: &G0,
    g1: &G1,
) -> bool
where
    for<'a> G0::Lender<'a>: SortedLender,
    for<'a> G1::Lender<'a>: SortedLender,
    M: PartialEq,
{
    if g0.num_nodes() != g1.num_nodes() {
        return false;
    }
    for_!(((node0, succ0), (node1, succ1)) in g0.iter().zip(g1.iter()) {
        debug_assert_eq!(node0, node1);
        let mut succ0 = succ0.into_iter().collect::<Vec<_>>();
        let mut succ1 = succ1.into_iter().collect::<Vec<_>>();
        succ0.sort_by_key(|x| x.0);
        succ1.sort_by_key(|x| x.0);
        if succ0 != succ1 {
            return false;
        }
    });
    true
}

/// A wrapper associating to each successor the label `()`.
///
/// This wrapper can be used whenever a method requires a labeled graph, but the
/// graph is actually unlabeled. It is (usually) a zero-cost abstraction.
///
/// If the method returns some graphs derived from the input, it will usually be
/// necessary to [project the labels away](crate::labels::Left).
#[derive(Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct UnitLabelGraph<G: SequentialGraph>(pub G);

#[doc(hidden)]
#[repr(transparent)]
pub struct UnitLender<L>(pub L);

impl<'succ, L> NodeLabelsLender<'succ> for UnitLender<L>
where
    L: for<'next> NodeLabelsLender<'next, Label = usize>,
{
    type Label = (usize, ());
    type IntoIterator = UnitSuccessors<LenderIntoIter<'succ, L>>;
}

impl<'succ, L> Lending<'succ> for UnitLender<L>
where
    L: for<'next> NodeLabelsLender<'next, Label = usize>,
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<L> Lender for UnitLender<L>
where
    L: for<'next> NodeLabelsLender<'next, Label = usize>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.0.next().map(|x| {
            let t = x.into_pair();
            (t.0, UnitSuccessors(t.1.into_iter()))
        })
    }
}

unsafe impl<L: SortedLender> SortedLender for UnitLender<L> where
    L: for<'next> NodeLabelsLender<'next, Label = usize>
{
}

#[doc(hidden)]
#[repr(transparent)]
pub struct UnitSuccessors<I>(pub I);

impl<I: Iterator<Item = usize>> Iterator for UnitSuccessors<I> {
    type Item = (usize, ());

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.0.next()?, ()))
    }
}

unsafe impl<I: Iterator<Item = usize> + SortedIterator> SortedIterator for UnitSuccessors<I> {}

impl<G: SequentialGraph> SequentialLabeling for UnitLabelGraph<G> {
    type Label = (usize, ());

    type Lender<'node>
        = UnitLender<G::Lender<'node>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        UnitLender(self.0.iter_from(from))
    }
}

impl<G: SequentialGraph> LabeledSequentialGraph<()> for UnitLabelGraph<G> {}

/// A labeled random-access graph.
///
/// A labeled random-access graph is a random-access labeling whose labels are
/// pairs `(usize, L)`. The first coordinate is the successor, the second is the
/// label.
///
/// On such a graph, successors are returned by the
/// [`successors`](LabeledRandomAccessGraph::successors) method rather than by
/// the [`labels`](RandomAccessLabeling::labels) method.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S, Rc<S>)]
pub trait LabeledRandomAccessGraph<L>: RandomAccessLabeling<Label = (usize, L)> {
    /// Returns pairs given by successors of a node and their labels.
    ///
    /// Note that this is just a convenience alias of the
    /// [`RandomAccessLabeling::labels`] method, which is overridden in this
    /// trait by an unimplemented, deprecated version to make its use impossible.
    /// This approach avoids that users might call `labels` expecting to get
    /// just the labels associated with a node.
    #[inline(always)]
    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        <Self as RandomAccessLabeling>::labels(self, node_id)
    }

    /// Disabling override of the [`RandomAccessLabeling::labels`] method.
    ///
    /// The `where` clause of this override contains an unsatisfiable private
    /// trait bound, which makes calling this method impossible. Use the
    /// [`LabeledRandomAccessGraph::successors`] method instead.
    #[allow(private_bounds)]
    fn labels(&self, _node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_>
    where
        for<'a> this_method_cannot_be_called_use_successors_instead: Clone,
    {
        // This code is actually impossible to execute due to the unsatisfiable
        // private trait bound.
        unimplemented!("use the `successors` method instead");
    }

    /// Returns whether there is an arc going from `src_node_id` to `dst_node_id`.
    ///
    /// Note that the default implementation performs a linear scan.
    fn has_arc(&self, src: usize, dst: usize) -> bool {
        for (succ, _) in self.successors(src) {
            if succ == dst {
                return true;
            }
        }
        false
    }
}

impl<G: RandomAccessGraph> RandomAccessLabeling for UnitLabelGraph<G> {
    type Labels<'succ>
        = UnitSuccessors<<<G as RandomAccessLabeling>::Labels<'succ> as IntoIterator>::IntoIter>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        UnitSuccessors(self.0.successors(node_id).into_iter())
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.0.outdegree(node_id)
    }
}

impl<G: RandomAccessGraph> LabeledRandomAccessGraph<()> for UnitLabelGraph<G> {}
