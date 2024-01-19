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
[`SequentialLabelling`] whose associated type `Label` is `usize`: labels are interpreted
as successors. Analogously, a [random-access graph](RandomAccessGraph) is simply a
[`RandomAccessLabelling`] extending a [`SequentialLabelling`] whose `Label` is `usize`.
To access the successors of a node, you should however use
[`RandomAccessLabelling::successors`], which delegates to [`labels`](RandomAccessLabelling::labels):
the latter method is overriden on purpose make its usage on graphs impossible.

In the same vein, a [sequential graph with labels](LabelledSequentialGraph) of type `L` is a
[`SequentialLabelling`] whose `Value` is `(usize, L)`
and a [random-access graph with labels](RandomAccessGraph) is a
[`RandomAccessLabelling`] extending a [`SequentialLabelling`] whose `Value` is `(usize, L)`.

Finally, the [zipping of a graph and a labelling](Zip) implements the
labelled graph traits.

Note that most utilities to manipulate graphs manipulate in fact
labelled graph. To use the same utilities on an unlabeled graph
you just have to wrap it in a [UnitLabelGraph], which
is a zero-cost abstraction assigning to each successor the label `()`.
Usually there is a convenience method doing the wrapping for you.

*/

use crate::prelude::{IteratorImpl, Pair, RandomAccessLabelling, SequentialLabelling};
use impl_tools::autoimpl;
use lender::*;

use super::labels::{LenderIntoIter, NodeLabelsLender, SortedIterator};

/// A graph that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or the successors of a node will be returned in ascending order.
/// The marker traits [SortedIterator] and [SortedSuccessors] can be used to
/// force these properties.
///
#[autoimpl(for<S: trait + ?Sized> &S, &mut S)]
pub trait SequentialGraph: SequentialLabelling<Label = usize> {}

pub type Successors<'succ, 'node, S> =
    <<S as SequentialLabelling>::Iterator<'node> as NodeLabelsLender<'succ>>::IntoIterator;

/// A [sequential graph](SequentialGraph) providing, additionally, random access to successor lists.
#[autoimpl(for<S: trait + ?Sized> &S, &mut S)]
pub trait RandomAccessGraph: RandomAccessLabelling<Label = usize> + SequentialGraph {
    /// Return the successors of a node.
    ///
    /// Note that this is just a convenience alias of the
    /// [`RandomAccessLabelling::labels`] method, which is overriden in this
    /// trait by an unimplemented, deprecated version to make its use impossible.
    /// This approach avoids that users might call `labels` expecting to get
    /// just the labels associated with a node.
    #[inline(always)]
    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Labels<'_> {
        <Self as RandomAccessLabelling>::labels(self, node_id)
    }

    /// Unconvenience override of the [`RandomAccessLabelling::labels`] method.
    ///
    /// This method contains [`std::unreachable`] to make it impossible
    /// its usage on graphs. Use the [`successors`] method instead.
    #[deprecated(note = "use the `successors` method instead; this method is just unreachable!()")]
    fn labels(&self, _node_id: usize) -> <Self as RandomAccessLabelling>::Labels<'_> {
        unreachable!("use the `successors` method instead");
    }

    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> bool {
        for neighbour_id in self.successors(src_node_id) {
            if neighbour_id == dst_node_id {
                return true;
            }
        }
        false
    }
}

/// A labelled sequential graph.
///
/// A labelled sequential graph is a sequential labelling whose labels are pairs `(usize, L)`.
/// The first coordinate is the successor, the second is the label.
pub trait LabelledSequentialGraph<L>: SequentialLabelling<Label = (usize, L)> {}

/// A trivial labelling associating to each successor the label `()`.
#[derive(Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct UnitLabelGraph<G: SequentialGraph>(pub G);

#[doc(hidden)]
#[repr(transparent)]
pub struct UnitIterator<L>(L);

impl<'succ, L> NodeLabelsLender<'succ> for UnitIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
{
    type Label = (usize, ());
    type IntoIterator = UnitSuccessors<LenderIntoIter<'succ, L>>;
}

impl<'succ, L> Lending<'succ> for UnitIterator<L>
where
    L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<L: Lender> Lender for UnitIterator<L>
where
    L: IntoLender + for<'next> NodeLabelsLender<'next, Label = usize>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.0.next().map(|x| {
            let t = x.into_pair();
            (t.0, UnitSuccessors(t.1.into_iter()))
        })
    }
}

#[repr(transparent)]
pub struct UnitSuccessors<I>(I);

impl<I: Iterator<Item = usize>> Iterator for UnitSuccessors<I> {
    type Item = (usize, ());

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.0.next()?, ()))
    }
}

impl<'a, G: SequentialGraph> SequentialLabelling for UnitLabelGraph<G> {
    type Label = (usize, ());

    type Iterator<'node> = UnitIterator<G::Iterator<'node>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        UnitIterator(self.0.iter_from(from))
    }
}

impl<'a, G: SequentialGraph> LabelledSequentialGraph<()> for UnitLabelGraph<G> {}

/// A labelled random-access graph.
///
/// A labelled random-access graph is a random-access labelling whose labels are
/// pairs `(usize, L)`. The first coordinate is the successor, the second is the
/// label.
pub trait LabelledRandomAccessGraph<L>: RandomAccessLabelling<Label = (usize, L)> {
    /// Return pairs given by successors of a node and their labels.
    ///
    /// Note that this is just a convenience alias of the
    /// [`RandomAccessLabelling::labels`] method, which is overriden in this
    /// trait by an unimplemented, deprecated version to make its use impossible.
    /// This approach avoids that users might call `labels` expecting to get
    /// just the labels associated with a node.
    #[inline(always)]
    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Labels<'_> {
        <Self as RandomAccessLabelling>::labels(self, node_id)
    }

    /// Unconvenience override of the [`RandomAccessLabelling::labels`] method.
    ///
    /// This method contains [`std::unreachable`] to make it impossible
    /// its usage on graphs. Use the [`successors`] method instead.
    #[deprecated(note = "use the `successors` method instead; this method is just unreachable!()")]
    fn labels(&self, _node_id: usize) -> <Self as RandomAccessLabelling>::Labels<'_> {
        unreachable!("use the `successors` method instead");
    }

    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> bool {
        for (neighbour_id, _) in self.successors(src_node_id) {
            if neighbour_id == dst_node_id {
                return true;
            }
        }
        false
    }
}

impl<'a, G: RandomAccessGraph> RandomAccessLabelling for UnitLabelGraph<G> {
    type Labels<'succ> =
        UnitSuccessors<<<G as RandomAccessLabelling>::Labels<'succ> as IntoIterator>::IntoIter>
        where Self: 'succ;

    fn num_arcs(&self) -> usize {
        self.0.num_arcs()
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Labels<'_> {
        UnitSuccessors(self.0.successors(node_id).into_iter())
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.0.outdegree(node_id)
    }
}

impl<'a, G: RandomAccessGraph> LabelledRandomAccessGraph<()> for UnitLabelGraph<G> {}
