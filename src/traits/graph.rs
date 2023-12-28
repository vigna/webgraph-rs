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
[`SequentialLabelling`] whose `Value` is `usize`: labels are interpreted
as successors. Analogously, a [random-access graph](RandomAccessGraph) is simply a
[`RandomAccessLabelling`] extending a [`SequentialLabelling`] whose `Value` is `usize`.

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

use crate::{
    prelude::{IteratorImpl, RandomAccessLabelling, SequentialLabelling},
    Tuple2,
};
use lender::*;

use super::labelling::{Labels, LendingIntoIter, NodeLabelsLending};

/// A graph that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or the successors of a node will be returned in ascending order.
/// The marker traits [SortedIterator] and [SortedSuccessors] can be used to
/// force these properties.
///
pub trait SequentialGraph: SequentialLabelling<Label = usize> {}

pub type Successors<'succ, 'node, S> = Labels<'succ, 'node, S>;

/// Marker trait for [iterators](SequentialGraph::Iterator) of [sequential graphs](SequentialGraph)
/// that returns nodes in ascending order.
///
/// # Safety
/// The first element of the pairs returned by the iterator must go from
/// zero to the [number of nodes](SequentialGraph::num_nodes) of the graph, excluded.
pub unsafe trait SortedIterator: Lender {}

/// Marker trait for [sequential graphs](SequentialGraph) whose [successors](SequentialGraph::Successors)
/// are returned in ascending order.
///
/// # Safety
/// The successors returned by the iterator must be in ascending order.
pub unsafe trait SortedSuccessors: IntoIterator {}

/// A [sequential graph](SequentialGraph) providing, additionally, random access to successor lists.
pub trait RandomAccessGraph: RandomAccessLabelling<Label = usize> + SequentialGraph {
    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> bool {
        for neighbour_id in self.successors(src_node_id) {
            // found
            if neighbour_id == dst_node_id {
                return true;
            }
            // early stop
            if neighbour_id > dst_node_id {
                return false;
            }
        }
        false
    }
}

/// We iter on the node ids in a range so it is sorted
unsafe impl<'a, G: RandomAccessGraph> SortedIterator for IteratorImpl<'a, G> {}

pub trait LabelledSequentialGraph<L>: SequentialLabelling<Label = (usize, L)> {}

#[derive(Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct UnitLabelGraph<'a, G: SequentialGraph>(pub &'a G);

#[repr(transparent)]
pub struct UnitIterator<L>(L);

impl<'succ, L> NodeLabelsLending<'succ> for UnitIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next, Item = usize>,
{
    type Item = (usize, ());
    type IntoIterator = UnitSuccessors<LendingIntoIter<'succ, L>>;
}

impl<'succ, L> Lending<'succ> for UnitIterator<L>
where
    L: Lender + for<'next> NodeLabelsLending<'next, Item = usize>,
{
    type Lend = (usize, <Self as NodeLabelsLending<'succ>>::IntoIterator);
}

impl<L: Lender> Lender for UnitIterator<L>
where
    L: IntoLender + for<'next> NodeLabelsLending<'next, Item = usize>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.0.next().map(|x| {
            let t = x.into_tuple();
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

impl<'a, G: SequentialGraph> SequentialLabelling for UnitLabelGraph<'a, G> {
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

impl<'a, G: SequentialGraph> LabelledSequentialGraph<()> for UnitLabelGraph<'a, G> {}

pub trait LabelledRandomAccessGraph<L>: RandomAccessLabelling<Label = (usize, L)> {}

impl<'a, G: RandomAccessGraph> RandomAccessLabelling for UnitLabelGraph<'a, G> {
    type Successors<'succ> =
        UnitSuccessors<<<G as RandomAccessLabelling>::Successors<'succ> as IntoIterator>::IntoIter>
        where Self: 'succ;

    fn num_arcs(&self) -> usize {
        self.0.num_arcs()
    }

    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Successors<'_> {
        UnitSuccessors(self.0.successors(node_id).into_iter())
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.0.outdegree(node_id)
    }
}

impl<'a, G: RandomAccessGraph> LabelledRandomAccessGraph<()> for UnitLabelGraph<'a, G> {}
