/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Basic traits to access graphs, both sequentially and
in random-access fashion.

*/

use core::{
    ops::Range,
    sync::atomic::{AtomicUsize, Ordering},
};
use dsi_progress_logger::*;
use lender::*;
use std::sync::Mutex;

use crate::{
    prelude::{IteratorImpl, RandomAccessLabelling, SequentialLabelling},
    Tuple2,
};

/// A graph that can be accessed sequentially.
///
/// Note that there is no guarantee that the iterator will return nodes in
/// ascending order, or the successors of a node will be returned in ascending order.
/// The marker traits [SortedIterator] and [SortedSuccessors] can be used to
/// force these properties.
///
/// The iterator returned by [iter](SequentialGraph::iter) is [lending](Lender):
/// to access the next pair, you must have finished to use the previous one. You
/// can invoke [`Lender::into_iter`] to get a standard iterator, in general
/// at the cost of some allocation and copying.
pub trait SequentialGraph: SequentialLabelling<Value = usize> {}

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
pub trait RandomAccessGraph: RandomAccessLabelling<Value = usize> + SequentialGraph {
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

pub trait LabelledSequentialGraph<L>: SequentialLabelling<Value = (usize, L)> {}
pub trait LabelledRandomAccessGraph<L>: RandomAccessLabelling<Value = (usize, L)> {}

#[repr(transparent)]
pub struct UnitLabelledSequentialGraph<G: SequentialGraph>(G);

#[repr(transparent)]
pub struct UnitIterator<L>(L);

impl<'succ, L> Lending<'succ> for UnitIterator<L>
where
    L: Lender,
    for<'next> Lend<'next, L>: Tuple2<_0 = usize>,
    for<'next> <Lend<'next, L> as Tuple2>::_1: IntoIterator,
{
    type Lend = (
        usize,
        UnitSuccessors<<<Lend<'succ, L> as Tuple2>::_1 as IntoIterator>::IntoIter>,
    );
}

impl<'node, L: Lender> Lender for UnitIterator<L>
where
    L: Lender,
    for<'next> Lend<'next, L>: Tuple2<_0 = usize>,
    for<'next> <Lend<'next, L> as Tuple2>::_1: IntoIterator,
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

impl<G: SequentialGraph> SequentialLabelling for UnitLabelledSequentialGraph<G> {
    type Value = (usize, ());

    type Successors<'succ> = UnitSuccessors<<G::Successors<'succ> as IntoIterator>::IntoIter>;

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
