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

use crate::prelude::{IteratorImpl, RandomAccessLabelling, SequentialLabelling};

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
