/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::visits::{
    breadth_first::{EventPred, FilterArgsPred},
    Sequential,
};
use nonmax::NonMaxUsize;
use std::{collections::VecDeque, ops::ControlFlow, ops::ControlFlow::Continue};
use sux::bits::BitVec;
use webgraph::traits::RandomAccessGraph;

/// A sequential breadth-first visit.
///
/// This implementation uses an algorithm that is slightly different from the
/// classical textbook algorithm, as we do not store parents or distances of the
/// nodes from the root: Parents and distances are computed on the fly and
/// passed to the callback function by visiting nodes when they are discovered,
/// rather than when they are extracted from the queue.
///
/// This approach requires inserting a level separator between nodes at
/// different distances: to obtain this result in a compact way, nodes are
/// represented using [`NonMaxUsize`], so the `None` variant of
/// `Option<NonMaxUsize>` can be used as a separator.
///
/// # Examples
///
/// Let's compute the distances from 0:
///
/// ```
/// use webgraph_algo::visits::*;
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph::labels::proj::Left;
/// use std::ops::ControlFlow::Continue;
/// use no_break::NoBreak;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
/// let mut visit = breadth_first::Seq::new(&graph);
/// let mut d = [0; 4];
/// visit.visit(
///     [0],
///     |event| {
///          // Set distance from 0
///          if let breadth_first::EventPred::Unknown { node, distance, .. } = event {
///              d[node] = distance;
///          }
///          Continue(())
///     },
/// ).continue_value_no_break();
///
/// assert_eq!(d, [0, 1, 2, 2]);
/// ```
///
/// Here instead we compute the size of the ball of radius two around node 0: to
/// minimize resource usage, we count nodes in the filter function, rather than
/// as the result of an event. In this way, node at distance two are counted but
/// not included in the queue, as it would happen if we were counting during an
/// [`EventPred::Unknown`] event.
///
/// ```
/// use std::convert::Infallible;
/// use webgraph_algo::visits::*;
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph::labels::proj::Left;
/// use std::ops::ControlFlow::Continue;
/// use no_break::NoBreak;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
/// let mut visit = breadth_first::Seq::new(&graph);
/// let mut count = 0;
/// visit.visit_filtered(
///     [0],
///     |event| { Continue(()) },
///     |breadth_first::FilterArgsPred { distance, .. }| {
///         if distance > 2 {
///             false
///         } else {
///             count += 1;
///             true
///         }
///     },
/// ).continue_value_no_break();
/// assert_eq!(count, 3);
/// ```
pub struct Seq<G: RandomAccessGraph> {
    graph: G,
    visited: BitVec,
    /// The visit queue; to avoid storing distances, we use `None` as a
    /// separator between levels. [`NonMaxUsize`] is used to avoid
    /// storage for the option variant tag.
    queue: VecDeque<Option<NonMaxUsize>>,
}

impl<G: RandomAccessGraph> Seq<G> {
    /// Creates a new sequential visit.
    ///
    /// # Arguments
    /// * `graph`: an immutable reference to the graph to visit.
    pub fn new(graph: G) -> Self {
        let num_nodes = graph.num_nodes();
        Self {
            graph,
            visited: BitVec::new(num_nodes),
            queue: VecDeque::new(),
        }
    }
}

impl<G: RandomAccessGraph> Sequential<EventPred> for Seq<G> {
    fn visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T,
        E,
        C: FnMut(&mut T, EventPred) -> ControlFlow<E, ()>,
        F: FnMut(&mut T, FilterArgsPred) -> bool,
    >(
        &mut self,
        roots: R,
        mut init: T,
        mut callback: C,
        mut filter: F,
    ) -> ControlFlow<E, ()> {
        self.queue.clear();

        for root in roots {
            if self.visited[root]
                || !filter(
                    &mut init,
                    FilterArgsPred {
                        node: root,
                        pred: root,
                        distance: 0,
                    },
                )
            {
                continue;
            }

            // We call the init event only if there are some non-filtered roots
            if self.queue.is_empty() {
                callback(&mut init, EventPred::Init {})?;
            }

            self.visited.set(root, true);
            self.queue.push_back(Some(
                NonMaxUsize::new(root).expect("node index should never be usize::MAX"),
            ));

            callback(
                &mut init,
                EventPred::Unknown {
                    node: root,
                    pred: root,
                    distance: 0,
                },
            )?;
        }

        if self.queue.is_empty() {
            return Continue(());
        }

        callback(
            &mut init,
            EventPred::DistanceChanged {
                distance: 0,
                nodes: self.queue.len(),
            },
        )?;

        // Insert marker
        self.queue.push_back(None);
        let mut distance = 1;

        while let Some(current_node) = self.queue.pop_front() {
            match current_node {
                Some(node) => {
                    let node = node.into();
                    for succ in self.graph.successors(node) {
                        let (node, pred) = (succ, node);
                        if !self.visited[succ] {
                            if filter(
                                &mut init,
                                FilterArgsPred {
                                    node,
                                    pred,

                                    distance,
                                },
                            ) {
                                self.visited.set(succ, true);
                                callback(
                                    &mut init,
                                    EventPred::Unknown {
                                        node,
                                        pred,

                                        distance,
                                    },
                                )?;
                                self.queue.push_back(Some(
                                    NonMaxUsize::new(succ)
                                        .expect("node index should never be usize::MAX"),
                                ))
                            }
                        } else {
                            callback(&mut init, EventPred::Known { node, pred })?;
                        }
                    }
                }
                None => {
                    // We are at the end of the current level, so
                    // we increment the distance and add a separator.
                    if !self.queue.is_empty() {
                        callback(
                            &mut init,
                            EventPred::DistanceChanged {
                                distance,
                                nodes: self.queue.len(),
                            },
                        )?;
                        distance += 1;
                        self.queue.push_back(None);
                    }
                }
            }
        }

        callback(&mut init, EventPred::Done {})
    }

    fn reset(&mut self) {
        self.queue.clear();
        self.visited.fill(false);
    }
}
