/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Fontana Tommaso
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
use webgraph::traits::{RandomAccessGraph, RandomAccessLabeling};

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
/// [`EventPred::Visit`] event.
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
///
/// The visit also implements the [`IntoIterator`] trait, so it can be used
/// in a `for` loop to iterate over all nodes in the order they are visited:
///
/// ```rust
/// use webgraph_algo::visits::*;
/// use webgraph::graphs::vec_graph::VecGraph;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
/// for node in &mut breadth_first::Seq::new(&graph) {
///    println!("Visited node: {}", node);
/// }
/// ```
///
/// Note that the iterator modifies the state of the visit, so it can re-use
/// the allocations.
pub struct Seq<'a, G: RandomAccessGraph> {
    graph: &'a G,
    visited: BitVec,
    /// The visit queue; to avoid storing distances, we use `None` as a
    /// separator between levels. [`NonMaxUsize`] is used to avoid
    /// storage for the option variant tag.
    queue: VecDeque<Option<NonMaxUsize>>,
}

impl<'a, G: RandomAccessGraph> Seq<'a, G> {
    /// Creates a new sequential visit.
    ///
    /// # Arguments
    /// * `graph`: an immutable reference to the graph to visit.
    pub fn new(graph: &'a G) -> Self {
        let num_nodes = graph.num_nodes();
        Self {
            graph,
            visited: BitVec::new(num_nodes),
            queue: VecDeque::new(),
        }
    }
}

impl<'a, G: RandomAccessGraph> Sequential<EventPred> for Seq<'a, G> {
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
                EventPred::Visit {
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
            EventPred::FrontierSize {
                distance: 0,
                size: self.queue.len(),
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
                                    EventPred::Visit {
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
                            callback(&mut init, EventPred::Revisit { node, pred })?;
                        }
                    }
                }
                None => {
                    // We are at the end of the current level, so
                    // we increment the distance and add a separator.
                    if !self.queue.is_empty() {
                        callback(
                            &mut init,
                            EventPred::FrontierSize {
                                distance,
                                size: self.queue.len(),
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

impl<'a, 'b, G: RandomAccessGraph> IntoIterator for &'a mut Seq<'b, G> {
    type Item = usize;
    type IntoIter = BfsOrder<'a, 'b, G>;

    fn into_iter(self) -> Self::IntoIter {
        BfsOrder::new(self)
    }
}

/// Iterator on **all nodes** of the graph in a BFS order
pub struct BfsOrder<'a, 'b, G: RandomAccessGraph> {
    visit: &'a mut Seq<'b, G>,
    /// If the queue is empty, resume the BFS from that node.
    ///
    /// This allows initializing the BFS from all orphan nodes without reading
    /// the reverse graph.
    start: usize,
    succ: <<G as RandomAccessLabeling>::Labels<'a> as IntoIterator>::IntoIter,
    /// Number of visited nodes, used to compute the length of the iterator.
    visited_nodes: usize,
}

impl<'a, 'b, G: RandomAccessGraph> BfsOrder<'a, 'b, G> {
    pub fn new(visit: &'a mut Seq<'b, G>) -> BfsOrder<'a, 'b, G> {
        visit.reset(); // ensure we start from a clean state
        let succ = visit.graph.successors(0).into_iter();
        BfsOrder {
            visit,
            start: 0,
            succ,
            visited_nodes: 0,
        }
    }
}

impl<'a, 'b, G: RandomAccessGraph> Iterator for BfsOrder<'a, 'b, G> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        let current_node = match self.visit.queue.pop_front() {
            Some(Some(node)) => node.into(),
            _ => {
                while self.visit.visited[self.start] {
                    self.start += 1;
                    if self.start >= self.visit.graph.num_nodes() {
                        return None;
                    }
                }
                self.visit.visited.set(self.start, true);
                self.start
            }
        };

        for succ in self.visit.graph.successors(current_node) {
            if !self.visit.visited[succ] {
                self.visit.queue.push_back(NonMaxUsize::new(succ));
                self.visit.visited.set(succ as _, true);
            }
        }
        self.visited_nodes += 1;
        Some(current_node)
    }
}

impl<'a, 'b, G: RandomAccessGraph> ExactSizeIterator for BfsOrder<'a, 'b, G> {
    fn len(&self) -> usize {
        self.visit.graph.num_nodes() - self.visited_nodes
    }
}
