/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Fontana Tommaso
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::{RandomAccessGraph, RandomAccessLabeling};
use crate::visits::{
    Sequential,
    breadth_first::{EventPred, FilterArgsPred},
};
use anyhow::Result;
use nonmax::NonMaxUsize;
use std::{collections::VecDeque, ops::ControlFlow, ops::ControlFlow::Continue};
use sux::bits::BitVec;
use sux::traits::BitVecOpsMut;

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
/// use webgraph::visits::*;
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
///          if let breadth_first::EventPred::Visit { node, distance, .. } = event {
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
/// use webgraph::visits::*;
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
/// use webgraph::visits::*;
/// use webgraph::graphs::vec_graph::VecGraph;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
/// for event in &mut breadth_first::Seq::new(&graph) {
///    println!("Event: {:?}", event);
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
        assert_ne!(
            num_nodes,
            usize::MAX,
            "The BFS Seq visit cannot be used on graphs with usize::MAX nodes."
        );
        Self {
            graph,
            visited: BitVec::new(num_nodes),
            queue: VecDeque::new(),
        }
    }

    /// Returns an iterator over the nodes visited by a BFS visit starting in parallel from multiple nodes.
    pub fn iter_from_roots(
        &mut self,
        roots: impl IntoIterator<Item = usize>,
    ) -> Result<BfsOrderFromRoots<'_, 'a, G>> {
        BfsOrderFromRoots::new(self, roots)
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
    type Item = IterEvent;
    type IntoIter = BfsOrder<'a, 'b, G>;

    fn into_iter(self) -> Self::IntoIter {
        BfsOrder::new(self)
    }
}

/// Iterator on **all nodes** of the graph in a BFS order
pub struct BfsOrder<'a, 'b, G: RandomAccessGraph> {
    visit: &'a mut Seq<'b, G>,
    /// The root of the current visit.
    root: usize,
    /// The current node being enumerated, i.e. the parent of the nodes returned
    /// by `succ`
    parent: usize,
    /// The current distance from the root.
    distance: usize,
    /// The successors of the `parent` node, this is done to be able to return
    /// also the parent.
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
            root: 0,
            parent: 0,
            distance: 0,
            succ,
            visited_nodes: 0,
        }
    }
}

/// An event returned by the BFS iterator [`BfsOrder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IterEvent {
    /// The root of the current visit
    pub root: usize,
    /// The parent of the current node
    pub parent: usize,
    /// The current node being visited
    pub node: usize,
    /// The distance of the current node from the root
    pub distance: usize,
}

impl<'a, 'b, G: RandomAccessGraph> Iterator for BfsOrder<'a, 'b, G> {
    type Item = IterEvent;

    fn next(&mut self) -> Option<Self::Item> {
        // handle the first node separately, as we need to pre-fill the succ
        // iterator to be able to implement `new`
        if self.visited_nodes == 0 {
            self.visited_nodes += 1;
            self.visit.visited.set(self.root, true);
            self.visit.queue.push_back(None);
            return Some(IterEvent {
                root: self.root,
                parent: self.root,
                node: self.root,
                distance: 0,
            });
        }
        loop {
            // fast path, if the successors iterator is not exhausted, we can just return the next node
            for succ in &mut self.succ {
                if self.visit.visited[succ] {
                    continue; // skip already visited nodes
                }

                // if it's a new node, we visit it and add it to the queue
                // of nodes whose successors we will visit
                let node = NonMaxUsize::new(succ);
                debug_assert!(node.is_some(), "Node index should never be usize::MAX");
                let node = unsafe { node.unwrap_unchecked() };
                self.visit.queue.push_back(Some(node));

                self.visit.visited.set(succ as _, true);
                self.visited_nodes += 1;
                return Some(IterEvent {
                    root: self.root,
                    parent: self.parent,
                    node: succ,
                    distance: self.distance,
                });
            }

            // the successors are exhausted, so we need to move to the next node
            loop {
                match self.visit.queue.pop_front().expect(
                    "Queue should never be empty here, as we always add a level separator after the first node.",
                ) {
                    // if we have a node, we can continue visiting its successors
                    Some(node) => {
                        self.parent = node.into();
                        // reset the successors iterator for the new current node
                        self.succ = self.visit.graph.successors(self.parent).into_iter();
                        break;
                    }
                    // new level separator, so we increment the distance
                    None => {
                        // if the queue is not empty, we need to add a new level separator
                        if !self.visit.queue.is_empty() {
                            self.distance += 1;
                            self.visit.queue.push_back(None);
                            continue;
                        }
                        self.distance = 0; // new visits, new distance

                        // the queue is empty, we need to find the next unvisited node
                        while self.visit.visited[self.root] {
                            self.root += 1;
                            if self.root >= self.visit.graph.num_nodes() {
                                return None;
                            }
                        }

                        self.visited_nodes += 1;
                        self.visit.visited.set(self.root, true);
                        self.visit.queue.push_back(None);

                        self.parent = self.root;
                        self.succ = self.visit.graph.successors(self.root).into_iter();

                        return Some(IterEvent {
                            root: self.root,
                            parent: self.root,
                            node: self.root,
                            distance: self.distance,
                        });
                    }
                }
            }
        }
    }
}

impl<'a, 'b, G: RandomAccessGraph> ExactSizeIterator for BfsOrder<'a, 'b, G> {
    fn len(&self) -> usize {
        self.visit.graph.num_nodes() - self.visited_nodes
    }
}

/// Iterator on the nodes reachable from the given roots in a BFS order
pub struct BfsOrderFromRoots<'a, 'b, G: RandomAccessGraph> {
    visit: &'a mut Seq<'b, G>,
    /// The current node being enumerated, i.e. the parent of the nodes returned
    /// by `succ`
    parent: usize,
    /// The current distance from the root.
    distance: usize,
    /// The successors of the `parent` node, this is done to be able to return
    /// also the parent.
    succ: <<G as RandomAccessLabeling>::Labels<'a> as IntoIterator>::IntoIter,
}

/// An event returned by the BFS iterator that starts from possibly multiple roots [`BfsOrderFromRoots`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IterFromRootsEvent {
    /// The parent of the current node
    pub parent: usize,
    /// The current node being visited
    pub node: usize,
    /// The distance of the current node from the root
    pub distance: usize,
}

impl<'a, 'b, G: RandomAccessGraph> BfsOrderFromRoots<'a, 'b, G> {
    pub fn new(
        visit: &'a mut Seq<'b, G>,
        roots: impl IntoIterator<Item = usize>,
    ) -> Result<BfsOrderFromRoots<'a, 'b, G>> {
        visit.reset(); // ensure we start from a clean state
        // put the roots in the queue, and add a level separator
        visit.queue.extend(
            roots
                .into_iter()
                .map(|root| Some(NonMaxUsize::new(root).unwrap())),
        );
        visit.queue.push_back(None);

        // setup the succ iterator for after we finish visiting the roots
        let first_root: usize = visit.queue[0].unwrap().into();
        let succ = visit.graph.successors(first_root).into_iter();
        Ok(BfsOrderFromRoots {
            visit,
            parent: first_root,
            distance: 0,
            succ,
        })
    }
}

impl<'a, 'b, G: RandomAccessGraph> Iterator for BfsOrderFromRoots<'a, 'b, G> {
    type Item = IterFromRootsEvent;
    fn next(&mut self) -> Option<Self::Item> {
        // if the distance is zero, we are visiting the roots, so we need to
        // return the roots as the first nodes, and put themself as their parents
        // and then re-enqueue them so we can visit their successors
        if self.distance == 0 {
            // we always put the None level separator at the end of the queue, so there will
            // always be at least one element in the queue
            let element = self.visit.queue.pop_front().unwrap();

            if let Some(node) = element {
                let node = node.into();
                self.visit.visited.set(node, true);
                self.visit.queue.push_back(element); // re-enqueue the node to visit its successors later
                return Some(IterFromRootsEvent {
                    parent: node,
                    node,
                    distance: 0,
                });
            } else {
                // finished the roots
                // add a level separator so we know where the distance 1 nodes start
                self.visit.queue.push_back(None);
                // succ and parent were already set to the first root, so we can fall through
            }
        }

        loop {
            // now that the roots are handled, we can proceed as the BFSOrder
            for succ in &mut self.succ {
                if self.visit.visited[succ] {
                    continue; // skip already visited nodes
                }

                // if it's a new node, we visit it and add it to the queue
                let node = NonMaxUsize::new(succ);
                debug_assert!(node.is_some(), "Node index should never be usize::MAX");
                let node = unsafe { node.unwrap_unchecked() };
                self.visit.queue.push_back(Some(node));

                self.visit.visited.set(succ as _, true);
                return Some(IterFromRootsEvent {
                    parent: self.parent,
                    node: succ,
                    distance: self.distance,
                });
            }

            'inner: loop {
                // succesors exhausted, we must look in the queue
                match self.visit.queue.pop_front().expect(
                    "Queue should never be empty here, as we always add a level separator after the first node.",
                ) {
                    // if we have a node, we can continue visiting its successors
                    Some(node) => {
                        self.parent = node.into();
                        // reset the successors iterator for the new current node
                        self.succ = self.visit.graph.successors(self.parent).into_iter();
                        break 'inner;
                    }
                    // new level separator, so we increment the distance
                    None => {
                        // if the queue is empty, we are done
                        if self.visit.queue.is_empty() {
                            return None; // no more nodes to visit
                        }
                        // we need to add a new level separator
                        self.visit.queue.push_back(None);
                        self.distance += 1;
                        continue 'inner;
                    }
                }
            }
        }
    }
}
