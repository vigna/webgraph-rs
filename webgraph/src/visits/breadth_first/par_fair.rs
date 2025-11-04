/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::visits::{breadth_first::*, Parallel};
use crate::{traits::RandomAccessGraph, utils::Granularity};
use parallel_frontier::Frontier;
use rayon::prelude::*;
use std::{
    ops::ControlFlow::{self, Continue},
    sync::atomic::Ordering,
};
use sux::bits::AtomicBitVec;
use sux::traits::AtomicBitVecOps;

/// Fair parallel breadth-first visits.
///
/// “Fairness” refers to the fact that the visit is parallelized by dividing the
/// visit queue in chunks of approximately equal size; threads consume the
/// chunks, and visit the associated nodes. Thus, the visiting cost is
/// distributed evenly among the threads, albeit the work done on the
/// enumeration of successors depends on the sum of the outdegrees nodes in a
/// chunk, which might differ significantly between chunks.
///
/// There are two version of the visit, which are type aliases to the same
/// common implementation: [`ParFairNoPred`] and [`ParFairPred`].
///
/// * [`ParFairNoPred`] does not keep track of predecessors; it can be used, for
///   example, to compute distances.
/// * [`ParFairPred`] keeps track of predecessors; it can be used, for example,
///   to compute a visit tree.
///
/// Each type of visit uses incrementally more space:
/// * [`ParFairNoPred`] uses one bit per node to remember known nodes and a
///   queue of `usize` representing nodes;
/// * [`ParFairPred`] uses one bit per node to remember known nodes and a queue
///   of pairs of `usize` representing nodes and their parents.
///
/// If you need predecessors but the cost of the callbacks is not significant
/// you can use a [low-memory parallel
/// visit](crate::visits::breadth_first::ParLowMem) instead.
///
/// The visits differ also in the type of events they generate:
/// * [`ParFairNoPred`] generates events of type [`EventNoPred`].
/// * [`ParFairPred`] generates events of type [`EventPred`].
///
/// With respect to [`EventNoPred`], [`EventPred`] provides the predecessor of
/// the current node.
///
/// # Examples
///
/// Let's compute the distances from 0. We will be using a
/// [`SyncSlice`](sync_cell_slice::SyncSlice) from the [`sync_cell_slice`] crate
/// to store the parent of each node.
///
/// ```
/// use webgraph::visits::Parallel;
/// use webgraph::visits::breadth_first::{*, self};
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph::labels::proj::Left;
/// use std::sync::atomic::AtomicUsize;
/// use std::sync::atomic::Ordering;
/// use std::ops::ControlFlow::Continue;
/// use sync_cell_slice::SyncSlice;
/// use no_break::NoBreak;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
/// let mut visit = breadth_first::ParFairNoPred::new(&graph);
/// let mut d = [0_usize; 4];
/// let mut d_sync = d.as_sync_slice();
/// visit.par_visit(
///     [0],
///     |event| {
///         // Set distance from 0
///         if let EventNoPred::Visit { node, distance, ..} = event {
///             // There will be exactly one set for each node
///             unsafe { d_sync[node].set(distance) };
///         }
///         Continue(())
///     },
/// ).continue_value_no_break();
///
/// assert_eq!(d[0], 0);
/// assert_eq!(d[1], 1);
/// assert_eq!(d[2], 2);
/// assert_eq!(d[3], 2);
/// ```
pub struct ParFair<G: RandomAccessGraph, const PRED: bool = false> {
    graph: G,
    granularity: usize,
    visited: AtomicBitVec,
}

/// A [fair parallel breadth-first visit](ParFair) that keeps track of
/// predecessors.
pub type ParFairPred<G> = ParFair<G, true>;

/// A [fair parallel breadth-first visit](ParFair) that does not keep track of
/// predecessors.
pub type ParFairNoPred<G> = ParFair<G, false>;

impl<G: RandomAccessGraph, const P: bool> ParFair<G, P> {
    /// Creates a fair parallel breadth-first visit.
    ///
    /// This constructor uses a default granularity of 128 nodes. Use
    /// [`with_granularity`](Self::with_granularity) to set a different
    ///  granularity.
    ///
    /// # Arguments
    ///
    /// * `graph`: the graph to visit.
    #[inline(always)]
    pub fn new(graph: G) -> Self {
        Self::with_granularity(graph, Granularity::Nodes(128))
    }

    /// Creates a fair parallel breadth-first visit.
    ///
    /// # Arguments
    ///
    /// * `graph`: the graph to visit.
    ///
    /// * `granularity`: High granularity reduces overhead, but may lead to
    ///   decreased performance on graphs with a skewed outdegree distribution.
    ///   From this parameter, we derive a [node
    ///   granularity](Granularity::node_granularity).
    #[inline(always)]
    pub fn with_granularity(graph: G, granularity: Granularity) -> Self {
        let num_nodes = graph.num_nodes();
        let num_arcs = graph.num_arcs();
        Self {
            graph,
            granularity: granularity.node_granularity(num_nodes, Some(num_arcs)),
            visited: AtomicBitVec::new(num_nodes),
        }
    }
}

impl<G: RandomAccessGraph + Sync> Parallel<EventNoPred> for ParFair<G, false> {
    fn par_visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T: Clone + Send + Sync,
        E: Send,
        C: Fn(&mut T, EventNoPred) -> ControlFlow<E, ()> + Sync,
        F: Fn(&mut T, FilterArgsNoPred) -> bool + Sync,
    >(
        &mut self,
        roots: R,
        mut init: T,
        callback: C,
        filter: F,
    ) -> ControlFlow<E, ()> {
        let mut filtered_roots = vec![];

        for root in roots {
            if self.visited.get(root, Ordering::Relaxed)
                || !filter(
                    &mut init,
                    FilterArgsNoPred {
                        node: root,
                        distance: 0,
                    },
                )
            {
                continue;
            }

            filtered_roots.push(root);
            self.visited.set(root, true, Ordering::Relaxed);
        }

        if filtered_roots.is_empty() {
            return Continue(());
        }

        callback(&mut init, EventNoPred::Init {})?;
        // We do not provide a capacity in the hope of allocating dynamically
        // space as the frontiers grow.
        // TODO: Frontier::with_threads needs to be updated to work without explicit ThreadPool
        let mut curr_frontier = Frontier::new();
        // Inject the filtered roots in the frontier.
        curr_frontier.as_mut()[0] = filtered_roots;
        let mut next_frontier = Frontier::new();
        let mut distance = 0;

        while !curr_frontier.is_empty() {
            callback(
                &mut init,
                EventNoPred::FrontierSize {
                    distance,
                    sizes: curr_frontier.len(),
                },
            )?;
            let distance_plus_one = distance + 1;
            // TODO: Handle thread_pool.install() removal - decide between rayon::scope or direct execution
            {
                curr_frontier
                    .par_iter()
                    .chunks(self.granularity)
                    .try_for_each_with(init.clone(), |init, chunk| {
                        chunk.into_iter().try_for_each(|&node| {
                            callback(init, EventNoPred::Visit { node, distance })?;
                            self.graph
                                .successors(node)
                                .into_iter()
                                .try_for_each(|succ| {
                                    // TODO: confusing
                                    let node = succ;
                                    if filter(
                                        init,
                                        FilterArgsNoPred {
                                            node,
                                            distance: distance_plus_one,
                                        },
                                    ) {
                                        if !self.visited.swap(succ, true, Ordering::Relaxed) {
                                            next_frontier.push(succ);
                                        } else {
                                            callback(init, EventNoPred::Revisit { node })?;
                                        }
                                    }

                                    Continue(())
                                })?;

                            Continue(())
                        })
                    })
            }?;

            distance += 1;
            // Swap the frontiers
            std::mem::swap(&mut curr_frontier, &mut next_frontier);
            // Clear the frontier we will fill in the next iteration
            next_frontier.clear();
        }

        callback(&mut init, EventNoPred::Done {})?;

        Continue(())
    }

    fn reset(&mut self) {
        self.visited.fill(false, Ordering::Relaxed);
    }
}

impl<G: RandomAccessGraph + Sync> Parallel<EventPred> for ParFair<G, true> {
    fn par_visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T: Clone + Send + Sync + Sync,
        E: Send,
        C: Fn(&mut T, EventPred) -> ControlFlow<E, ()> + Sync,
        F: Fn(&mut T, <EventPred as super::super::Event>::FilterArgs) -> bool + Sync,
    >(
        &mut self,
        roots: R,
        mut init: T,
        callback: C,
        filter: F,
    ) -> ControlFlow<E, ()> {
        let mut filtered_roots = vec![];

        for root in roots {
            if self.visited.get(root, Ordering::Relaxed)
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

            filtered_roots.push((root, root));
            self.visited.set(root, true, Ordering::Relaxed);
        }

        if filtered_roots.is_empty() {
            return Continue(());
        }

        callback(&mut init, EventPred::Init {})?;
        // We do not provide a capacity in the hope of allocating dynamically
        // space as the frontiers grow.
        // TODO: Frontier::with_threads needs to be updated to work without explicit ThreadPool
        let mut curr_frontier = Frontier::new();
        // Inject the filtered roots in the frontier.
        curr_frontier.as_mut()[0] = filtered_roots;
        let mut next_frontier = Frontier::new();
        let mut distance = 0;

        while !curr_frontier.is_empty() {
            callback(
                &mut init,
                EventPred::FrontierSize {
                    distance,
                    size: curr_frontier.len(),
                },
            )?;
            let distance_plus_one = distance + 1;
            // TODO: Handle thread_pool.install() removal - decide between rayon::scope or direct execution
            {
                curr_frontier
                    .par_iter()
                    .chunks(self.granularity)
                    .try_for_each_with(init.clone(), |init, chunk| {
                        chunk.into_iter().try_for_each(|&(node, pred)| {
                            callback(
                                init,
                                EventPred::Visit {
                                    node,
                                    pred,
                                    distance,
                                },
                            )?;
                            self.graph
                                .successors(node)
                                .into_iter()
                                .try_for_each(|succ| {
                                    let (node, pred) = (succ, node);
                                    if filter(
                                        init,
                                        FilterArgsPred {
                                            node,
                                            pred,
                                            distance: distance_plus_one,
                                        },
                                    ) {
                                        if !self.visited.swap(succ, true, Ordering::Relaxed) {
                                            next_frontier.push((node, pred));
                                        } else {
                                            callback(init, EventPred::Revisit { node, pred })?;
                                        }
                                    }

                                    Continue(())
                                })?;

                            Continue(())
                        })
                    })
            }?;
            distance += 1;
            // Swap the frontiers
            std::mem::swap(&mut curr_frontier, &mut next_frontier);
            // Clear the frontier we will fill in the next iteration
            next_frontier.clear();
        }

        callback(&mut init, EventPred::Done {})
    }

    fn reset(&mut self) {
        self.visited.fill(false, Ordering::Relaxed);
    }
}
