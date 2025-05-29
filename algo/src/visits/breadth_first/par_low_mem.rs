/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::visits::{
    breadth_first::{EventPred, FilterArgsPred},
    Parallel,
};
use parallel_frontier::Frontier;
use rayon::{prelude::*, ThreadPool};
use std::{
    ops::ControlFlow::{self, Continue},
    sync::atomic::Ordering,
};
use sux::bits::AtomicBitVec;
use webgraph::{traits::RandomAccessGraph, utils::Granularity};

/// Low-memory parallel breadth-first visits.
///
/// “Low memory” refers to the fact that the visit is parallelized by dividing
/// the visit queue in chunks of approximately equal size, but nodes are visited
/// when they are discovered, rather than when they are extracted from the visit
/// queue. This approach makes unnecessary to store parents in the visit queue,
/// thus reducing the memory usage, even if this visit supports [`EventPred`].
/// However, the visiting cost is distributed unevenly among the threads, as it
/// depends on the sum of the outdegrees of the nodes in a chunk, which might
/// differ significantly between chunks.
///
/// If the cost of the callbacks is significant, you can use a [fair parallel
/// visit](crate::visits::breadth_first::ParFairNoPred) to distribute the
/// visiting cost evenly among the threads.
///
/// # Examples
///
/// Let's compute the breadth-first tree starting from 0. We will be using a
/// [`SyncSlice`](sync_cell_slice::SyncSlice) from the [`sync_cell_slice`] crate
/// to store the parent of each node.
///
/// ```
/// use webgraph_algo::visits::Parallel;
/// use webgraph_algo::visits::breadth_first::{*, self};
/// use webgraph_algo::thread_pool;
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph::labels::proj::Left;
/// use std::sync::atomic::AtomicUsize;
/// use std::sync::atomic::Ordering;
/// use std::ops::ControlFlow::Continue;
/// use no_break::NoBreak;
/// use sync_cell_slice::SyncSlice;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
/// let mut visit = breadth_first::ParLowMem::new(&graph);
/// let mut tree = vec![0_usize; 4];
/// let mut tree_sync = tree.as_sync_slice();
/// visit.par_visit(
///     [0],
///     |event|
///     {
///         // Store the parent
///         if let EventPred::Unknown { node, pred, ..} = event {
///             // There will be exactly one set for each node
///             unsafe { tree_sync[node].set(pred) };
///         }
///         Continue(())
///     },
///     &thread_pool![],
/// ).continue_value_no_break();
///
/// assert_eq!(tree[0], 0);
/// assert_eq!(tree[1], 0);
/// assert_eq!(tree[2], 1);
/// assert_eq!(tree[3], 1);
/// ```
pub struct ParLowMem<G: RandomAccessGraph> {
    graph: G,
    granularity: usize,
    visited: AtomicBitVec,
}

impl<G: RandomAccessGraph> ParLowMem<G> {
    /// Creates a low-memory parallel breadth-first visit.
    ///
    /// This constructor uses a default granularity of 128 nodes. Use
    /// [`with_granularity`](Self::with_granularity) to set a different
    ///  granularity.
    ///
    /// # Arguments
    ///
    /// * `graph`: the graph to visit.
    pub fn new(graph: G) -> Self {
        Self::with_granularity(graph, Granularity::Nodes(128))
    }

    /// Creates a low-memory parallel breadth-first visit.
    ///
    /// # Arguments
    ///
    /// * `graph`: the graph to visit.
    ///
    /// * `granularity`: High granularity reduces overhead, but may lead to
    ///   decreased performance on graphs with a skewed outdegree distribution.
    ///   From this parameter, we derive a [node
    ///   granularity](Granularity::node_granularity).
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

impl<G: RandomAccessGraph + Sync> Parallel<EventPred> for ParLowMem<G> {
    fn par_visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T: Clone + Send + Sync,
        E: Send,
        C: Fn(&mut T, EventPred) -> ControlFlow<E, ()> + Sync,
        F: Fn(&mut T, FilterArgsPred) -> bool + Sync,
    >(
        &mut self,
        roots: R,
        mut init: T,
        callback: C,
        filter: F,
        thread_pool: &ThreadPool,
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

            // We call the init event only if there are some non-filtered roots
            if filtered_roots.is_empty() {
                callback(&mut init, EventPred::Init {})?;
            }

            filtered_roots.push(root);
            self.visited.set(root, true, Ordering::Relaxed);

            callback(
                &mut init,
                EventPred::Unknown {
                    node: root,
                    pred: root,
                    distance: 0,
                },
            )?;
        }

        if filtered_roots.is_empty() {
            return Continue(());
        }

        // We do not provide a capacity in the hope of allocating dynamically
        // space as the frontiers grow.
        let mut curr_frontier = Frontier::with_threads(thread_pool, None);
        // Inject the filtered roots in the frontier.
        curr_frontier.as_mut()[0] = filtered_roots;
        let mut next_frontier = Frontier::with_threads(thread_pool, None);
        let mut distance = 1;

        // Visit the connected component
        while !curr_frontier.is_empty() {
            callback(
                &mut init,
                EventPred::DistanceChanged {
                    distance: distance - 1,
                    nodes: curr_frontier.len(),
                },
            )?;
            thread_pool.install(|| {
                curr_frontier
                    .par_iter()
                    .chunks(self.granularity)
                    .try_for_each_with(init.clone(), |init, chunk| {
                        chunk.into_iter().try_for_each(|&node| {
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
                                            distance,
                                        },
                                    ) {
                                        if !self.visited.swap(succ, true, Ordering::Relaxed) {
                                            callback(
                                                init,
                                                EventPred::Unknown {
                                                    node,
                                                    pred,
                                                    distance,
                                                },
                                            )?;
                                            next_frontier.push(succ);
                                        } else {
                                            callback(init, EventPred::Known { node, pred })?;
                                        }
                                    }

                                    Continue(())
                                })
                        })
                    })
            })?;
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
