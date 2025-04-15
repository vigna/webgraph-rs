/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{
    distances::exact_sum_sweep::scc_graph::SccGraph,
    sccs::{self, Sccs},
    utils::math,
    visits::{
        breadth_first::{EventNoPred, ParFairNoPred},
        FilterArgs, Parallel,
    },
};
use dsi_progress_logger::*;
use no_break::NoBreak;
use nonmax::NonMaxUsize;
use rayon::{prelude::*, ThreadPool};
use std::{
    ops::ControlFlow::Continue,
    sync::{
        atomic::{AtomicUsize, Ordering},
        RwLock,
    },
};
use sux::bits::AtomicBitVec;
use sync_cell_slice::SyncSlice;
use webgraph::traits::RandomAccessGraph;

use super::{Level, Missing};

pub(super) struct DirExactSumSweepComputer<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    V1: Parallel<EventNoPred> + Sync,
    V2: Parallel<EventNoPred> + Sync,
    OL: Level,
> {
    pub graph: &'a G1,
    pub transpose: &'a G2,
    pub num_nodes: usize,
    pub radial_vertices: AtomicBitVec,
    /// The lower bound of the diameter.
    pub diameter_low: usize,
    /// The upper bound of the radius.
    pub radius_high: usize,
    /// A vertex whose eccentricity equals the diameter.
    pub diameter_vertex: usize,
    /// A vertex whose eccentricity equals the radius.
    pub radius_vertex: usize,
    /// Number of iterations performed until now.
    pub iterations: usize,
    /// The lower bound of the forward eccentricities.
    pub forward_low: Box<[usize]>,
    /// The upper bound of the forward eccentricities.
    pub forward_high: Box<[usize]>,
    /// The lower bound of the backward eccentricities.
    pub backward_low: Box<[usize]>,
    /// The upper bound of the backward eccentricities.
    pub backward_high: Box<[usize]>,
    /// Number of iterations before the radius was found.
    pub radius_iterations: Option<usize>,
    /// Number of iterations before the diameter was found.
    pub diameter_iterations: Option<usize>,
    /// Number of iterations before all forward eccentricities were found.
    pub forward_iter: Option<usize>,
    /// Number of iterations before all eccentricities were found.
    pub all_iter: Option<usize>,
    /// The strongly connected components.
    pub scc: Sccs,
    /// The strongly connected components diagram.
    pub scc_graph: SccGraph<G1, G2>,
    /// Total forward distance from already processed vertices (used as tie-break for the choice
    /// of the next vertex to process).
    pub forward_tot: Box<[usize]>,
    /// Total backward distance from already processed vertices (used as tie-break for the choice
    /// of the next vertex to process).
    pub backward_tot: Box<[usize]>,
    pub compute_radial_vertices: bool,
    pub visit: V1,
    pub transposed_visit: V2,
    _marker: std::marker::PhantomData<OL>,
}

impl<'a, G: RandomAccessGraph + Sync, OL: Level>
    DirExactSumSweepComputer<'a, G, G, ParFairNoPred<&'a G>, ParFairNoPred<&'a G>, OL>
{
    /// Build a new instance to compute the *ExactSumSweep* algorithm on
    /// symmetric (i.e., undirected) graphs.
    pub(super) fn new_symm(graph: &'a G, pl: &mut impl ProgressLog) -> Self {
        // TODO debug_assert!(check_symmetric(graph), "graph should be symmetric");

        let scc = sccs::symm_seq(graph, pl);
        let scc_graph = SccGraph::new_symm(&scc);
        let visit = ParFairNoPred::new(graph);
        let transposed_visit = ParFairNoPred::new(graph);

        Self::_new(
            graph,
            graph,
            None,
            scc,
            scc_graph,
            visit,
            transposed_visit,
            pl,
        )
    }
}

impl<'a, G1: RandomAccessGraph + Sync, G2: RandomAccessGraph + Sync, OL: Level>
    DirExactSumSweepComputer<'a, G1, G2, ParFairNoPred<&'a G1>, ParFairNoPred<&'a G2>, OL>
{
    /// Build a new instance to compute the *ExactSumSweep* algorithm on
    /// directed graphs.
    ///
    /// # Arguments
    /// * `graph`: the direct graph.
    /// * `transpose`: the transpose of `graph`.
    /// * `output`: the desired output of the algorithm.
    /// * `radial_vertices`: an [`AtomicBitVec`] where `v[i]` is true if node
    ///   `i` is to be considered radial vertex. If [`None`] the algorithm will
    ///   use the biggest connected component.
    /// * `pl`: a progress logger.
    pub(super) fn new(
        graph: &'a G1,
        transpose: &'a G2,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ProgressLog,
    ) -> Self {
        assert_eq!(graph.num_nodes(), transpose.num_nodes());
        assert_eq!(graph.num_arcs(), transpose.num_arcs());
        /* TODOdebug_assert!(
            check_transposed(graph, transpose),
            "transpose should be the transpose of graph"
        );*/

        let scc = sccs::tarjan(graph, pl);
        let scc_graph = SccGraph::new(graph, transpose, &scc, pl);
        let visit = ParFairNoPred::new(graph);
        let transposed_visit = ParFairNoPred::new(transpose);

        Self::_new(
            graph,
            transpose,
            radial_vertices,
            scc,
            scc_graph,
            visit,
            transposed_visit,
            pl,
        )
    }
}

impl<
        'a,
        G1: RandomAccessGraph + Sync,
        G2: RandomAccessGraph + Sync,
        V1: Parallel<EventNoPred> + Sync,
        V2: Parallel<EventNoPred> + Sync,
        OL: Level,
    > DirExactSumSweepComputer<'a, G1, G2, V1, V2, OL>
{
    #[allow(clippy::too_many_arguments)]
    fn _new(
        graph: &'a G1,
        transpose: &'a G2,
        radial_vertices: Option<AtomicBitVec>,
        scc: Sccs,
        scc_graph: SccGraph<G1, G2>,
        visit: V1,
        transposed_visit: V2,
        pl: &mut impl ProgressLog,
    ) -> Self {
        let num_nodes = graph.num_nodes();

        let compute_radial_vertices = radial_vertices.is_none();
        let acc_radial = if let Some(r) = radial_vertices {
            debug_assert_eq!(r.len(), num_nodes);
            r
        } else {
            AtomicBitVec::new(num_nodes)
        };

        pl.info(format_args!("Initializing data structure"));

        DirExactSumSweepComputer {
            graph,
            transpose,
            num_nodes,
            forward_tot: vec![0; num_nodes].into_boxed_slice(),
            backward_tot: vec![0; num_nodes].into_boxed_slice(),
            forward_low: vec![0; num_nodes].into_boxed_slice(),
            forward_high: vec![num_nodes; num_nodes].into_boxed_slice(),
            backward_low: vec![0; num_nodes].into_boxed_slice(),
            backward_high: vec![num_nodes; num_nodes].into_boxed_slice(),
            scc_graph,
            scc,
            diameter_low: 0,
            radius_high: usize::MAX,
            radius_iterations: None,
            diameter_iterations: None,
            all_iter: None,
            forward_iter: None,
            iterations: 0,
            radial_vertices: acc_radial,
            radius_vertex: 0,
            diameter_vertex: 0,
            compute_radial_vertices,
            visit,
            transposed_visit,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<
        G1: RandomAccessGraph + Sync,
        G2: RandomAccessGraph + Sync,
        V1: Parallel<EventNoPred> + Sync,
        V2: Parallel<EventNoPred> + Sync,
        OL: Level,
    > DirExactSumSweepComputer<'_, G1, G2, V1, V2, OL>
{
    #[inline(always)]
    fn incomplete_forward(&self, index: usize) -> bool {
        self.forward_low[index] != self.forward_high[index]
    }

    #[inline(always)]
    fn incomplete_backward(&self, index: usize) -> bool {
        self.backward_low[index] != self.backward_high[index]
    }

    /// Performs `iterations` steps of the SumSweep heuristic, starting from vertex `start`.
    ///
    /// For more information see Section 3 of the paper.
    ///
    /// # Arguments
    /// * `start`: The starting vertex.
    /// * `iterations`: The number of iterations.
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A concurrent progress logger.
    fn sum_sweep_heuristic(
        &mut self,
        start: usize,
        iterations: usize,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) {
        self.step_sum_sweep(Some(start), true, thread_pool, pl, |node| {
            format!(
                "Performing initial forward SumSweep heuristic visit from {}...",
                node
            )
        });

        for i in 2..=iterations {
            if i % 2 == 0 {
                let v = math::argmax_filtered(&self.backward_tot, &self.backward_low, |i, _| {
                    self.incomplete_backward(i)
                });
                self.step_sum_sweep(v, false, thread_pool, pl, |node| {
                    format!(
                        "Performing initial backward SumSweep heuristic visit from {}...",
                        node
                    )
                });
            } else {
                let v = math::argmax_filtered(&self.forward_tot, &self.forward_low, |i, _| {
                    self.incomplete_forward(i)
                });
                self.step_sum_sweep(v, true, thread_pool, pl, |node| {
                    format!(
                        "Performing initial forward SumSweep heuristic visit from {}...",
                        node
                    )
                });
            }
        }
    }

    /// Computes diameter, radius, and/or all eccentricities.
    ///
    /// # Arguments
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A progress logger.
    pub fn compute(&mut self, thread_pool: &ThreadPool, pl: &mut impl ProgressLog) {
        if self.num_nodes == 0 {
            return;
        }

        pl.start("Computing ExactSumSweep...");
        let mut cpl = pl.concurrent();

        if self.compute_radial_vertices {
            self.compute_radial_vertices(thread_pool, &mut cpl);
        }

        let max_outdegree_vertex = thread_pool
            .install(|| {
                (0..self.num_nodes)
                    .into_par_iter()
                    .map(|v| (self.graph.outdegree(v), v))
                    .max_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)))
            })
            .unwrap()
            .1; // The iterator is not empty

        self.sum_sweep_heuristic(max_outdegree_vertex, 6, thread_pool, &mut cpl);

        let mut points = [self.graph.num_nodes() as f64; 5];
        let mut missing_nodes = self.find_missing_nodes(thread_pool, &mut cpl);
        let mut old_missing_nodes;

        pl.info(format_args!(
            "Missing nodes: {} out of {}",
            missing_nodes,
            self.num_nodes * 2
        ));

        while missing_nodes > 0 {
            let step_to_perform = math::argmax(points).expect("Could not find step to perform");

            match step_to_perform {
                0 => self.all_cc_upper_bound(thread_pool, &mut cpl),
                1 => {
                    let v = math::argmax_filtered(&self.forward_high, &self.forward_tot, |i, _| {
                        self.incomplete_forward(i)
                    });
                    self.step_sum_sweep(v, true, thread_pool, &mut cpl, |node| {
                        format!(
                            "Performing a forward BFV from a node maximizing the upper bound ({})...",
                            node
                        )
                    })
                }
                2 => {
                    let v = math::argmin_filtered(&self.forward_low, &self.forward_tot, |i, _| {
                        self.radial_vertices[i]
                    });
                    self.step_sum_sweep(v, true, thread_pool, &mut cpl, |node| {
                        format!(
                            "Performing a forward BFV from a node minimizing the lower bound ({})...",
                            node
                        )
                    })
                }
                3 => {
                    let v =
                        math::argmax_filtered(&self.backward_high, &self.backward_tot, |i, _| {
                            self.incomplete_backward(i)
                        });
                    self.step_sum_sweep(v, false, thread_pool, &mut cpl, |node| {
                        format!(
                            "Performing a backward BFV from a node maximizing the upper bound ({})...",
                            node
                        )
                    })
                }
                4 => {
                    let v =
                        math::argmax_filtered(&self.backward_tot, &self.backward_high, |i, _| {
                            self.incomplete_backward(i)
                        });
                    self.step_sum_sweep(v, false, thread_pool, &mut cpl, |node| {
                        format!(
                            "Performing a backward BFV from a node maximizing the distance sum ({})",
                            node
                        )
                    })
                }
                5.. => panic!(),
            }

            // Update each step utility.
            // For more information see Section 4.6 of the paper.

            old_missing_nodes = missing_nodes;
            missing_nodes = self.find_missing_nodes(thread_pool, &mut cpl);
            points[step_to_perform] = (old_missing_nodes - missing_nodes) as f64;

            // This is to make rust-analyzer happy as it cannot recognize mut reference
            #[allow(clippy::needless_range_loop)]
            for i in 0..points.len() {
                if i != step_to_perform && points[i] >= 0.0 {
                    points[i] += 2.0 / self.iterations as f64;
                }
            }

            pl.info(format_args!(
                "Missing nodes: {} out of {}",
                missing_nodes,
                self.num_nodes * 2
            ));
        }

        pl.done();
    }

    /// Uses a heuristic to decide which is the best pivot to choose in each strongly connected
    /// component, in order to perform the [`Self::all_cc_upper_bound`] method.
    ///
    /// # Arguments
    /// * `pl`: A progress logger..
    fn find_best_pivot(&self, pl: &mut impl ProgressLog) -> Vec<usize> {
        debug_assert!(self.num_nodes < usize::MAX);

        let mut pivot: Vec<Option<NonMaxUsize>> = vec![None; self.scc.num_components()];
        let components = self.scc.components();
        pl.expected_updates(Some(components.len()));
        pl.item_name("node");
        pl.display_memory(false);
        pl.start("Computing best pivots...");

        for (v, &component) in components.iter().enumerate().rev() {
            if let Some(p) = pivot[component] {
                let p = p.into();
                let current = self.backward_low[v]
                    + self.forward_low[v]
                    + if self.incomplete_forward(v) {
                        0
                    } else {
                        self.num_nodes
                    }
                    + if self.incomplete_backward(v) {
                        0
                    } else {
                        self.num_nodes
                    };

                let best = self.backward_low[p]
                    + self.forward_low[p]
                    + if self.incomplete_forward(p) {
                        0
                    } else {
                        self.num_nodes
                    }
                    + if self.incomplete_backward(p) {
                        0
                    } else {
                        self.num_nodes
                    };

                if current < best
                    || (current == best
                        && self.forward_tot[v] + self.backward_tot[v]
                            <= self.forward_tot[p] + self.backward_tot[p])
                {
                    pivot[component] = NonMaxUsize::new(v);
                }
            } else {
                pivot[component] = NonMaxUsize::new(v);
            }
            pl.light_update();
        }

        pl.done();

        pivot.into_iter().map(|x| x.unwrap().into()).collect()
    }

    /// Computes and stores in variable [`Self::radial_vertices`] the set of vertices that are
    /// either in the biggest strongly connected component or that are able to reach vertices in
    /// the biggest strongly connected component.
    ///
    /// # Arguments
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A progress logger.
    fn compute_radial_vertices(
        &mut self,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) {
        if self.num_nodes == 0 {
            return;
        }

        let component = self.scc.components();
        let scc_sizes = self.scc.compute_sizes();
        let max_size_scc = math::argmax(&scc_sizes).expect("Could not find max size scc.");

        // TODO: eliminate double scan
        pl.info(format_args!(
            "Searching for biggest strongly connected component"
        ));

        let mut v = self.num_nodes;

        while v > 0 {
            v -= 1;
            if component[v] == max_size_scc {
                break;
            }
        }
        pl.expected_updates(None);
        pl.item_name("node");
        pl.display_memory(false);
        pl.start("Computing radial vertices...");

        let radial_vertices = &self.radial_vertices;
        self.transposed_visit
            .par_visit_with(
                [v],
                pl.clone(),
                |pl, event| {
                    if let EventNoPred::Unknown { node, .. } = event {
                        pl.light_update();
                        radial_vertices.set(node, true, Ordering::Relaxed)
                    }
                    Continue(())
                },
                thread_pool,
            )
            .continue_value_no_break();
        self.transposed_visit.reset();

        pl.done();
    }

    /// Performs a (forward or backward) BFS, updating lower bounds on the eccentricities
    /// of all visited vertices.
    ///
    /// For more information see Section 4.1 of the paper.
    ///
    /// # Arguments
    /// * `start`: The starting vertex of the BFS. If [`None`], no visit happens.
    /// * `forward`: Whether the BFS is performed following the direction of edges or
    ///   in the opposite direction.
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A progress logger.
    /// * `message`: The message to print to the log.
    fn step_sum_sweep(
        &mut self,
        start: Option<usize>,
        forward: bool,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
        message: impl FnOnce(usize) -> String,
    ) {
        if let Some(start) = start {
            if forward {
                self.forward_step_sum_sweep(start, thread_pool, pl, message);
            } else {
                self.backwards_step_sum_sweep(start, thread_pool, pl, message);
            }
            self.iterations += 1;
        }
    }

    #[inline(always)]
    fn backwards_step_sum_sweep(
        &mut self,
        start: usize,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
        message: impl FnOnce(usize) -> String,
    ) {
        pl.item_name("node");
        pl.display_memory(false);
        pl.expected_updates(None);
        pl.start(message(start));

        let max_dist = AtomicUsize::new(0);
        let radius = RwLock::new((self.radius_high, self.radius_vertex));

        let forward_low = self.forward_low.as_sync_slice();
        let forward_tot = self.forward_tot.as_sync_slice();

        self.transposed_visit
            .par_visit_with(
                [start],
                pl.clone(),
                |pl, event| {
                    if let EventNoPred::Unknown { node, distance, .. } = event {
                        pl.light_update();
                        // Safety for unsafe blocks: each node gets accessed exactly once, so no data races can happen
                        max_dist.fetch_max(distance, Ordering::Relaxed);

                        let node_forward_low = unsafe { forward_low[node].get() };
                        let node_forward_high = self.forward_high[node];

                        unsafe { forward_tot[node].set(forward_tot[node].get() + distance) };

                        if node_forward_low != node_forward_high && node_forward_low < distance {
                            unsafe { forward_low[node].set(distance) };

                            if distance == node_forward_high && self.radial_vertices[node] {
                                let mut update_radius = false;
                                {
                                    let radius_lock = radius.read().unwrap();
                                    if distance < radius_lock.0 {
                                        update_radius = true;
                                    }
                                }

                                if update_radius {
                                    let mut radius_lock = radius.write().unwrap();
                                    if distance < radius_lock.0 {
                                        radius_lock.0 = distance;
                                        radius_lock.1 = node;
                                    }
                                }
                            }
                        }
                    };
                    Continue(())
                },
                thread_pool,
            )
            .continue_value_no_break();

        self.transposed_visit.reset();

        let ecc_start = max_dist.load(Ordering::Relaxed);

        self.backward_low[start] = ecc_start;
        self.backward_high[start] = ecc_start;

        (self.radius_high, self.radius_vertex) = radius.into_inner().unwrap();

        if self.diameter_low < ecc_start {
            self.diameter_low = ecc_start;
            self.diameter_vertex = start;
        }

        pl.done();
    }

    #[inline(always)]
    fn forward_step_sum_sweep(
        &mut self,
        start: usize,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
        message: impl FnOnce(usize) -> String,
    ) {
        pl.item_name("node");
        pl.display_memory(false);
        pl.expected_updates(None);
        pl.start(message(start));

        let max_dist = AtomicUsize::new(0);

        let backward_low = self.backward_low.as_sync_slice();
        let backward_tot = self.backward_tot.as_sync_slice();

        self.visit.reset();
        self.visit
            .par_visit_with(
                [start],
                pl.clone(),
                |pl, event| {
                    if let EventNoPred::Unknown { node, distance, .. } = event {
                        // SAFETY: each node gets accessed exactly once, so no data races can happen
                        pl.light_update();
                        max_dist.fetch_max(distance, Ordering::Relaxed);

                        let node_backward_high = self.backward_high[node];
                        let node_backward_low = unsafe { backward_low[node].get() };

                        unsafe { backward_tot[node].set(backward_tot[node].get() + distance) };

                        if node_backward_low != node_backward_high && node_backward_low < distance {
                            unsafe { backward_low[node].set(distance) };
                        }
                    }
                    Continue(())
                },
                thread_pool,
            )
            .continue_value_no_break();

        let ecc_start = max_dist.load(Ordering::Relaxed);

        self.forward_low[start] = ecc_start;
        self.forward_high[start] = ecc_start;

        if self.diameter_low < ecc_start {
            self.diameter_low = ecc_start;
            self.diameter_vertex = start;
        }
        if self.radial_vertices[start] && self.radius_high > ecc_start {
            self.radius_high = ecc_start;
            self.radius_vertex = start;
        }

        pl.done();
    }

    /// Performs a (forward or backward) BFS inside each strongly connected component, starting
    /// from the pivot.
    ///
    /// For more information see Section 4.2 on the paper.
    ///
    /// # Arguments
    /// * `pivot`: An array containing in position `i` the pivot of the `i`-th strongly connected
    ///   component.
    /// * `forward`: Whether the BFS is performed following the direction of edges or
    ///   in the opposite direction.
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A progress logger.
    ///
    /// # Return
    /// Two arrays.
    ///
    /// The first one contains the distance of each vertex from the pivot of its strongly connected
    /// component, while the second one contains in position `i` the eccentricity of the pivot of the
    /// `i`-th strongly connected component.
    fn compute_dist_pivot(
        &self,
        pivot: &[usize],
        forward: bool,
        thread_pool: &ThreadPool,
        pl: &mut impl ProgressLog,
    ) -> (Vec<usize>, Vec<usize>) {
        pl.expected_updates(None);
        pl.display_memory(false);

        let (dist_pivot, usize_ecc_pivot) = if forward {
            pl.start("Computing forward dist pivots...");
            self.compute_dist_pivot_from_graph(pivot, self.graph, thread_pool)
        } else {
            pl.start("Computing backwards dist pivots...");
            self.compute_dist_pivot_from_graph(pivot, self.transpose, thread_pool)
        };

        pl.done();

        (dist_pivot, usize_ecc_pivot)
    }

    #[inline(always)]
    fn compute_dist_pivot_from_graph(
        &self,
        pivot: &[usize],
        graph: &(impl RandomAccessGraph + Sync),
        thread_pool: &ThreadPool,
    ) -> (Vec<usize>, Vec<usize>) {
        let components = self.scc.components();
        let mut ecc_pivot = Vec::with_capacity(self.scc.num_components());
        ecc_pivot.resize_with(self.scc.num_components(), || AtomicUsize::new(0));
        let mut dist_pivot = vec![0; self.num_nodes];
        let dist_pivot_mut = dist_pivot.as_sync_slice();
        let current_index = AtomicUsize::new(0);

        thread_pool.broadcast(|_| {
            let mut bfs = ParFairNoPred::new(graph);
            let mut current_pivot_index = current_index.fetch_add(1, Ordering::Relaxed);

            while let Some(&p) = pivot.get(current_pivot_index) {
                let pivot_component = components[p];
                let component_ecc_pivot = &ecc_pivot[pivot_component];

                bfs.par_visit_filtered(
                    [p],
                    |event| {
                        if let EventNoPred::Unknown { node, distance, .. } = event {
                            // Safety: each node is accessed exactly once
                            unsafe { dist_pivot_mut[node].set(distance) };
                            component_ecc_pivot.store(distance, Ordering::Relaxed);
                        };
                        Continue(())
                    },
                    |FilterArgs::<EventNoPred> { node, .. }| components[node] == pivot_component,
                    thread_pool,
                )
                .continue_value_no_break();

                current_pivot_index = current_index.fetch_add(1, Ordering::Relaxed);
            }
        });

        let usize_ecc_pivot = unsafe {
            let mut clone = std::mem::ManuallyDrop::new(ecc_pivot);
            Vec::from_raw_parts(
                clone.as_mut_ptr() as *mut usize,
                clone.len(),
                clone.capacity(),
            )
        };

        (dist_pivot, usize_ecc_pivot)
    }

    /// Performs a step of the ExactSumSweep algorithm.
    ///
    /// For more information see Section 4.2 of the paper.
    ///
    /// # Arguments
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A progress logger.
    fn all_cc_upper_bound(&mut self, thread_pool: &ThreadPool, pl: &mut impl ProgressLog) {
        pl.item_name("element");
        pl.display_memory(false);
        pl.expected_updates(Some(2 * self.scc.num_components() + self.num_nodes));
        pl.start("Performing the AllCCUpperBound step of the ExactSumSweep algorithm...");

        let pivot = self.find_best_pivot(pl);

        let (dist_pivot_f, mut ecc_pivot_f) =
            self.compute_dist_pivot(&pivot, true, thread_pool, pl);
        let (dist_pivot_b, mut ecc_pivot_b) =
            self.compute_dist_pivot(&pivot, false, thread_pool, pl);
        let components = self.scc.components();

        // Tarjan's algorithm emits components in reverse topological order.
        // In order to bound forward eccentricities in reverse topological order the components
        // are traversed as is.
        pl.info(format_args!("Bounding forward eccentricities of pivots..."));
        for (c, &p) in pivot.iter().enumerate() {
            for connection in self.scc_graph.successors(c) {
                let next_c = connection.target;
                let start = connection.start;
                let end = connection.end;

                ecc_pivot_f[c] = std::cmp::max(
                    ecc_pivot_f[c],
                    dist_pivot_f[start] + 1 + dist_pivot_b[end] + ecc_pivot_f[next_c],
                );

                if ecc_pivot_f[c] >= self.forward_high[p] {
                    ecc_pivot_f[c] = self.forward_high[p];
                    break;
                }
            }
            pl.light_update();
        }

        // Tarjan's algorithm emits components in reverse topological order.
        // In order to bound backward eccentricities in topological order the components order
        // must be reversed.
        pl.info(format_args!(
            "Bounding backward eccentricities of pivots..."
        ));
        for c in (0..self.scc.num_components()).rev() {
            for component in self.scc_graph.successors(c) {
                let next_c = component.target;
                let start = component.start;
                let end = component.end;

                ecc_pivot_b[next_c] = std::cmp::max(
                    ecc_pivot_b[next_c],
                    dist_pivot_f[start] + 1 + dist_pivot_b[end] + ecc_pivot_b[c],
                );

                if ecc_pivot_b[next_c] >= self.backward_high[pivot[next_c]] {
                    ecc_pivot_b[next_c] = self.backward_high[pivot[next_c]];
                }
            }
            pl.light_update();
        }

        let radius = RwLock::new((self.radius_high, self.radius_vertex));

        let forward_high = self.forward_high.as_sync_slice();
        let backward_high = self.backward_high.as_sync_slice();

        pl.info(format_args!("Refining upper bounds of nodes..."));
        thread_pool.install(|| {
            (0..self.num_nodes).into_par_iter().for_each(|node| {
                // Safety for unsafe blocks: each node gets accessed exactly
                // once, so no data races can happen

                let mut node_forward_high = unsafe { forward_high[node].get() };
                let pivot_value = dist_pivot_b[node] + ecc_pivot_f[components[node]];

                if pivot_value < node_forward_high {
                    unsafe { forward_high[node].set(pivot_value) };
                    node_forward_high = pivot_value;
                }

                if node_forward_high == self.forward_low[node] {
                    let new_ecc = node_forward_high;

                    if self.radial_vertices[node] {
                        let mut update_radius = false;
                        {
                            let radius_lock = radius.read().unwrap();
                            if new_ecc < radius_lock.0 {
                                update_radius = true;
                            }
                        }

                        if update_radius {
                            let mut radius_lock = radius.write().unwrap();
                            if new_ecc < radius_lock.0 {
                                radius_lock.0 = new_ecc;
                                radius_lock.1 = node;
                            }
                        }
                    }
                }

                unsafe {
                    backward_high[node].set(std::cmp::min(
                        backward_high[node].get(),
                        dist_pivot_f[node] + ecc_pivot_b[components[node]],
                    ))
                };
            });
        });

        pl.update_with_count(self.num_nodes);

        (self.radius_high, self.radius_vertex) = radius.into_inner().unwrap();

        self.iterations += 3;

        pl.done();
    }

    /// Computes how many nodes are still to be processed, before outputting the result.
    ///
    /// # Arguments
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A progress logger.
    fn find_missing_nodes(&mut self, thread_pool: &ThreadPool, pl: &mut impl ProgressLog) -> usize {
        pl.item_name("node");
        pl.display_memory(false);
        pl.expected_updates(Some(self.num_nodes));
        pl.start("Computing missing nodes...");

        let missing = thread_pool.install(|| {
            (0..self.num_nodes)
                .into_par_iter() // TODO: with_min_len (also elsewhere)
                .fold(Default::default, |mut acc: Missing, node| {
                    if self.incomplete_forward(node) {
                        acc.all_forward += 1;
                        if self.forward_high[node] > self.diameter_low {
                            acc.diameter_forward += 1;
                        }
                        if self.radial_vertices[node] && self.forward_low[node] < self.radius_high {
                            acc.radius += 1;
                        }
                    }
                    if self.incomplete_backward(node) {
                        acc.all_backward += 1;
                        if self.backward_high[node] > self.diameter_low {
                            acc.diameter_backward += 1;
                        }
                    }
                    acc
                })
                .reduce(Default::default, |acc, elem| acc + elem)
        });

        pl.update_with_count(self.num_nodes);

        if missing.radius == 0 && self.radius_iterations.is_none() {
            self.radius_iterations = Some(self.iterations);
        }
        if (missing.diameter_forward == 0 || missing.diameter_backward == 0)
            && self.diameter_iterations.is_none()
        {
            self.diameter_iterations = Some(self.iterations);
        }
        if missing.all_forward == 0 && self.forward_iter.is_none() {
            self.forward_iter = Some(self.iterations);
        }
        if missing.all_forward == 0 && missing.all_backward == 0 {
            self.all_iter = Some(self.iterations);
        }

        pl.done();

        OL::missing_nodes(&missing)
    }
}
