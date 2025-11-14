/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::sccs::Sccs;
use dsi_progress_logger::ProgressLog;
use nonmax::NonMaxUsize;
use rayon::prelude::*;
use std::marker::PhantomData;
use webgraph::traits::RandomAccessGraph;

#[derive(Clone, Debug)]
pub(super) struct SccGraphConnection {
    /// The component this connection is connected to
    pub(super) target: usize,
    /// The start node of the connection
    pub(super) start: usize,
    /// The end node of the connection
    pub(super) end: usize,
}

pub(super) struct SccGraph<G1: RandomAccessGraph, G2: RandomAccessGraph> {
    /// Slice of offsets where the `i`-th offset is how many elements to skip in [`Self::data`]
    /// in order to reach the first element relative to component `i`.
    segments_offset: Box<[usize]>,
    data: Box<[SccGraphConnection]>,
    _marker: PhantomData<(G1, G2)>,
}

#[inline(always)]
fn arc_value<G1: RandomAccessGraph, G2: RandomAccessGraph>(
    graph: &G1,
    transpose: &G2,
    start: usize,
    end: usize,
) -> usize {
    let start_value = transpose.outdegree(start);
    let end_value = graph.outdegree(end);
    start_value + end_value
}

impl<G: RandomAccessGraph> SccGraph<G, G> {
    pub(super) fn new_symm(sccs: &Sccs) -> Self {
        Self {
            segments_offset: vec![0; sccs.num_components()].into_boxed_slice(),
            data: Vec::new().into_boxed_slice(),
            _marker: PhantomData,
        }
    }
}

impl<G1: RandomAccessGraph, G2: RandomAccessGraph> SccGraph<G1, G2> {
    pub(super) fn new(graph: &G1, transpose: &G2, scc: &Sccs, pl: &mut impl ProgressLog) -> Self {
        pl.display_memory(false);
        pl.expected_updates(None);
        pl.start("Computing strongly connected components graph...");

        let (vec_lengths, vec_connections) =
            Self::find_edges_through_scc(graph, transpose, scc, pl);

        pl.done();

        Self {
            segments_offset: vec_lengths.into_boxed_slice(),
            data: vec_connections.into_boxed_slice(),
            _marker: PhantomData,
        }
    }

    /// The successors of the specified strongly connected component.
    pub(super) fn successors(&self, component: usize) -> &[SccGraphConnection] {
        let offset = self.segments_offset[component];
        let &end = self
            .segments_offset
            .get(component + 1)
            .unwrap_or(&self.data.len());
        &self.data[offset..end]
    }

    /// For each edge in the DAG of strongly connected components, finds a
    /// corresponding edge in the graph. This edge is used in the
    /// `all_cc_upper_bound` method.
    fn find_edges_through_scc(
        graph: &G1,
        transpose: &G2,
        sccs: &Sccs,
        pl: &mut impl ProgressLog,
    ) -> (Vec<usize>, Vec<SccGraphConnection>) {
        pl.item_name("node");
        pl.display_memory(false);
        pl.expected_updates(Some(graph.num_nodes()));
        pl.start("Selecting arcs...");

        let number_of_scc = sccs.num_components();
        let node_components = sccs.components();
        let mut vertices_in_scc = vec![Vec::new(); number_of_scc];

        let mut scc_graph = vec![Vec::new(); number_of_scc];
        let mut start_bridges = vec![Vec::new(); number_of_scc];
        let mut end_bridges = vec![Vec::new(); number_of_scc];

        for (vertex, &component) in node_components.iter().enumerate() {
            vertices_in_scc[component].push(vertex);
        }

        {
            let mut child_components = Vec::new();
            let mut best_start = vec![None; number_of_scc];
            let mut best_end = vec![None; number_of_scc];

            for (c, component) in vertices_in_scc.into_iter().enumerate() {
                component.into_iter().for_each(|v| {
                    for succ in graph.successors(v) {
                        let succ_component = node_components[succ];
                        if c != succ_component {
                            if best_start[succ_component].is_none() {
                                best_end[succ_component] = NonMaxUsize::new(succ);
                                best_start[succ_component] = NonMaxUsize::new(v);
                                child_components.push(succ_component);
                            }

                            let current_value = arc_value(graph, transpose, v, succ);
                            if current_value
                                > arc_value(
                                    graph,
                                    transpose,
                                    best_start[succ_component].unwrap().into(),
                                    best_end[succ_component].unwrap().into(),
                                )
                            {
                                best_end[succ_component] = NonMaxUsize::new(succ);
                                best_start[succ_component] = NonMaxUsize::new(v);
                            }
                        }
                    }
                    pl.light_update();
                });

                let number_of_children = child_components.len();
                let mut scc_vec = Vec::with_capacity(number_of_children);
                let mut start_vec = Vec::with_capacity(number_of_children);
                let mut end_vec = Vec::with_capacity(number_of_children);
                for &child in child_components.iter() {
                    scc_vec.push(child);
                    start_vec.push(best_start[child].unwrap().into());
                    end_vec.push(best_end[child].unwrap().into());
                    best_start[child] = None;
                }
                scc_graph[c] = scc_vec;
                start_bridges[c] = start_vec;
                end_bridges[c] = end_vec;
                child_components.clear();
            }
        }

        pl.done();

        pl.item_name("connection");
        pl.expected_updates(Some(scc_graph.par_iter().map(|v| v.len()).sum()));
        pl.start("Creating connections...");

        let mut lengths = Vec::new();
        let mut connections = Vec::new();
        let mut offset = 0;

        for ((children, starts), ends) in scc_graph
            .into_iter()
            .zip(start_bridges.into_iter())
            .zip(end_bridges.into_iter())
        {
            lengths.push(offset);
            for ((child, start), end) in children
                .into_iter()
                .zip(starts.into_iter())
                .zip(ends.into_iter())
            {
                connections.push(SccGraphConnection {
                    target: child,
                    start,
                    end,
                });
                offset += 1;
                pl.light_update();
            }
        }

        pl.done();

        (lengths, connections)
    }
}
