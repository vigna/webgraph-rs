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
pub struct SccGraphConnection {
    /// The component this connection is connected to
    pub target: usize,
    /// The start node of the connection
    pub start: usize,
    /// The end node of the connection
    pub end: usize,
}

pub struct SccGraph<G1: RandomAccessGraph, G2: RandomAccessGraph> {
    /// Slice of offsets where the `i`-th offset is how many elements to skip in [`Self::data`]
    /// in order to reach the first element relative to component `i`.
    segments_offset: Box<[usize]>,
    data: Box<[SccGraphConnection]>,
    _marker: PhantomData<(G1, G2)>,
}

#[inline(always)]
fn arc_value<G1: RandomAccessGraph, G2: RandomAccessGraph>(
    graph: &G1,
    reversed_graph: &G2,
    start: usize,
    end: usize,
) -> usize {
    let start_value = reversed_graph.outdegree(start);
    let end_value = graph.outdegree(end);
    start_value + end_value
}

impl<G: RandomAccessGraph> SccGraph<G, G> {
    /// Creates a strongly connected components graph from the provided undirected graph.
    ///
    /// # Arguments
    /// * `graph`: An immutable reference to the graph.
    /// * `scc`: An immutable reference to a [`StronglyConnectedComponents`] instance.
    pub fn new_symm(scc: &Sccs) -> Self {
        Self {
            segments_offset: vec![0; scc.num_components()].into_boxed_slice(),
            data: Vec::new().into_boxed_slice(),
            _marker: PhantomData,
        }
    }
}

impl<G1: RandomAccessGraph, G2: RandomAccessGraph> SccGraph<G1, G2> {
    /// Creates a strongly connected components graph from provided graphs and strongly connected
    /// components.
    ///
    /// # Arguments
    /// * `graph`: An immutable reference to the graph.
    /// * `reversed_graph`: An immutable reference to `graph` transposed.
    /// * `scc`: An immutable reference to a [`StronglyConnectedComponents`] instance.
    /// * `pl`: A progress logger.
    pub fn new_directed(
        graph: &G1,
        reversed_graph: &G2,
        scc: &Sccs,
        pl: &mut impl ProgressLog,
    ) -> Self {
        pl.display_memory(false);
        pl.expected_updates(None);
        pl.start("Computing strongly connected components graph...");

        let (vec_lengths, vec_connections) =
            Self::find_edges_through_scc(graph, reversed_graph, scc, pl);

        pl.done();

        Self {
            segments_offset: vec_lengths.into_boxed_slice(),
            data: vec_connections.into_boxed_slice(),
            _marker: PhantomData,
        }
    }

    /// The children of the passed strongly connected component.
    ///
    /// # Arguments
    /// * `component`: the component.
    ///
    /// # Panics
    /// Panics if a non-existent component index is passed.
    pub fn children(&self, component: usize) -> &[SccGraphConnection] {
        let offset = self.segments_offset[component];
        let &end = self
            .segments_offset
            .get(component + 1)
            .unwrap_or(&self.data.len());
        &self.data[offset..end]
    }

    /// For each edge in the DAG of strongly connected components, finds a corresponding edge
    /// in the graph. This edge is used in the [`Self::all_cc_upper_bound`] method.
    ///
    /// # Arguments
    /// * `pl`: A progress logger.
    fn find_edges_through_scc(
        graph: &G1,
        reversed_graph: &G2,
        scc: &Sccs,
        pl: &mut impl ProgressLog,
    ) -> (Vec<usize>, Vec<SccGraphConnection>) {
        assert!(
            graph.num_nodes() < usize::MAX,
            "Graph should have a number of nodes < usize::MAX"
        );

        pl.item_name("node");
        pl.display_memory(false);
        pl.expected_updates(Some(graph.num_nodes()));
        pl.start("Selecting arcs...");

        let number_of_scc = scc.num_components();
        let node_components = scc.components();
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

                            let current_value = arc_value(graph, reversed_graph, v, succ);
                            if current_value
                                > arc_value(
                                    graph,
                                    reversed_graph,
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
