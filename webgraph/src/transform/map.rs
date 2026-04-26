/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::par_sorted_graph::SortedPairIter;
use crate::prelude::*;
use crate::traits::{IntoParLenders, NodeLabelsLender};
use anyhow::{Result, ensure};
use value_traits::slices::SliceByValue;

/// Returns a [`ParSortedGraph`] obtained by mapping the nodes of the provided
/// graph through the given map.
///
/// The map is not required to be bijective: multiple source nodes may map to the
/// same destination node. Duplicate arcs are removed.
///
/// The `num_nodes` parameter specifies the number of nodes of the resulting
/// graph: it must be strictly greater than every value in the map.
///
/// Note that if the graph implements [`IntoParLenders`], [`map_split`] will be
/// much faster.
///
/// For the meaning of the additional parameter, see
/// [`ParSortedGraphConf`](crate::graphs::par_sorted_graph::ParSortedGraphConf).
pub fn map(
    graph: &impl SequentialGraph,
    map: &impl SliceByValue<Value = usize>,
    num_nodes: usize,
    memory_usage: MemoryUsage,
) -> Result<ParSortedGraph<SortedPairIter<true>>> {
    ensure!(
        map.len() == graph.num_nodes(),
        "The given map has {} values and thus it's incompatible with a graph with {} nodes.",
        map.len(),
        graph.num_nodes(),
    );

    ParSortedGraph::config()
        .dedup()
        .memory_usage(memory_usage)
        .sort_pairs(
            num_nodes,
            graph
                .iter()
                .into_pairs()
                .map(|(src, dst)| (map.index_value(src), map.index_value(dst))),
        )
}

/// Returns a [`ParSortedGraph`] representing the mapped graph, computed in
/// parallel.
///
/// The map is not required to be bijective: multiple source nodes may map to the
/// same destination node. Duplicate arcs are removed.
///
/// The `num_nodes` parameter specifies the number of nodes of the resulting
/// graph: it must be strictly greater than every value in the map.
///
/// The graph must implement [`IntoParLenders`]; use [`ParGraph`] to wrap a
/// [splittable] graph as needed.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameter, see
/// [`ParSortedGraphConf`](crate::graphs::par_sorted_graph::ParSortedGraphConf).
///
/// [`ParGraph`]: crate::graphs::par_graphs::ParGraph
/// [splittable]: SplitLabeling
/// [install]: rayon::ThreadPool::install
pub fn map_split<G, M>(
    graph: G,
    map: &M,
    num_nodes: usize,
    memory_usage: MemoryUsage,
) -> Result<ParSortedGraph<SortedPairIter<true>>>
where
    G: SequentialGraph
        + IntoParLenders<
            ParLender: for<'a> NodeLabelsLender<
                'a,
                Label = usize,
                IntoIterator: IntoIterator<IntoIter: Send>,
            >,
        >,
    M: SliceByValue<Value = usize> + Send + Sync,
{
    ensure!(
        map.len() == graph.num_nodes(),
        "The given map has {} values and thus it's incompatible with a graph with {} nodes.",
        map.len(),
        graph.num_nodes(),
    );

    let conf = ParSortedGraph::config().dedup().memory_usage(memory_usage);
    let (lenders, _boundaries) = graph.into_par_lenders();
    let iters = lenders.into_vec().into_iter().map(|lender| {
        lender
            .into_pairs()
            .map(|(src, dst)| (map.index_value(src), map.index_value(dst)))
    });
    conf.par_sort_pair_iters(num_nodes, iters)
}
