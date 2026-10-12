/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::graphs::par_sorted_graph::{ParSortedGraphConf, SortedPairIter};
use crate::prelude::*;
use crate::traits::{IntoParLenders, NodeLabelsLender};
use anyhow::{Result, ensure};
use dsi_progress_logger::ProgressLog;
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
/// Note that if the graph implements [`IntoParLenders`], [`map_par`] will be
/// much faster.
///
/// For the meaning of the additional parameter, see [`ParSortedGraphConf`].
pub fn map_seq(
    graph: &impl SequentialGraph,
    map: &impl SliceByValue<Value = usize>,
    num_nodes: usize,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
) -> Result<ParSortedGraph<SortedPairIter<true>>> {
    check_map_len("map", map.len(), graph.num_nodes())?;
    let conf = ParSortedGraph::config()
        .dedup()
        .memory_usage(memory_usage)
        .progress_logger(pl);
    sort_mapped_seq(conf, graph, map, num_nodes)
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
/// For the meaning of the additional parameter, see [`ParSortedGraphConf`].
///
/// [`ParGraph`]: crate::graphs::par_graphs::ParGraph
/// [splittable]: SplitLabeling
/// [install]: rayon::ThreadPool::install
pub fn map_par<G, M>(
    graph: G,
    map: &M,
    num_nodes: usize,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
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
    check_map_len("map", map.len(), graph.num_nodes())?;
    let conf = ParSortedGraph::config()
        .dedup()
        .memory_usage(memory_usage)
        .progress_logger(pl);
    sort_mapped_par(conf, graph, map, num_nodes)
}

/// Checks that a map (`what` describes its kind) contains a value for each
/// node of a graph.
pub(crate) fn check_map_len(what: &str, map_len: usize, num_nodes: usize) -> Result<()> {
    ensure!(
        map_len == num_nodes,
        "The given {what} has {map_len} values and thus it's incompatible with a graph with {num_nodes} nodes.",
    );
    Ok(())
}

/// Sorts the arcs of a graph after mapping their endpoints through a map,
/// using the given configuration.
pub(crate) fn sort_mapped_seq<PL: ProgressLog, const DEDUP: bool>(
    conf: ParSortedGraphConf<PL, DEDUP>,
    graph: &impl SequentialGraph,
    map: &impl SliceByValue<Value = usize>,
    num_nodes: usize,
) -> Result<ParSortedGraph<SortedPairIter<DEDUP>>> {
    conf.sort_pairs(
        num_nodes,
        graph
            .iter()
            .into_pairs()
            .map(|(src, dst)| (map.index_value(src), map.index_value(dst))),
    )
}

/// Sorts in parallel the arcs of a graph after mapping their endpoints
/// through a map, using the given configuration.
pub(crate) fn sort_mapped_par<G, M, PL: ProgressLog, const DEDUP: bool>(
    conf: ParSortedGraphConf<PL, DEDUP>,
    graph: G,
    map: &M,
    num_nodes: usize,
) -> Result<ParSortedGraph<SortedPairIter<DEDUP>>>
where
    G: IntoParLenders<
        ParLender: for<'a> NodeLabelsLender<
            'a,
            Label = usize,
            IntoIterator: IntoIterator<IntoIter: Send>,
        >,
    >,
    M: SliceByValue<Value = usize> + Send + Sync,
{
    let (lenders, _boundaries) = graph.into_par_lenders();
    let iters = lenders.into_vec().into_iter().map(|lender| {
        lender
            .into_pairs()
            .map(|(src, dst)| (map.index_value(src), map.index_value(dst)))
    });
    conf.par_sort_pair_iters(num_nodes, iters)
}
