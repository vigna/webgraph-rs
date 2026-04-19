/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::par_sorted_graph::SortedPairIter;
use crate::prelude::*;
use crate::utils::par_sort_iters::ParSortIters;
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
/// Note that if the graph is [splittable], [`map_split`] will be much faster.
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
///
/// [splittable]: SplitLabeling
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

    let par_sort = ParSortIters::new_dedup(num_nodes)?.memory_usage(memory_usage);

    let pairs = graph
        .iter()
        .into_pairs()
        .map(|(src, dst)| ((map.index_value(src), map.index_value(dst)), ()));

    Ok(ParSortedGraph(
        par_sort
            .sort_labeled_seq::<DefaultBatchCodec<true>, _>(DefaultBatchCodec::default(), pairs)?
            .into(),
    ))
}

/// Returns a [`ParSortedGraph`] representing the mapped graph starting from a
/// [splittable] graph, computed in parallel.
///
/// The map is not required to be bijective: multiple source nodes may map to the
/// same destination node. Duplicate arcs are removed.
///
/// The `num_nodes` parameter specifies the number of nodes of the resulting
/// graph: it must be strictly greater than every value in the map.
///
/// Note that if the graph is not [splittable] you must use [`map`], albeit it
/// will be slower.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
///
/// [splittable]: SplitLabeling
/// [install]: rayon::ThreadPool::install
pub fn map_split<'g, S, M>(
    graph: &'g S,
    map: &M,
    num_nodes: usize,
    memory_usage: MemoryUsage,
    cutpoints: Option<Vec<usize>>,
) -> Result<ParSortedGraph<SortedPairIter<true>>>
where
    S: SequentialGraph
        + for<'a> SplitLabeling<
            SplitLender<'g>: NodeLabelsLender<
                'a,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
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

    let mut par_sort_iters = ParSortIters::new_dedup(num_nodes)?.memory_usage(memory_usage);
    if let Some(num_arcs) = graph.num_arcs_hint() {
        par_sort_iters = par_sort_iters.expected_num_pairs(num_arcs as usize);
    }

    let pairs: Vec<_> = match cutpoints {
        Some(cp) => graph.split_iter_at(cp),
        None => {
            let parts = rayon::current_num_threads();
            graph.split_iter(parts)
        }
    }
    .into_iter()
    .map(|iter| {
        iter.into_pairs()
            .map(|(src, dst)| ((map.index_value(src), map.index_value(dst)), ()))
    })
    .collect();

    Ok(ParSortedGraph(
        par_sort_iters
            .sort_labeled::<DefaultBatchCodec<true>, _>(DefaultBatchCodec::default(), pairs)?
            .into(),
    ))
}
