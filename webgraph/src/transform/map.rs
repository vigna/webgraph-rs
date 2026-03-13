/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::prelude::sort_pairs::KMergeIters;
use crate::prelude::*;
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::{SortedPairIter, SplitIters};
use anyhow::{Result, ensure};
use dsi_progress_logger::prelude::*;
use lender::*;
use tempfile::Builder;
use value_traits::slices::SliceByValue;

/// Returns a [sequential](crate::traits::SequentialGraph) graph obtained by
/// mapping the nodes of the provided graph through the given map.
///
/// The map is not required to be bijective: multiple source nodes may map to the
/// same destination node. Duplicate arcs are removed.
///
/// The `num_nodes` parameter specifies the number of nodes of the resulting
/// graph: it must be strictly greater than every value in the map.
///
/// Note that if the graph is [splittable](SplitLabeling),
/// [`map_split`] will be much faster.
///
/// For the meaning of the additional parameter, see [`SortPairs`].
pub fn map(
    graph: &impl SequentialGraph,
    map: &impl SliceByValue<Value = usize>,
    num_nodes: usize,
    memory_usage: MemoryUsage,
) -> Result<
    Left<arc_list_graph::ArcListGraph<KMergeIters<CodecIter<DefaultBatchCodec<true>>, (), true>>>,
> {
    ensure!(
        map.len() == graph.num_nodes(),
        "The given map has {} values and thus it's incompatible with a graph with {} nodes.",
        map.len(),
        graph.num_nodes(),
    );
    let dir = Builder::new().prefix("map_").tempdir()?;
    log::info!(
        "Creating a temporary directory for the sorted pairs: {}",
        dir.path().display()
    );

    let mut sorted = SortPairs::new_dedup(memory_usage, dir.path())?;

    let mut pl = progress_logger![
        item_name = "node",
        expected_updates = Some(graph.num_nodes()),
        display_memory = true
    ];
    pl.start("Creating batches...");
    for_!( (src, succ) in graph.iter() {
        let mapped_src = map.index_value(src);
        for dst in succ {
            sorted.push(mapped_src, map.index_value(dst))?;
        }
        pl.light_update();
    });

    let edges = sorted.iter()?;
    let sorted = arc_list_graph::ArcListGraph::new_labeled(num_nodes, edges);
    pl.done();

    Ok(Left(sorted))
}

/// Returns a [`SplitIters`] structure representing the mapped graph
/// starting from a [splittable](SplitLabeling) graph, computed in parallel.
///
/// The [`SplitIters`] structure can be easily converted into a vector of
/// lenders using the [`From`] trait, suitable for
/// [`BvCompConfig::par_comp_lenders`](crate::graphs::bvgraph::BvCompConfig::par_comp_lenders).
///
/// The map is not required to be bijective: multiple source nodes may map to the
/// same destination node. Duplicate arcs are removed.
///
/// The `num_nodes` parameter specifies the number of nodes of the resulting
/// graph: it must be strictly greater than every value in the map.
///
/// Note that if the graph is not [splittable](SplitLabeling) you must use
/// [`map`], albeit it will be slower.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install](rayon::ThreadPool::install) a custom pool if you want to customize
/// the parallelism.
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
pub fn map_split<'g, S, M>(
    graph: &'g S,
    map: &M,
    num_nodes: usize,
    memory_usage: MemoryUsage,
    cutpoints: Option<Vec<usize>>,
) -> Result<SplitIters<SortedPairIter<true>>>
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
            .map(|(src, dst)| (map.index_value(src), map.index_value(dst)))
    })
    .collect();

    par_sort_iters.sort(pairs)
}
