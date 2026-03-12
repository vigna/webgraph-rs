/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::prelude::sort_pairs::KMergeIters;
use crate::prelude::*;
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::{SortedPairIter, SplitIters};
use anyhow::{Context, Result, ensure};
use dsi_progress_logger::prelude::*;
use lender::*;
use tempfile::Builder;
use value_traits::slices::SliceByValue;

/// Returns a [sequential](crate::traits::SequentialGraph) permuted graph.
///
/// Note that if the graph is [splittable](SplitLabeling),
/// [`permute_split`] will be much faster.
///
/// The permutation is assumed to be bijective. For the meaning of the
/// additional parameter, see [`SortPairs`].
pub fn permute(
    graph: &impl SequentialGraph,
    perm: &impl SliceByValue<Value = usize>,
    memory_usage: MemoryUsage,
) -> Result<Left<arc_list_graph::ArcListGraph<KMergeIters<CodecIter<DefaultBatchCodec>, ()>>>> {
    ensure!(
        perm.len() == graph.num_nodes(),
        "The given permutation has {} values and thus it's incompatible with a graph with {} nodes.",
        perm.len(),
        graph.num_nodes(),
    );
    let dir = Builder::new().prefix("permute_").tempdir()?;
    log::info!(
        "Creating a temporary directory for the sorted pairs: {}",
        dir.path().display()
    );

    // create a stream where to dump the sorted pairs
    let mut sorted = SortPairs::new(memory_usage, dir.path())?;

    // get a permuted view
    let pgraph = PermutedGraph { graph, perm };

    let mut pl = progress_logger![
        item_name = "node",
        expected_updates = Some(graph.num_nodes()),
        display_memory = true
    ];
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_!( (src, succ) in pgraph.iter() {
        for dst in succ {
            sorted.push(src, dst)?;
        }
        pl.light_update();
    });

    // get a graph on the sorted data
    let edges = sorted.iter().context("Could not read arcs")?;
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), edges);
    pl.done();

    Ok(Left(sorted))
}

/// Returns a [`SplitIters`] structure representing the permuted graph
/// starting from a [splittable](SplitLabeling) graph, computed in parallel.
///
/// The [`SplitIters`] structure can be easily converted into a vector of
/// lenders using the [`From`] trait, suitable for
/// [`BvCompConfig::par_comp_lenders`](crate::graphs::bvgraph::BvCompConfig::par_comp_lenders).
///
/// Note that if the graph is not [splittable](SplitLabeling) you must use
/// [`permute`], albeit it will be slower.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install](rayon::ThreadPool::install) a custom pool if you want to customize
/// the parallelism.
///
/// The permutation is assumed to be bijective. For the meaning of the
/// additional parameter, see [`ParSortIters`].
pub fn permute_split<'g, S, P>(
    graph: &'g S,
    perm: &P,
    memory_usage: MemoryUsage,
) -> Result<SplitIters<SortedPairIter>>
where
    S: SequentialGraph
        + for<'a> SplitLabeling<
            SplitLender<'g>: NodeLabelsLender<
                'a,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
            >,
        >,
    P: SliceByValue<Value = usize> + Send + Sync,
{
    ensure!(
        perm.len() == graph.num_nodes(),
        "The given permutation has {} values and thus it's incompatible with a graph with {} nodes.",
        perm.len(),
        graph.num_nodes(),
    );

    let par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    let parts = rayon::current_num_threads();

    let pairs: Vec<_> = graph
        .split_iter(parts)
        .into_iter()
        .map(|iter| {
            iter.into_pairs()
                .map(|(src, dst)| (perm.index_value(src), perm.index_value(dst)))
        })
        .collect();

    par_sort_iters.sort(pairs)
}
