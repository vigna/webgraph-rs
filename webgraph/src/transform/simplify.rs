/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::{
    arc_list_graph, no_selfloops_graph::NoSelfLoopsGraph, union_graph::UnionGraph,
};
use crate::labels::Left;
use crate::traits::{
    LenderIntoIter, NodeLabelsLender, SequentialGraph, SortedIterator, SortedLender, SplitLabeling,
};
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::sort_pairs::{KMergeIters, SortPairs};
use crate::utils::{CodecIter, DefaultBatchCodec, MemoryUsage, SortedPairIter, SplitIters};
use anyhow::{Context, Result};
use dsi_progress_logger::prelude::*;
use lender::*;
use tempfile::Builder;

use super::transpose;

/// Returns a simplified (i.e., undirected and loopless) version of the provided
/// sorted (both on nodes and successors) graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// This method exploits the fact that the input graph is already sorted,
/// sorting half the number of arcs of [`simplify`].
pub fn simplify_sorted<G: SequentialGraph>(
    graph: G,
    memory_usage: MemoryUsage,
) -> Result<
    NoSelfLoopsGraph<
        UnionGraph<
            G,
            Left<arc_list_graph::ArcListGraph<KMergeIters<CodecIter<DefaultBatchCodec>, ()>>>,
        >,
    >,
>
where
    for<'a> G::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'a, G::Lender<'b>>: SortedIterator,
{
    let transpose = transpose(&graph, memory_usage).context("Could not transpose the graph")?;
    Ok(NoSelfLoopsGraph(UnionGraph(graph, transpose)))
}

/// Returns a simplified (i.e., undirected and loopless) version of the provided
/// graph as a [sequential graph](crate::traits::SequentialGraph).
///
/// Note that if the graph is sorted (both on nodes and successors), it is
/// recommended to use [`simplify_sorted`].
///
/// For the meaning of the additional parameter, see [`SortPairs`].
pub fn simplify(
    graph: &impl SequentialGraph,
    memory_usage: MemoryUsage,
) -> Result<
    Left<
        arc_list_graph::ArcListGraph<
            impl Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync + 'static,
        >,
    >,
> {
    let dir = Builder::new().prefix("simplify_").tempdir()?;
    let mut sorted = SortPairs::new_dedup(memory_usage, dir.path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node")
        .expected_updates(Some(graph.num_nodes()));
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_![(src, succ) in graph.iter() {
        for dst in succ {
            if src != dst {
                sorted.push(src, dst)?;
                sorted.push(dst, src)?;
            }
        }
        pl.light_update();
    }];
    // merge the batches
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), sorted.iter()?);
    pl.done();

    Ok(Left(sorted))
}

/// Returns a [`SplitIters`] structure representing a simplified (i.e.,
/// undirected and loopless) version of the provided graph, computed in parallel.
///
/// The [`SplitIters`] structure can be easily converted into a vector of
/// lenders using the [`From`] trait, suitable for
/// [`BvCompConfig::par_comp_lenders`](crate::graphs::bvgraph::BvCompConfig::par_comp_lenders).
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install](rayon::ThreadPool::install) a custom pool if you want to customize
/// the parallelism.
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
pub fn simplify_split<'g, S>(
    graph: &'g S,
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
{
    let par_sort_iters = ParSortIters::new_dedup(graph.num_nodes())?.memory_usage(memory_usage);

    let pairs: Vec<_> = match cutpoints {
        Some(cp) => graph.split_iter_at(cp),
        None => {
            let parts = rayon::current_num_threads();
            graph.split_iter(parts)
        }
    }
    .into_iter()
    .map(|iter| {
        iter.into_pairs().flat_map(|(src, dst)| {
            // The two-element iterator is fully inlined by LLVM,
            // generating the same code as a hand-written loop.
            if src != dst {
                Some((src, dst)).into_iter().chain(Some((dst, src)))
            } else {
                None.into_iter().chain(None)
            }
        })
    })
    .collect();

    par_sort_iters.sort(pairs)
}
