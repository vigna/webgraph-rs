/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::{
    arc_list_graph, no_selfloops_graph::NoSelfLoopsGraph, union_graph::UnionGraph,
};
use crate::labels::Left;
use crate::traits::{
    LenderIntoIter, RayonChannelIterExt, SequentialGraph, SortedIterator, SortedLender,
    SplitLabeling,
};
use crate::utils::sort_pairs::{KMergeIters, SortPairs};
use crate::utils::{CodecIter, DefaultBatchCodec, MemoryUsage};
use anyhow::{Context, Result};
use dsi_progress_logger::prelude::*;
use itertools::Itertools;
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
    let mut sorted = SortPairs::new(memory_usage, dir.path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node")
        .expected_updates(Some(graph.num_nodes()));
    pl.start("Creating batches...");
    // create batches of sorted edges
    let mut iter = graph.iter();
    while let Some((src, succ)) = iter.next() {
        for dst in succ {
            if src != dst {
                sorted.push(src, dst)?;
                sorted.push(dst, src)?;
            }
        }
        pl.light_update();
    }
    // merge the batches
    let map: fn(((usize, usize), ())) -> (usize, usize) = |(pair, _)| pair;
    let filter: fn(&(usize, usize)) -> bool = |(src, dst)| src != dst;
    let iter = Itertools::dedup(sorted.iter()?.map(map).filter(filter));
    let sorted = arc_list_graph::ArcListGraph::new(graph.num_nodes(), iter);
    pl.done();

    Ok(sorted)
}

/// Returns a simplified (i.e., undirected and loopless) version of the provided
/// graph as a [sequential graph](crate::traits::SequentialGraph).
///
/// This method uses splitting to sort in parallel different parts of the graph.
///
/// For the meaning of the additional parameter, see [`SortPairs`].
pub fn simplify_split<S>(
    graph: &S,
    memory_usage: MemoryUsage,
) -> Result<
    Left<
        arc_list_graph::ArcListGraph<
            itertools::Dedup<KMergeIters<CodecIter<DefaultBatchCodec>, ()>>,
        >,
    >,
>
where
    S: SequentialGraph + SplitLabeling,
{
    let num_threads = rayon::current_num_threads();
    let (tx, rx) = crossbeam_channel::unbounded();

    let mut dirs = vec![];

    rayon::in_place_scope(|scope| {
        let mut thread_id = 0;
        #[allow(clippy::explicit_counter_loop)] // enumerate requires some extra bounds here
        for iter in graph.split_iter(num_threads) {
            let tx = tx.clone();
            let dir = Builder::new()
                .prefix(&format!("simplify_split_{thread_id}_"))
                .tempdir()
                .expect("Could not create a temporary directory");
            let dir_path = dir.path().to_path_buf();
            dirs.push(dir);
            scope.spawn(move |_| {
                log::debug!("Spawned thread {thread_id}");
                let mut sorted = SortPairs::new(memory_usage, dir_path).unwrap();
                for_!( (src, succ) in iter {
                    for dst in succ {
                        if src != dst {
                            sorted.push(src, dst).unwrap();
                            sorted.push(dst, src).unwrap();
                        }
                    }
                });
                let result = sorted.iter().context("Could not read arcs").unwrap();
                tx.send(result).expect("Could not send the sorted pairs");
                log::debug!("Thread {thread_id} finished");
            });
            thread_id += 1;
        }
    });
    drop(tx);

    // get a graph on the sorted data
    log::debug!("Waiting for threads to finish");
    let edges: KMergeIters<CodecIter<DefaultBatchCodec>> = rx.into_rayon_iter().sum();
    let edges = edges.dedup();
    log::debug!("All threads finished");
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), edges);

    drop(dirs);
    Ok(Left(sorted))
}
