/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::{arc_list_graph, UnionGraph};
use crate::labels::Left;
use crate::traits::{SequentialGraph, SplitLabeling};
use crate::utils::sort_pairs::{BatchIterator, KMergeIters, SortPairs};
use anyhow::{Context, Result};
use dsi_progress_logger::prelude::*;
use itertools::{Dedup, Itertools};
use lender::*;
use tempfile::Builder;

use super::transpose;

/// Returns a simplified (i.e., undirected and loopless) version of the provided
/// graph as a [sequential graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn simplify_sorted<G: SequentialGraph>(
    graph: &G,
    batch_size: usize,
) -> Result<UnionGraph<&G, Left<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<()>, ()>>>>>
{
    Ok(UnionGraph(
        graph,
        transpose(graph, batch_size).context("Could not transpose the graph")?,
    ))
}

/// Returns a simplified (i.e., undirected and loopless) version of the provided
/// graph as a [sequential graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn simplify(
    graph: &impl SequentialGraph,
    batch_size: usize,
) -> Result<
    Left<
        arc_list_graph::ArcListGraph<
            std::iter::Map<
                Dedup<
                    core::iter::Filter<
                        core::iter::Map<
                            KMergeIters<BatchIterator<()>>,
                            fn((usize, usize, ())) -> (usize, usize),
                        >,
                        fn(&(usize, usize)) -> bool,
                    >,
                >,
                fn((usize, usize)) -> (usize, usize, ()),
            >,
        >,
    >,
> {
    let dir = Builder::new().prefix("simplify-").tempdir()?;
    let mut sorted = SortPairs::new(batch_size, dir.path())?;

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
    let map: fn((usize, usize, ())) -> (usize, usize) = |(src, dst, _)| (src, dst);
    let filter: fn(&(usize, usize)) -> bool = |(src, dst)| src != dst;
    let iter = Itertools::dedup(sorted.iter()?.map(map).filter(filter));
    let sorted = arc_list_graph::ArcListGraph::new(graph.num_nodes(), iter);
    pl.done();

    Ok(Left(sorted))
}

/// Returns a simplified (i.e., undirected and loopless) version of the provided
/// graph as a [sequential graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn simplify_split<S>(
    graph: &S,
    batch_size: usize,
    mut threads: impl AsMut<rayon::ThreadPool>,
) -> Result<Left<arc_list_graph::ArcListGraph<itertools::Dedup<KMergeIters<BatchIterator<()>, ()>>>>>
where
    S: SequentialGraph + SplitLabeling,
{
    let pool = threads.as_mut();
    let num_threads = pool.current_num_threads();
    let (tx, rx) = std::sync::mpsc::channel();

    let mut dirs = vec![];

    pool.in_place_scope(|scope| {
        let mut thread_id = 0;
        #[allow(clippy::explicit_counter_loop)] // enumerate requires some extra bounds here
        for iter in graph.split_iter(num_threads) {
            let tx = tx.clone();
            let dir = Builder::new()
                .prefix(&format!("Simplify{}", thread_id))
                .tempdir()
                .expect("Could not create a temporary directory");
            let dir_path = dir.path().to_path_buf();
            dirs.push(dir);
            scope.spawn(move |_| {
                log::debug!("Spawned thread {}", thread_id);
                let mut sorted = SortPairs::new(batch_size / num_threads, dir_path).unwrap();
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
                log::debug!("Thread {} finished", thread_id);
            });
            thread_id += 1;
        }
    });
    drop(tx);

    // get a graph on the sorted data
    log::debug!("Waiting for threads to finish");
    let edges: KMergeIters<BatchIterator> = rx.iter().sum();
    let edges = edges.dedup();
    log::debug!("All threads finished");
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), edges);

    drop(dirs);
    Ok(Left(sorted))
}
