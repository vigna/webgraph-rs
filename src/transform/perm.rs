/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::prelude::sort_pairs::{BatchIterator, KMergeIters};
use crate::prelude::*;
use anyhow::{ensure, Context, Result};
use dsi_progress_logger::prelude::*;
use lender::*;
use sux::traits::BitFieldSlice;
use tempfile::Builder;

/// Returns a [sequential](crate::traits::SequentialGraph) permuted graph.
///
/// This assumes that the permutation is bijective.
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn permute(
    graph: &impl SequentialGraph,
    perm: &impl BitFieldSlice<usize>,
    batch_size: usize,
) -> Result<Left<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<()>, ()>>>> {
    ensure!(perm.len() == graph.num_nodes(),
        "The given permutation has {} values and thus it's incompatible with a graph with {} nodes.", 
        perm.len(), graph.num_nodes(),
    );
    let dir = Builder::new().prefix("Permute").tempdir()?;

    // create a stream where to dump the sorted pairs
    let mut sorted = SortPairs::new(batch_size, dir)?;

    // get a premuted view
    let pgraph = PermutedGraph { graph, perm };

    let mut pl = ProgressLogger::default();
    pl.item_name("node")
        .expected_updates(Some(graph.num_nodes()));
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_!( (src, succ) in pgraph.iter() {
        for dst in succ {
            sorted.push(dst, src)?;
        }
        pl.light_update();
    });

    // get a graph on the sorted data
    let edges = sorted.iter().context("Could not read arcs")?;
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), edges);
    pl.done();

    Ok(Left(sorted))
}

/// Returns a [sequential](crate::traits::SequentialGraph) permuted graph.
///
/// This assumes that the permutation is bijective.
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn permute_split<S, P>(
    graph: &S,
    perm: &P,
    batch_size: usize,
    mut threads: impl AsMut<rayon::ThreadPool>,
) -> Result<Left<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<()>, ()>>>>
where
    S: SequentialGraph + SplitLabeling,
    P: BitFieldSlice<usize> + Send + Sync + Clone,
    for<'a> <S as SequentialLabeling>::Lender<'a>: Send + Sync + Clone + ExactSizeLender,
{
    ensure!(perm.len() == graph.num_nodes(),
        "The given permutation has {} values and thus it's incompatible with a graph with {} nodes.", 
        perm.len(), graph.num_nodes(),
    );

    // get a premuted view
    let pgraph = PermutedGraph { graph, perm };

    let pool = threads.as_mut();
    let num_threads = pool.current_num_threads();
    let mut dirs = vec![];

    let edges = pool.in_place_scope(|scope| {
        let (tx, rx) = std::sync::mpsc::channel();

        for (thread_id, iter) in pgraph.split_iter(num_threads).enumerate() {
            let tx = tx.clone();
            let dir = Builder::new()
                .prefix(&format!("Permute_{}", thread_id))
                .tempdir()
                .expect("Could not create a temporary directory");
            let dir_path = dir.path().to_path_buf();
            dirs.push(dir);
            scope.spawn(move |_| {
                log::debug!("Spawned thread {}", thread_id);
                let mut sorted = SortPairs::new(batch_size / num_threads, dir_path).unwrap();
                for_!( (src, succ) in iter {
                    for dst in succ {
                        sorted.push(src, dst).unwrap();
                    }
                });
                tx.send(sorted.iter().context("Could not read arcs").unwrap())
                    .expect("Could not send the sorted pairs");
                log::debug!("Thread {} finished", thread_id);
            });
        }
        drop(tx);

        // get a graph on the sorted data
        log::debug!("Waiting for threads to finish");
        rx.iter().sum()
    });

    log::debug!("All threads finished");
    Ok(Left(arc_list_graph::ArcListGraph::new_labeled(
        graph.num_nodes(),
        edges,
    )))
}
