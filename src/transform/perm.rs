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
use tempfile::Builder;

/// Returns a [sequential](crate::traits::SequentialGraph) permuted graph.
///
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn permute(
    graph: &impl SequentialGraph,
    perm: &[usize],
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
