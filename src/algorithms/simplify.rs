/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graph::arc_list_graph;
use crate::prelude::*;
//use crate::traits::graph::Adapter;
use crate::traits::SequentialGraph;
use crate::utils::{BatchIterator, KMergeIters, SortPairs};
use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
use itertools::{Dedup, Itertools};
/// Make the graph undirected and remove selfloops
#[allow(clippy::type_complexity)]
pub fn simplify(
    graph: &impl SequentialGraph,
    batch_size: usize,
) -> Result<
    arc_list_graph::ArcListGraph<
        Dedup<
            core::iter::Filter<
                core::iter::Map<
                    KMergeIters<BatchIterator>,
                    fn((usize, usize, ())) -> (usize, usize),
                >,
                fn(&(usize, usize)) -> bool,
            >,
        >,
    >,
> {
    let dir = tempfile::tempdir()?;
    let mut sorted = SortPairs::new(batch_size, dir.into_path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name = "node";
    pl.expected_updates = Some(graph.num_nodes());
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

    Ok(sorted)
}
