/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::prelude::proj::Left;
use crate::prelude::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, SortPairs};
use crate::prelude::{BitDeserializer, BitSerializer, LabeledSequentialGraph, SequentialGraph};
use crate::traits::graph::UnitLabelGraph;
use crate::utils::MemoryUsage;
use anyhow::Result;
use dsi_bitstream::traits::NE;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use tempfile::Builder;

/// Returns the transpose of the provided labeled graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameters, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn transpose_labeled<
    S: BitSerializer<NE, BitWriter> + Clone,
    D: BitDeserializer<NE, BitReader> + Clone + 'static,
>(
    graph: &impl LabeledSequentialGraph<S::SerType>,
    batch_size: usize,
    serializer: S,
    deserializer: D,
) -> Result<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<D>, D::DeserType>>>
where
    S::SerType: Send + Sync + Copy,
    D::DeserType: Clone + Copy,
{
    let dir = Builder::new().prefix("transpose_").tempdir()?;
    let mut sorted = SortPairs::new_labeled(
        MemoryUsage::BatchSize(batch_size),
        dir.path(),
        serializer,
        deserializer,
    )?;

    let mut pl = progress_logger![
        item_name = "node",
        expected_updates = Some(graph.num_nodes()),
        display_memory = true
    ];
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_!( (src, succ) in graph.iter() {
        for (dst, l) in succ {
            sorted.push_labeled(dst, src, l)?;
        }
        pl.light_update();
    });
    // merge the batches
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), sorted.iter()?);
    pl.done();

    Ok(sorted)
}

/// Returns the transpose of the provided graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn transpose(
    graph: impl SequentialGraph,
    batch_size: usize,
) -> Result<Left<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<()>, ()>>>> {
    Ok(Left(transpose_labeled(
        &UnitLabelGraph(graph),
        batch_size,
        (),
        (),
    )?))
}
