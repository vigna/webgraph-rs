/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::labels::LeftIterator;
use crate::prelude::proj::Left;
use crate::prelude::sort_pairs::KMergeIters;
use crate::prelude::{LabeledSequentialGraph, SequentialGraph, SortPairs};
use crate::traits::graph::UnitLabelGraph;
use crate::traits::{NodeLabelsLender, SplitLabeling, UnitLender};
use crate::utils::{BatchCodec, CodecIter, DefaultBatchCodec, MemoryUsage, ParSortGraph};
use anyhow::Result;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use tempfile::Builder;

/// Returns the transpose of the provided labeled graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameters, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn transpose_labeled<C: BatchCodec>(
    graph: &impl LabeledSequentialGraph<C::Label>,
    memory_usage: MemoryUsage,
    batch_codec: C,
) -> Result<arc_list_graph::ArcListGraph<KMergeIters<CodecIter<C>, C::Label>>>
where
    C::Label: Clone + 'static,
    CodecIter<C>: Clone + Send + Sync,
{
    let dir = Builder::new().prefix("transpose_").tempdir()?;
    let mut sorted = SortPairs::new_labeled(memory_usage, dir.path(), batch_codec)?;

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
    memory_usage: MemoryUsage,
) -> Result<Left<arc_list_graph::ArcListGraph<KMergeIters<CodecIter<DefaultBatchCodec>, ()>>>> {
    Ok(Left(transpose_labeled(
        &UnitLabelGraph(graph),
        memory_usage,
        DefaultBatchCodec::default(),
    )?))
}

/// Returns the transpose of the provided labeled graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameters, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn par_transpose_labeled<
    'graph,
    G: 'graph
        + LabeledSequentialGraph<C::Label>
        + for<'a> SplitLabeling<
            SplitLender<'a>: for<'b> NodeLabelsLender<
                'b,
                Label: crate::traits::Pair<Left = usize, Right = C::Label> + Copy,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
            > + Send
                                 + Sync,
            IntoIterator<'a>: IntoIterator<IntoIter: Send + Sync>,
        >,
    C,
>(
    graph: &'graph G,
    memory_usage: MemoryUsage,
    batch_codec: C,
) -> Result<Vec<impl for<'a> NodeLabelsLender<'a, Label = (usize, C::Label)> + Send + Sync + 'graph>>
where
    C: BatchCodec + 'graph,
    CodecIter<C>: Clone + Send + Sync,
{
    let par_sort_graph = ParSortGraph::new(graph.num_nodes())?.memory_usage(memory_usage);
    let parts = num_cpus::get();

    let (start_nodes, pairs): (Vec<usize>, Vec<_>) = graph
        .split_iter(parts)
        .into_iter()
        .map(|(start_node, iter)| (start_node, iter.into_labeled_pairs()))
        .unzip();

    par_sort_graph
        .try_sort_labeled::<C, std::convert::Infallible>(batch_codec, pairs)?
        .into_iter()
        .enumerate()
        .map(|(i, res)| {
            arc_list_graph::Iter::try_new_from(graph.num_nodes(), res.into_iter(), start_nodes[i])
        })
        .collect()
}

pub fn par_transpose<'graph, G>(
    graph: &'graph G,
    memory_usage: MemoryUsage,
) -> Result<Vec<impl for<'a> NodeLabelsLender<'a, Label = usize> + Send + Sync>>
where
    // TODO check if 'graph is needed
    G: 'graph
        + SequentialGraph
        + for<'a> SplitLabeling<
            SplitLender<'a>: for<'b> NodeLabelsLender<
                'b,
                Label = usize,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
            > + Send
                                 + Sync,
            IntoIterator<'a>: IntoIterator<IntoIter: Send + Sync>,
        >,
{
    let num_nodes = graph.num_nodes();
    let par_sort_graph = ParSortGraph::new(num_nodes)?.memory_usage(memory_usage);
    let parts = num_cpus::get();

    let (start_nodes, pairs): (Vec<usize>, Vec<_>) = graph
        .split_iter(parts)
        .into_iter()
        .map(|(start_node, iter)| (start_node, UnitLender(iter).into_labeled_pairs()))
        .unzip();

    let batch_codec = DefaultBatchCodec::default();
    Ok(par_sort_graph
        .try_sort_labeled::<_, std::convert::Infallible>(batch_codec, pairs)?
        .into_iter()
        .enumerate()
        .map(|(i, res)| {
            LeftIterator(
                arc_list_graph::Iter::try_new_from(
                    graph.num_nodes(),
                    res.into_iter(),
                    start_nodes[i],
                )
                .unwrap()
                .take(*start_nodes.get(i + 1).unwrap_or(&num_nodes) - start_nodes[i]),
            )
        })
        .collect::<Vec<_>>())
}
