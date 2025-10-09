/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::rc::Rc;

use crate::graphs::arc_list_graph;
use crate::labels::LeftIterator;
use crate::prelude::proj::Left;
use crate::prelude::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, SortPairs};
use crate::prelude::{BitDeserializer, BitSerializer, LabeledSequentialGraph, SequentialGraph};
use crate::traits::graph::UnitLabelGraph;
use crate::traits::{NodeLabelsLender, SplitLabeling, UnitLender};
use crate::utils::{MemoryUsage, ParSortGraph};
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

/// Returns the transpose of the provided labeled graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameters, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn par_transpose_labeled<
    'graph,
    G: 'graph
        + LabeledSequentialGraph<S::SerType>
        + for<'a> SplitLabeling<
            SplitLender<'a>: for<'b> NodeLabelsLender<
                'b,
                Label: crate::traits::Pair<Left = usize, Right = S::SerType> + Copy,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
            > + Send
                                 + Sync,
            IntoIterator<'a>: IntoIterator<IntoIter: Send + Sync>,
        >,
    S: BitSerializer<NE, BitWriter> + Clone + Send + Sync,
    D: BitDeserializer<NE, BitReader, DeserType: Clone + Send + Sync> + Clone + Send + Sync + 'static,
>(
    graph: &'graph G,
    _batch_size: usize,
    serializer: S,
    deserializer: D,
) -> Result<
    Vec<impl for<'a> NodeLabelsLender<'a, Label = (usize, D::DeserType)> + Send + Sync + 'graph>,
>
where
    S: 'graph,
    S::SerType: Send + Sync + Copy,
    D::DeserType: Clone + Copy,
{
    let par_sort_graph = ParSortGraph::new(graph.num_nodes())?;
    let parts = num_cpus::get();

    let (start_nodes, pairs): (Vec<usize>, Vec<_>) = graph
        .split_iter(parts)
        .into_iter()
        .map(|(start_node, iter)| (start_node, iter.into_labeled_pairs::<'graph>()))
        .unzip();

    par_sort_graph
        .try_sort_labeled::<S, D, std::convert::Infallible>(&serializer, deserializer, pairs)?
        .into_iter()
        .enumerate()
        .map(|(i, res)| {
            arc_list_graph::Iter::try_new_from(graph.num_nodes(), res.into_iter(), start_nodes[i])
        })
        .collect()
}

pub fn par_transpose<
    'graph,
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
>(
    graph: &'graph G,
    _batch_size: usize,
) -> Result<Vec<impl for<'a> NodeLabelsLender<'a, Label = usize> + Send + Sync>> {
    let num_nodes = graph.num_nodes();
    let par_sort_graph = ParSortGraph::new(num_nodes)?;
    let parts = num_cpus::get();

    let (start_nodes, pairs): (Vec<usize>, Vec<_>) = graph
        .split_iter(parts)
        .into_iter()
        .map(|(start_node, iter)| (start_node, UnitLender(iter).into_labeled_pairs::<'graph>()))
        .unzip();

    Ok(par_sort_graph
        .try_sort_labeled::<(), (), std::convert::Infallible>(&(), (), pairs)?
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
