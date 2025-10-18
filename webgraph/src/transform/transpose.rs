/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::prelude::proj::Left;
use crate::prelude::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters};
use crate::prelude::{
    BitDeserializer, BitSerializer, LabeledSequentialGraph, SequentialGraph, SortPairs,
};
use crate::traits::graph::UnitLabelGraph;
use crate::traits::{NodeLabelsLender, SplitLabeling, UnitLender};
use crate::utils::{MemoryUsage, ParSortIters, SplitIters};
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
    memory_usage: MemoryUsage,
    serializer: S,
    deserializer: D,
) -> Result<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<D>, D::DeserType>>>
where
    S::SerType: Send + Sync + Copy,
    D::DeserType: Clone + Copy,
{
    let dir = Builder::new().prefix("transpose_").tempdir()?;
    let mut sorted = SortPairs::new_labeled(memory_usage, dir.path(), serializer, deserializer)?;

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
) -> Result<Left<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<()>, ()>>>> {
    Ok(Left(transpose_labeled(
        &UnitLabelGraph(graph),
        memory_usage,
        (),
        (),
    )?))
}

/// Returns a [`SplitIters`] structure representing the
/// transpose of the provided labeled splittable graph.
///
/// The [`SplitIters`] structure can easily converted into a vector of `(node,
/// lender)` pairs using [this `From`
/// implementation](crate::prelude::SplitIters#impl-From<SplitIters<IT>-for-Vec<(usize,+Iter<L,+I>)>).
///
/// For the meaning of the additional parameters, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn transpose_labeled_split<
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
    S: BitSerializer<NE, BitWriter> + Clone + Send + Sync + 'graph,
    D: BitDeserializer<NE, BitReader, DeserType: Clone + Send + Sync> + Clone + Send + Sync + 'static,
>(
    graph: &'graph G,
    memory_usage: MemoryUsage,
    serializer: S,
    deserializer: D,
) -> Result<
    SplitIters<
        impl IntoIterator<Item = ((usize, usize), D::DeserType), IntoIter: Send + Sync>
            + use<'graph, G, S, D>,
    >,
>
where
    S::SerType: Send + Sync + Copy,
    D::DeserType: Clone + Copy,
{
    let par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    let parts = num_cpus::get();

    let pairs: Vec<_> = graph
        .split_iter(parts)
        .into_iter()
        .map(|(_start_node, iter)| iter.into_labeled_pairs())
        .collect();

    par_sort_iters.try_sort_labeled::<S, D, std::convert::Infallible>(
        &serializer,
        deserializer,
        pairs,
    )
}

/// Returns a [`SplitIters`] structure representing the
/// transpose of the provided splittable graph.
///
/// The [`SplitIters`] structure can easily converted into a vector of `(node,
/// lender)` pairs using [this `From`
/// implementation](crate::prelude::SplitIters#impl-From<SplitIters<IT>-for-Vec<(usize,+LeftIterator<Iter<(),+Map<I,+fn((usize,+usize))+->+(usize,+usize,+())>)>).
///
/// For the meaning of the additional parameters, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
pub fn transpose_split<
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
    memory_usage: MemoryUsage,
) -> Result<
    SplitIters<
        impl IntoIterator<Item = ((usize, usize), ()), IntoIter: Send + Sync> + use<'graph, G>,
    >,
> {
    let par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    let parts = num_cpus::get();

    let pairs: Vec<_> = graph
        .split_iter(parts)
        .into_iter()
        .map(|(_start_node, iter)| UnitLender(iter).into_labeled_pairs())
        .collect();

    par_sort_iters.try_sort_labeled::<(), (), std::convert::Infallible>(&(), (), pairs)
}
