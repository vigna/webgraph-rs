/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::prelude::proj::Left;
use crate::prelude::sort_pairs::KMergeIters;
use crate::prelude::{LabeledSequentialGraph, SequentialGraph, SortPairs};
use crate::traits::graph::UnitLabelGraph;
use crate::traits::{NodeLabelsLender, SplitLabeling};
use crate::utils::{
    BatchCodec, CodecIter, DefaultBatchCodec, MemoryUsage, ParSortIters, SplitIters,
};
use anyhow::Result;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use tempfile::Builder;

/// Returns the transpose of the provided labeled graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameters, see [`SortPairs`].
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
/// For the meaning of the additional parameter, see [`SortPairs`].
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

/// Returns a [`SplitIters`] structure representing the
/// transpose of the provided labeled splittable graph.
///
/// The [`SplitIters`] structure can easily converted into a vector of `(node,
/// lender)` pairs using [this `From`
/// implementation](crate::prelude::SplitIters#impl-From<SplitIters<IT>-for-Vec<(usize,+Iter<L,+I>)>).
///
/// For the meaning of the additional parameters, see [`SortPairs`].
pub fn transpose_labeled_split<
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
    C: BatchCodec + 'graph,
>(
    graph: &'graph G,
    memory_usage: MemoryUsage,
    batch_codec: C,
) -> Result<
    SplitIters<
        impl IntoIterator<Item = ((usize, usize), C::Label), IntoIter: Send + Sync> + use<'graph, G, C>,
    >,
>
where
    CodecIter<C>: Clone + Send + Sync,
{
    let par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    let parts = rayon::current_num_threads();

    let pairs: Vec<_> = graph
        .split_iter(parts)
        .into_iter()
        .map(|iter| iter.into_labeled_pairs().map(|((a, b), l)| ((b, a), l)))
        .collect();

    par_sort_iters.try_sort_labeled::<C, std::convert::Infallible, _>(batch_codec, pairs)
}

/// Returns a [`SplitIters`] structure representing the
/// transpose of the provided splittable graph.
///
/// The [`SplitIters`] structure can easily converted into a vector of `(node,
/// lender)` pairs using [this `From`
/// implementation](crate::prelude::SplitIters#impl-From<SplitIters<IT>-for-Vec<(usize,+LeftIterator<Iter<(),+Map<I,+fn((usize,+usize))+->+(usize,+usize,+())>)>).
///
/// For the meaning of the additional parameters, see [`SortPairs`].
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
    SplitIters<impl IntoIterator<Item = (usize, usize), IntoIter: Send + Sync> + use<'graph, G>>,
> {
    let par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    let parts = num_cpus::get();

    let pairs: Vec<_> = graph
        .split_iter(parts)
        .into_iter()
        .map(|iter| iter.into_pairs().map(|(src, dst)| ((dst, src), ())))
        .collect();

    let batch_codec = DefaultBatchCodec::default();
    let SplitIters { boundaries, iters } = par_sort_iters
        .try_sort_labeled::<DefaultBatchCodec, std::convert::Infallible, _>(batch_codec, pairs)?;

    Ok(SplitIters {
        boundaries,
        iters: Box::into_iter(iters)
            .map(|iter| iter.into_iter().map(|(pair, _)| pair))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    })
}
