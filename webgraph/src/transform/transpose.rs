/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::par_sorted_graph::{
    LabeledCodec, ParSortedGraph, ParSortedLabeledGraph, ParSortedLabeledGraphConf,
    SortedLabeledIter, SortedPairIter,
};
use crate::prelude::sort_pairs::KMergeIters;
use crate::prelude::{LabeledSequentialGraph, SequentialGraph};
use crate::traits::{BitDeserializer, BitSerializer, NodeLabelsLender, SplitLabeling};
use crate::utils::{
    BitReader, BitWriter, CodecIter, DefaultBatchCodec, MemoryUsage, ParSortIters, SplitIters,
};
use anyhow::Result;
use dsi_bitstream::prelude::NE;

/// Returns the transpose of the provided labeled graph as a
/// [`ParSortedLabeledGraph`].
///
/// For the meaning of the additional parameters, see
/// [`ParSortedLabeledGraphConf`](crate::graphs::par_sorted_graph::ParSortedLabeledGraphConf).
pub fn transpose_labeled<SD>(
    graph: &impl LabeledSequentialGraph<SD::SerType>,
    memory_usage: MemoryUsage,
    sd: SD,
) -> Result<ParSortedLabeledGraph<SD::SerType, SortedLabeledIter<SD>>>
where
    SD: BitSerializer<NE, BitWriter<NE>>
        + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
        + Send
        + Sync
        + Clone,
    SD::SerType: Clone + Copy + Send + Sync + 'static,
{
    ParSortedLabeledGraphConf::new()
        .memory_usage(memory_usage)
        .sort_pairs_seq(
            graph.num_nodes(),
            sd,
            graph
                .iter()
                .into_labeled_pairs()
                .map(|((src, dst), l)| ((dst, src), l)),
        )
}

/// Returns the transpose of the provided graph as a [`ParSortedGraph`].
///
/// For the meaning of the additional parameter, see
/// [`ParSortedGraphConf`](crate::graphs::par_sorted_graph::ParSortedGraphConf).
pub fn transpose(
    graph: impl SequentialGraph,
    memory_usage: MemoryUsage,
) -> Result<ParSortedGraph<SortedPairIter>> {
    ParSortedGraph::config()
        .memory_usage(memory_usage)
        .sort_pairs_seq(
            graph.num_nodes(),
            graph.iter().into_pairs().map(|(src, dst)| (dst, src)),
        )
}

/// Returns a [`SplitIters`] structure representing the transpose of the
/// provided labeled splittable graph, computed in parallel.
///
/// For graph compression, the result can be converted into a
/// [`ParSortedLabeledGraph`](crate::graphs::par_sorted_graph::ParSortedLabeledGraph) by calling [`.into()`](Into::into).
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameters, see
/// [`ParSortedLabeledGraphConf`](crate::graphs::par_sorted_graph::ParSortedLabeledGraphConf).
///
/// [install]: rayon::ThreadPool::install
pub fn transpose_labeled_split<SD, G>(
    graph: &G,
    memory_usage: MemoryUsage,
    sd: SD,
    cutpoints: Option<Vec<usize>>,
) -> Result<SplitIters<SortedLabeledIter<SD>>>
where
    SD: BitSerializer<NE, BitWriter<NE>>
        + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
        + Send
        + Sync
        + Clone,
    SD::SerType: Clone + Copy + Send + Sync + 'static,
    G: LabeledSequentialGraph<SD::SerType>
        + for<'a> SplitLabeling<
            SplitLender<'a>: for<'b> NodeLabelsLender<
                'b,
                Label: crate::traits::Pair<Left = usize, Right = SD::SerType> + Copy,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
            > + Send
                                 + Sync,
            IntoIterator<'a>: IntoIterator<IntoIter: Send + Sync>,
        >,
{
    let mut par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    if let Some(num_arcs) = graph.num_arcs_hint() {
        par_sort_iters = par_sort_iters.expected_num_pairs(num_arcs as usize);
    }

    let pairs: Vec<_> = match cutpoints {
        Some(cp) => graph.split_iter_at(cp),
        None => {
            let parts = rayon::current_num_threads();
            graph.split_iter(parts)
        }
    }
    .into_iter()
    .map(|iter| iter.into_labeled_pairs().map(|((a, b), l)| ((b, a), l)))
    .collect();

    let codec = LabeledCodec::new(sd);
    par_sort_iters.try_sort_labeled::<LabeledCodec<SD>, std::convert::Infallible, _>(codec, pairs)
}

/// Returns a [`ParSortedGraph`] representing the transpose of the provided
/// splittable graph, computed in parallel.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameters, see
/// [`ParSortedGraphConf`](crate::graphs::par_sorted_graph::ParSortedGraphConf).
///
/// [install]: rayon::ThreadPool::install
pub fn transpose_split<
    'g,
    G: SequentialGraph
        + for<'a> SplitLabeling<
            SplitLender<'g>: NodeLabelsLender<
                'a,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
            >,
        >,
>(
    graph: &'g G,
    memory_usage: MemoryUsage,
    cutpoints: Option<Vec<usize>>,
) -> Result<ParSortedGraph<KMergeIters<CodecIter<DefaultBatchCodec>>>> {
    let mut par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    if let Some(num_arcs) = graph.num_arcs_hint() {
        par_sort_iters = par_sort_iters.expected_num_pairs(num_arcs as usize);
    }

    let pairs: Vec<_> = match cutpoints {
        Some(cp) => graph.split_iter_at(cp),
        None => {
            let parts = rayon::current_num_threads();
            graph.split_iter(parts)
        }
    }
    .into_iter()
    .map(|iter| iter.into_pairs().map(|(src, dst)| ((dst, src), ())))
    .collect();

    Ok(ParSortedGraph(
        par_sort_iters
            .sort_labeled::<DefaultBatchCodec, _>(DefaultBatchCodec::default(), pairs)?
            .into(),
    ))
}
