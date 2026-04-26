/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::par_sorted_graph::{
    ParSortedGraph, ParSortedLabeledGraph, ParSortedLabeledGraphConf, SortedLabeledIter,
    SortedPairIter,
};
use crate::prelude::{LabeledSequentialGraph, SequentialGraph};
use crate::traits::{BitDeserializer, BitSerializer, IntoParLenders, NodeLabelsLender, Pair};
use crate::utils::{BitReader, BitWriter, MemoryUsage};
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
) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
where
    SD: BitSerializer<NE, BitWriter<NE>>
        + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
        + Send
        + Sync
        + Clone,
    SD::SerType: Clone + Copy + Send + Sync + 'static,
{
    ParSortedLabeledGraphConf::default()
        .memory_usage(memory_usage)
        .sort_pairs(
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
        .sort_pairs(
            graph.num_nodes(),
            graph.iter().into_pairs().map(|(src, dst)| (dst, src)),
        )
}

/// Returns the transpose of the provided labeled graph as a
/// [`ParSortedLabeledGraph`], computed in parallel.
///
/// The graph must implement [`IntoParLenders`]; use [`ParGraph`] to wrap a
/// [splittable] graph as needed.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameters, see
/// [`ParSortedLabeledGraphConf`](crate::graphs::par_sorted_graph::ParSortedLabeledGraphConf).
///
/// [`ParGraph`]: crate::graphs::par_graphs::ParGraph
/// [splittable]: crate::traits::SplitLabeling
/// [install]: rayon::ThreadPool::install
pub fn transpose_labeled_split<SD, G>(
    graph: G,
    memory_usage: MemoryUsage,
    sd: SD,
) -> Result<ParSortedLabeledGraph<SortedLabeledIter<SD>>>
where
    SD: BitSerializer<NE, BitWriter<NE>>
        + BitDeserializer<NE, BitReader<NE>, DeserType = SD::SerType>
        + Send
        + Sync
        + Clone,
    SD::SerType: Clone + Copy + Send + Sync + 'static,
    G: LabeledSequentialGraph<SD::SerType>
        + IntoParLenders<
            ParLender: for<'a> NodeLabelsLender<
                'a,
                Label: Pair<Left = usize, Right = SD::SerType> + Copy,
                IntoIterator: IntoIterator<IntoIter: Send>,
            >,
        >,
{
    let num_nodes = graph.num_nodes();
    let conf = ParSortedLabeledGraphConf::default().memory_usage(memory_usage);
    let (lenders, _boundaries) = graph.into_par_lenders();
    let iters = lenders
        .into_vec()
        .into_iter()
        .map(|lender| lender.into_labeled_pairs().map(|((a, b), l)| ((b, a), l)));
    conf.par_sort_pair_iters(num_nodes, sd, iters)
}

/// Returns the transpose of the provided graph as a [`ParSortedGraph`],
/// computed in parallel.
///
/// The graph must implement [`IntoParLenders`]; use [`ParGraph`] to wrap a
/// [splittable] graph as needed.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameters, see
/// [`ParSortedGraphConf`](crate::graphs::par_sorted_graph::ParSortedGraphConf).
///
/// [`ParGraph`]: crate::graphs::par_graphs::ParGraph
/// [splittable]: crate::traits::SplitLabeling
/// [install]: rayon::ThreadPool::install
pub fn transpose_split<G>(
    graph: G,
    memory_usage: MemoryUsage,
) -> Result<ParSortedGraph<SortedPairIter>>
where
    G: SequentialGraph
        + IntoParLenders<
            ParLender: for<'a> NodeLabelsLender<
                'a,
                Label = usize,
                IntoIterator: IntoIterator<IntoIter: Send>,
            >,
        >,
{
    let num_nodes = graph.num_nodes();
    let conf = ParSortedGraph::config().memory_usage(memory_usage);
    let (lenders, _boundaries) = graph.into_par_lenders();
    let iters = lenders
        .into_vec()
        .into_iter()
        .map(|lender| lender.into_pairs().map(|(src, dst)| (dst, src)));
    conf.par_sort_pair_iters(num_nodes, iters)
}
