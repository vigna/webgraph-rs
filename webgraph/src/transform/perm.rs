/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use super::map::{check_map_len, sort_mapped_par, sort_mapped_seq};
use crate::graphs::par_sorted_graph::{ParSortedGraph, SortedPairIter};
use crate::prelude::*;
use crate::traits::{IntoParLenders, NodeLabelsLender};
use anyhow::Result;
use dsi_progress_logger::ProgressLog;
use value_traits::slices::SliceByValue;

/// Returns a [`ParSortedGraph`] representing the permuted graph.
///
/// Note that if the graph implements [`IntoParLenders`], [`permute_par`] will be
/// much faster.
///
/// The permutation is assumed to be bijective. For the meaning of the
/// additional parameter, see [`ParSortedGraphConf`].
///
/// [`ParSortedGraphConf`]: crate::graphs::par_sorted_graph::ParSortedGraphConf
pub fn permute_seq<G: SequentialGraph, P: SliceByValue<Value = usize>>(
    graph: &G,
    perm: &P,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
) -> Result<ParSortedGraph<SortedPairIter>> {
    check_map_len("permutation", perm.len(), graph.num_nodes())?;
    let conf = ParSortedGraph::config()
        .memory_usage(memory_usage)
        .progress_logger(pl);
    sort_mapped_seq(conf, graph, perm, graph.num_nodes())
}

/// Returns a [`ParSortedGraph`] representing the permuted graph, computed in
/// parallel.
///
/// The graph must implement [`IntoParLenders`]; use [`ParGraph`] to wrap a
/// graph as needed.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// The permutation is assumed to be bijective. For the meaning of the
/// additional parameter, see [`ParSortedGraphConf`].
///
/// [`ParGraph`]: crate::graphs::par_graphs::ParGraph
/// [install]: rayon::ThreadPool::install
/// [`ParSortedGraphConf`]: crate::graphs::par_sorted_graph::ParSortedGraphConf
pub fn permute_par<G, P>(
    graph: G,
    perm: &P,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
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
    P: SliceByValue<Value = usize> + Send + Sync,
{
    check_map_len("permutation", perm.len(), graph.num_nodes())?;
    let num_nodes = graph.num_nodes();
    let conf = ParSortedGraph::config()
        .memory_usage(memory_usage)
        .progress_logger(pl);
    sort_mapped_par(conf, graph, perm, num_nodes)
}
