/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::par_sorted_graph::{ParSortedGraph, SortedPairIter};
use crate::prelude::*;
use crate::traits::{IntoParLenders, NodeLabelsLender};
use anyhow::{Result, ensure};
use dsi_progress_logger::ProgressLog;
use value_traits::slices::SliceByValue;

/// Returns a [`ParSortedGraph`] representing the permuted graph.
///
/// Note that if the graph implements [`IntoParLenders`], [`permute_par`] will
/// be much faster.
///
/// The permutation is assumed to be bijective. For the meaning of the
/// additional parameter, see
/// [`ParSortedGraphConf`](crate::graphs::par_sorted_graph::ParSortedGraphConf).
pub fn permute<G: SequentialGraph, P: SliceByValue<Value = usize>>(
    graph: &G,
    perm: &P,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
) -> Result<ParSortedGraph<SortedPairIter>> {
    ensure!(
        perm.len() == graph.num_nodes(),
        "The given permutation has {} values and thus it's incompatible with a graph with {} nodes.",
        perm.len(),
        graph.num_nodes(),
    );
    let pgraph = PermutedGraph::new(graph, perm);
    let num_nodes = pgraph.num_nodes();
    ParSortedGraph::config()
        .memory_usage(memory_usage)
        .progress_logger(pl)
        .sort_pairs(num_nodes, pgraph.iter().into_pairs())
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
/// additional parameter, see
/// [`ParSortedGraphConf`](crate::graphs::par_sorted_graph::ParSortedGraphConf).
///
/// [`ParGraph`]: crate::graphs::par_graphs::ParGraph
/// [install]: rayon::ThreadPool::install
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
    ensure!(
        perm.len() == graph.num_nodes(),
        "The given permutation has {} values and thus it's incompatible with a graph with {} nodes.",
        perm.len(),
        graph.num_nodes(),
    );
    let num_nodes = graph.num_nodes();
    let conf = ParSortedGraph::config()
        .memory_usage(memory_usage)
        .progress_logger(pl);
    let (lenders, _boundaries) = graph.into_par_lenders();
    let iters = lenders.into_vec().into_iter().map(|lender| {
        lender
            .into_pairs()
            .map(|(src, dst)| (perm.index_value(src), perm.index_value(dst)))
    });
    conf.par_sort_pair_iters(num_nodes, iters)
}
