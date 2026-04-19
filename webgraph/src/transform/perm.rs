/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::par_sorted_graph::{ParSortedGraph, SortedPairIter};
use crate::prelude::*;
use anyhow::{Result, ensure};
use lender::*;
use value_traits::slices::SliceByValue;

/// Returns a [`SortedGraph`] representing the permuted graph.
///
/// Note that if the graph is [splittable], [`permute_split`] will be much
/// faster.
///
/// The permutation is assumed to be bijective. For the meaning of the
/// additional parameter, see
/// [`SortedGraphConfig`](crate::graphs::sorted_graph::SortedGraphConfig).
///
/// [splittable]: SplitLabeling
pub fn permute<G: SequentialGraph, P: SliceByValue<Value = usize>>(
    graph: &G,
    perm: &P,
    memory_usage: MemoryUsage,
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
        .sort_pairs_seq(num_nodes, pgraph.iter().into_pairs())
}

/// Returns a [`SortedGraph`] representing the permuted graph starting from a
/// [splittable] graph, computed in parallel.
///
/// Note that if the graph is not [splittable] you must use [`permute`],
/// albeit it will be slower.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// The permutation is assumed to be bijective. For the meaning of the
/// additional parameter, see
/// [`SortedGraphConfig`](crate::graphs::sorted_graph::SortedGraphConfig).
///
/// [splittable]: SplitLabeling
/// [install]: rayon::ThreadPool::install
pub fn permute_split<S, P>(
    graph: &S,
    perm: &P,
    memory_usage: MemoryUsage,
) -> Result<ParSortedGraph<SortedPairIter>>
where
    S: SequentialGraph + SplitLabeling,
    P: SliceByValue<Value = usize> + Send + Sync + Clone,
    for<'a> S::Lender<'a>: Clone + ExactSizeLender + lender::FusedLender + Send + Sync,
    for<'a, 'b> LenderIntoIter<'b, S::Lender<'a>>: Send + Sync,
{
    ensure!(
        perm.len() == graph.num_nodes(),
        "The given permutation has {} values and thus it's incompatible with a graph with {} nodes.",
        perm.len(),
        graph.num_nodes(),
    );
    let pgraph = PermutedGraph::new(graph, perm);
    ParSortedGraph::config()
        .memory_usage(memory_usage)
        .par_sort(&pgraph)
}
