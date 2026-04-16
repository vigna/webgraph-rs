/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::labels::Left;
use crate::traits::{
    LenderIntoIter, NodeLabelsLender, SequentialGraph, SortedIterator, SortedLender, SplitLabeling,
};
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::sort_pairs::SortPairs;
use crate::utils::{MemoryUsage, SortedPairIter, SplitIters};
use anyhow::Result;
use dsi_progress_logger::prelude::*;
use lender::*;
use tempfile::Builder;

/// Merges two sorted iterators of node pairs, deduplicating consecutive
/// equal elements. When `NO_LOOPS` is true, self-loops (pairs where
/// source equals destination) are also removed.
#[derive(Clone)]
struct MergeDedupPairs<const NO_LOOPS: bool, I0, I1> {
    iter0: I0,
    iter1: I1,
    pending0: Option<(usize, usize)>,
    pending1: Option<(usize, usize)>,
    last: Option<(usize, usize)>,
}

impl<const NO_LOOPS: bool, I0, I1> MergeDedupPairs<NO_LOOPS, I0, I1>
where
    I0: Iterator<Item = (usize, usize)>,
    I1: Iterator<Item = (usize, usize)>,
{
    fn new(mut iter0: I0, mut iter1: I1) -> Self {
        Self {
            pending0: iter0.next(),
            pending1: iter1.next(),
            iter0,
            iter1,
            last: None,
        }
    }
}

impl<const NO_LOOPS: bool, I0, I1> Iterator for MergeDedupPairs<NO_LOOPS, I0, I1>
where
    I0: Iterator<Item = (usize, usize)>,
    I1: Iterator<Item = (usize, usize)>,
{
    type Item = (usize, usize);

    fn next(&mut self) -> Option<(usize, usize)> {
        loop {
            let pair = match (self.pending0, self.pending1) {
                (Some(a), Some(b)) if a <= b => {
                    self.pending0 = self.iter0.next();
                    a
                }
                (Some(_), Some(b)) => {
                    self.pending1 = self.iter1.next();
                    b
                }
                (Some(a), None) => {
                    self.pending0 = self.iter0.next();
                    a
                }
                (None, Some(b)) => {
                    self.pending1 = self.iter1.next();
                    b
                }
                (None, None) => return None,
            };
            if self.last == Some(pair) {
                continue;
            }
            self.last = Some(pair);
            if NO_LOOPS && pair.0 == pair.1 {
                continue;
            }
            return Some(pair);
        }
    }
}

/// Returns a symmetrized version of the provided sorted (both on nodes and
/// successors) graph as a [sequential graph].
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// This method exploits the fact that the input graph is already sorted: it
/// sorts only the reverse arcs (via [`SortPairs`]), then lazily merges them
/// with the forward arcs using a two-way merge with deduplication. The
/// forward arcs are iterated directly from the graph without any I/O.
///
/// For a parallel version using splitting, see [`symmetrize_sorted_split`].
///
/// [sequential graph]: crate::traits::SequentialGraph
pub fn symmetrize_sorted<'g, const NO_LOOPS: bool, G: SequentialGraph>(
    graph: &'g G,
    memory_usage: MemoryUsage,
) -> Result<
    Left<
        arc_list_graph::ArcListGraph<
            impl Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync + 'g,
        >,
    >,
>
where
    for<'a> G::Lender<'a>: Clone + Send + Sync + SortedLender,
    for<'a, 'b> LenderIntoIter<'a, G::Lender<'b>>: Clone + Send + Sync + SortedIterator,
{
    let num_nodes = graph.num_nodes();

    // Sort only reverse arcs
    let dir = Builder::new().prefix("symmetrize_sorted_").tempdir()?;
    let mut reverse = SortPairs::new(memory_usage, dir.path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node").expected_updates(Some(num_nodes));
    pl.start("Sorting reverse arcs...");

    for_![(src, succ) in graph.iter() {
        for dst in succ {
            reverse.push(dst, src)?;
        }
        pl.light_update();
    }];

    pl.done();

    // Forward arcs directly from the graph (sorted, no I/O)
    let forward = graph.iter().into_pairs();
    // Reverse arcs from SortPairs (sorted, backed by temp files)
    let reverse = reverse.iter()?.map(|((s, d), ())| (s, d));
    // Lazy merge with deduplication
    let merged = MergeDedupPairs::<NO_LOOPS, _, _>::new(forward, reverse);

    Ok(Left(arc_list_graph::ArcListGraph::new_labeled(
        num_nodes,
        merged.map(|p| (p, ())),
    )))
}

/// Returns a [`SplitIters`] structure representing a symmetrized version of
/// the provided sorted (both on nodes and successors) [splittable] graph,
/// computed in parallel.
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// This method exploits the fact that the input graph is already sorted: it
/// sorts only the reverse arcs (half the total) via [`ParSortIters`], then
/// splits the original graph at the same evenly spaced boundaries and lazily
/// merges forward and reverse pairs per partition with deduplication. This
/// makes it roughly twice as fast as [`symmetrize_split`] for the sorting
/// phase.
///
/// Note that since the output boundaries are determined by [`ParSortIters`]
/// (evenly spaced by node count), arc-balanced cutpoints (e.g., from a DCF)
/// cannot be used for the output partitions.
///
/// The [`SplitIters`] structure can be easily converted into a vector of
/// lenders using the [`From`] trait, suitable for
/// [`BvCompConfig::par_comp_lenders`].
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
///
/// [splittable]: crate::traits::SplitLabeling
/// [`BvCompConfig::par_comp_lenders`]: crate::graphs::bvgraph::BvCompConfig::par_comp_lenders
/// [install]: rayon::ThreadPool::install
pub fn symmetrize_sorted_split<'g, const NO_LOOPS: bool, S>(
    graph: &'g S,
    memory_usage: MemoryUsage,
    cutpoints: Option<Vec<usize>>,
) -> Result<SplitIters<impl Iterator<Item = (usize, usize)> + Clone + Send + Sync + 'g>>
where
    S: SequentialGraph + SplitLabeling,
    for<'a> S::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'a, S::Lender<'b>>: SortedIterator,
    for<'a> S::SplitLender<'g>:
        NodeLabelsLender<'a, IntoIterator: IntoIterator<IntoIter: Clone + Send + Sync>> + Clone,
{
    // Sort only the reverse arcs in parallel
    let mut par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    if let Some(num_arcs) = graph.num_arcs_hint() {
        par_sort_iters = par_sort_iters.expected_num_pairs(num_arcs as usize);
    }

    let reverse_pairs: Vec<_> = match cutpoints {
        Some(cp) => graph.split_iter_at(cp),
        None => {
            let parts = rayon::current_num_threads();
            graph.split_iter(parts)
        }
    }
    .into_iter()
    .map(|iter| iter.into_pairs().map(|(src, dst)| (dst, src)))
    .collect();

    let SplitIters { boundaries, iters } = par_sort_iters.sort(reverse_pairs)?;

    // Split the original graph at the same boundaries used by ParSortIters,
    // then lazily merge forward and reverse pairs per partition.
    let forward_lenders = graph.split_iter_at(boundaries.iter().copied());

    let merged: Vec<_> = forward_lenders
        .into_iter()
        .zip(iters.into_vec())
        .map(|(fwd, rev)| MergeDedupPairs::<NO_LOOPS, _, _>::new(fwd.into_pairs(), rev))
        .collect();

    Ok(SplitIters::new(boundaries, merged.into_boxed_slice()))
}

/// Returns a symmetrized version of the provided graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// Note that if the graph is sorted (both on nodes and successors), it is
/// recommended to use [`symmetrize_sorted`].
///
/// For the meaning of the additional parameter, see [`SortPairs`].
pub fn symmetrize<const NO_LOOPS: bool>(
    graph: &impl SequentialGraph,
    memory_usage: MemoryUsage,
) -> Result<
    Left<
        arc_list_graph::ArcListGraph<
            impl Iterator<Item = ((usize, usize), ())> + Clone + Send + Sync + 'static,
        >,
    >,
> {
    let dir = Builder::new().prefix("symmetrize_").tempdir()?;
    let mut sorted = SortPairs::new_dedup(memory_usage, dir.path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node")
        .expected_updates(Some(graph.num_nodes()));
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_![(src, succ) in graph.iter() {
        for dst in succ {
            if src != dst {
                sorted.push(src, dst)?;
                sorted.push(dst, src)?;
            } else if !NO_LOOPS {
                sorted.push(src, dst)?;
            }
        }
        pl.light_update();
    }];
    // merge the batches
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), sorted.iter()?);
    pl.done();

    Ok(Left(sorted))
}

/// Returns a [`SplitIters`] structure representing a symmetrized version of the
/// provided graph, computed in parallel.
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// The [`SplitIters`] structure can be easily converted into a vector of
/// lenders using the [`From`] trait, suitable for
/// [`BvCompConfig::par_comp_lenders`].
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
///
/// [`BvCompConfig::par_comp_lenders`]: crate::graphs::bvgraph::BvCompConfig::par_comp_lenders
/// [install]: rayon::ThreadPool::install
pub fn symmetrize_split<'g, const NO_LOOPS: bool, S>(
    graph: &'g S,
    memory_usage: MemoryUsage,
    cutpoints: Option<Vec<usize>>,
) -> Result<SplitIters<SortedPairIter<true>>>
where
    S: SequentialGraph
        + for<'a> SplitLabeling<
            SplitLender<'g>: NodeLabelsLender<
                'a,
                IntoIterator: IntoIterator<IntoIter: Send + Sync>,
            >,
        >,
{
    let mut par_sort_iters = ParSortIters::new_dedup(graph.num_nodes())?.memory_usage(memory_usage);
    if let Some(num_arcs) = graph.num_arcs_hint() {
        par_sort_iters = par_sort_iters.expected_num_pairs(2 * num_arcs as usize);
    }

    let pairs: Vec<_> = match cutpoints {
        Some(cp) => graph.split_iter_at(cp),
        None => {
            let parts = rayon::current_num_threads();
            graph.split_iter(parts)
        }
    }
    .into_iter()
    .map(|iter| {
        iter.into_pairs().flat_map(move |(src, dst)| {
            // The two-element iterator is fully inlined by LLVM,
            // generating the same code as a hand-written loop.
            if src != dst {
                Some((src, dst)).into_iter().chain(Some((dst, src)))
            } else if !NO_LOOPS {
                Some((src, dst)).into_iter().chain(None)
            } else {
                None.into_iter().chain(None)
            }
        })
    })
    .collect();

    par_sort_iters.sort(pairs)
}
