/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::graphs::par_sorted_graph::{ParSortedGraph, SortedPairIter};
use crate::traits::{
    IntoParLenders, LenderIntoIter, NodeLabelsLender, SequentialGraph, SortedIterator,
    SortedLender, SplitLabeling,
};
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::{MemoryUsage, SplitIters};
use anyhow::Result;
use dsi_progress_logger::ProgressLog;

/// Returns an iterator on an arc and on its reverse, skipping self-loops if
/// `NO_LOOPS` is true, and returning them once otherwise.
///
/// The two-element iterator is fully inlined by LLVM, generating the same code
/// as a hand-written loop.
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

#[inline(always)]
fn sym_arcs<const NO_LOOPS: bool>(
    (src, dst): (usize, usize),
) -> core::iter::Chain<core::option::IntoIter<(usize, usize)>, core::option::IntoIter<(usize, usize)>>
{
    if src != dst {
        Some((src, dst)).into_iter().chain(Some((dst, src)))
    } else if !NO_LOOPS {
        Some((src, dst)).into_iter().chain(None)
    } else {
        None.into_iter().chain(None)
    }
}

/// Re-splits a sorted graph at the boundaries of its sorted reverse arcs, and
/// lazily merges the forward and reverse arcs of each part, removing
/// duplicates and, if `NO_LOOPS` is true, self-loops.
fn merge_forward<'g, const NO_LOOPS: bool, G, R>(
    graph: &'g G,
    SplitIters { boundaries, iters }: SplitIters<R>,
) -> ParSortedGraph<impl Iterator<Item = ((usize, usize), ())> + Send + Sync + 'g>
where
    G: SequentialGraph + SplitLabeling,
    for<'a> G::SplitLender<'g>:
        NodeLabelsLender<'a, IntoIterator: IntoIterator<IntoIter: Clone + Send + Sync>> + Clone,
    R: Iterator<Item = (usize, usize)> + Send + Sync + 'g,
{
    let merged: Vec<_> = graph
        .split_iter_at(boundaries.iter().copied())
        .into_iter()
        .zip(iters.into_vec())
        // A hand-written merge is about 10% faster than itertools' merge,
        // dedup, and filter adapters
        .map(|(fwd, rev)| MergeDedupPairs::<NO_LOOPS, _, _>::new(fwd.into_pairs(), rev))
        .collect();
    ParSortedGraph::from_parts(boundaries, merged.into_boxed_slice())
}

/// Returns a [`ParSortedGraph`] representing a symmetrized version of the
/// provided sorted (both on nodes and successors) graph.
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// This method exploits the fact that the input graph is already sorted: it
/// sorts only the reverse arcs (half the total) via [`ParSortIters`], then
/// re-splits the original graph at the same evenly spaced boundaries and
/// lazily merges forward and reverse pairs per partition with
/// deduplication. This makes it roughly twice as fast as [`symmetrize_seq`]
/// for the sorting phase.
///
/// Note that since the output boundaries are determined by [`ParSortIters`]
/// (evenly spaced by node count), arc-balanced cutpoints (e.g., from a DCF)
/// cannot be used for the output partitions.
///
/// The graph must implement [`SplitLabeling`] so that it can be re-split at
/// the sort boundaries.
///
/// For a parallel version, see [`symmetrize_sorted_par`].
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
pub fn symmetrize_sorted_seq<'g, const NO_LOOPS: bool, G>(
    graph: &'g G,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
) -> Result<ParSortedGraph<impl Iterator<Item = ((usize, usize), ())> + Send + Sync + 'g>>
where
    G: SequentialGraph + SplitLabeling,
    for<'a> G::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'a, G::Lender<'b>>: SortedIterator,
    for<'a> G::SplitLender<'g>:
        NodeLabelsLender<'a, IntoIterator: IntoIterator<IntoIter: Clone + Send + Sync>> + Clone,
{
    let reverse = ParSortIters::new(graph.num_nodes())?
        .memory_usage(memory_usage)
        .sort_seq(graph.iter().into_pairs().map(|(src, dst)| (dst, src)), pl)?;
    Ok(merge_forward::<NO_LOOPS, _, _>(graph, reverse))
}

/// Returns a [`ParSortedGraph`] representing a symmetrized version of the
/// provided sorted (both on nodes and successors) graph, computed in
/// parallel.
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// This method exploits the fact that the input graph is already sorted: it
/// sorts only the reverse arcs (half the total) via [`ParSortIters`], then
/// re-splits the original graph at the same evenly spaced boundaries and
/// lazily merges forward and reverse pairs per partition with
/// deduplication. This makes it roughly twice as fast as [`symmetrize_par`]
/// for the sorting phase.
///
/// Note that since the output boundaries are determined by [`ParSortIters`]
/// (evenly spaced by node count), arc-balanced cutpoints (e.g., from a DCF)
/// cannot be used for the output partitions.
///
/// The graph must implement [`SplitLabeling`] so that it can be re-split at
/// the sort boundaries.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameter, see [`ParSortIters`].
///
/// [install]: rayon::ThreadPool::install
pub fn symmetrize_sorted_par<'g, const NO_LOOPS: bool, G>(
    graph: &'g G,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
) -> Result<ParSortedGraph<impl Iterator<Item = ((usize, usize), ())> + Send + Sync + 'g>>
where
    G: SequentialGraph + SplitLabeling,
    &'g G: IntoParLenders<
        ParLender: for<'a> NodeLabelsLender<
            'a,
            Label = usize,
            IntoIterator: IntoIterator<IntoIter: Clone + Send + Sync>,
        > + Clone,
    >,
    for<'a> G::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'a, G::Lender<'b>>: SortedIterator,
    for<'a> G::SplitLender<'g>:
        NodeLabelsLender<'a, IntoIterator: IntoIterator<IntoIter: Clone + Send + Sync>> + Clone,
{
    let par_sort_iters = ParSortIters::new(graph.num_nodes())?.memory_usage(memory_usage);
    let (lenders, _boundaries) = graph.into_par_lenders();
    let reverse_pairs: Vec<_> = lenders
        .into_vec()
        .into_iter()
        .map(|lender| lender.into_pairs().map(|(src, dst)| (dst, src)))
        .collect();
    let reverse = par_sort_iters.sort(reverse_pairs, pl)?;
    Ok(merge_forward::<NO_LOOPS, _, _>(graph, reverse))
}

/// Returns a [`ParSortedGraph`] representing a symmetrized version of the
/// provided graph.
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// Note that if the graph is sorted (both on nodes and successors), it is
/// recommended to use [`symmetrize_sorted_seq`].
///
/// For the meaning of the additional parameter, see [`ParSortedGraphConf`].
///
/// [`ParSortedGraphConf`]: crate::graphs::par_sorted_graph::ParSortedGraphConf
pub fn symmetrize_seq<const NO_LOOPS: bool>(
    graph: &impl SequentialGraph,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
) -> Result<ParSortedGraph<SortedPairIter<true>>> {
    ParSortedGraph::config()
        .dedup()
        .memory_usage(memory_usage)
        .progress_logger(pl)
        .sort_pairs(
            graph.num_nodes(),
            graph.iter().into_pairs().flat_map(sym_arcs::<NO_LOOPS>),
        )
}

/// Returns a [`ParSortedGraph`] representing a symmetrized version of the
/// provided graph, computed in parallel.
///
/// If `NO_LOOPS` is true, self-loops are removed from the result.
///
/// The graph must implement [`IntoParLenders`]; use [`ParGraph`] to wrap a
/// [splittable] graph as needed.
///
/// Parallelism is controlled via the current Rayon thread pool. Please
/// [install] a custom pool if you want to customize the parallelism.
///
/// For the meaning of the additional parameter, see [`ParSortedGraphConf`].
///
/// [`ParGraph`]: crate::graphs::par_graphs::ParGraph
/// [splittable]: SplitLabeling
/// [install]: rayon::ThreadPool::install
/// [`ParSortedGraphConf`]: crate::graphs::par_sorted_graph::ParSortedGraphConf
pub fn symmetrize_par<const NO_LOOPS: bool, G>(
    graph: G,
    memory_usage: MemoryUsage,
    pl: &mut impl ProgressLog,
) -> Result<ParSortedGraph<SortedPairIter<true>>>
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
    let conf = ParSortedGraph::config()
        .dedup()
        .memory_usage(memory_usage)
        .progress_logger(pl);
    let (lenders, _boundaries) = graph.into_par_lenders();
    let iters = lenders
        .into_vec()
        .into_iter()
        .map(|lender| lender.into_pairs().flat_map(sym_arcs::<NO_LOOPS>));
    conf.par_sort_pair_iters(num_nodes, iters)
}
