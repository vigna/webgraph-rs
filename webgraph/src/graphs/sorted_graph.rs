/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! A graph built by sorting pairs of nodes in parallel.

use crate::graphs::arc_list_graph;
use crate::labels::proj::LeftIterator;
use crate::prelude::*;
use crate::utils::par_sort_iters::ParSortIters;
use crate::utils::{MemoryUsage, SortedPairIter};
use anyhow::Result;
use lender::*;
use std::iter::Flatten;
use std::num::NonZeroUsize;

/// A graph representation built by sorting arc pairs in parallel.
///
/// Stores partition boundaries and sorted iterators for each partition.
/// The number of nodes is derived from the last boundary value.
pub struct SortedGraph<I: Clone> {
    boundaries: Box<[usize]>,
    iters: Box<[I]>,
}

type SeqIter<'a, I> = Flatten<std::iter::Cloned<std::slice::Iter<'a, I>>>;

#[allow(clippy::type_complexity)]
type MapFn = fn((usize, usize)) -> ((usize, usize), ());

impl SortedGraph<SortedPairIter> {
    /// Creates a new [`SortedGraph`] by sorting arcs from the given graph
    /// using default partitioning and memory settings.
    pub fn new<G>(graph: G) -> Result<Self>
    where
        G: SequentialGraph
            + for<'g, 'a> SplitLabeling<
                SplitLender<'g>: NodeLabelsLender<
                    'a,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                >,
            >,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();

        let mut par_sort = ParSortIters::new(num_nodes)?;
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }

        let pairs: Vec<_> = graph
            .split_iter(rayon::current_num_threads())
            .into_iter()
            .map(|iter| iter.into_pairs())
            .collect();

        let split = par_sort.sort(pairs)?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }

    /// Creates a new [`SortedGraph`] by sorting arcs from the given graph
    /// using default partitioning and custom memory settings.
    pub fn with_mem<G>(graph: G, memory_usage: MemoryUsage) -> Result<Self>
    where
        G: SequentialGraph
            + for<'g, 'a> SplitLabeling<
                SplitLender<'g>: NodeLabelsLender<
                    'a,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                >,
            >,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();

        let mut par_sort = ParSortIters::new(num_nodes)?.memory_usage(memory_usage);
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }

        let pairs: Vec<_> = graph
            .split_iter(rayon::current_num_threads())
            .into_iter()
            .map(|iter| iter.into_pairs())
            .collect();

        let split = par_sort.sort(pairs)?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }

    /// Creates a new [`SortedGraph`] by sorting arcs from the given graph
    /// using custom partitioning and default memory settings.
    pub fn with_part<G>(graph: G, num_partitions: NonZeroUsize) -> Result<Self>
    where
        G: SequentialGraph
            + for<'g, 'a> SplitLabeling<
                SplitLender<'g>: NodeLabelsLender<
                    'a,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                >,
            >,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();

        let mut par_sort = ParSortIters::new(num_nodes)?.num_partitions(num_partitions);
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }

        let pairs: Vec<_> = graph
            .split_iter(rayon::current_num_threads())
            .into_iter()
            .map(|iter| iter.into_pairs())
            .collect();

        let split = par_sort.sort(pairs)?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }

    /// Creates a new [`SortedGraph`] by sorting arcs from the given graph
    /// using custom partitioning and memory settings.
    pub fn with_part_and_mem<G>(
        graph: G,
        num_partitions: NonZeroUsize,
        memory_usage: MemoryUsage,
    ) -> Result<Self>
    where
        G: SequentialGraph
            + for<'g, 'a> SplitLabeling<
                SplitLender<'g>: NodeLabelsLender<
                    'a,
                    IntoIterator: IntoIterator<IntoIter: Send + Sync>,
                >,
            >,
    {
        let num_nodes = graph.num_nodes();
        let num_arcs_hint = graph.num_arcs_hint();

        let mut par_sort = ParSortIters::new(num_nodes)?
            .num_partitions(num_partitions)
            .memory_usage(memory_usage);
        if let Some(num_arcs) = num_arcs_hint {
            par_sort = par_sort.expected_num_pairs(num_arcs as usize);
        }

        let pairs: Vec<_> = graph
            .split_iter(rayon::current_num_threads())
            .into_iter()
            .map(|iter| iter.into_pairs())
            .collect();

        let split = par_sort.sort(pairs)?;
        Ok(SortedGraph {
            boundaries: split.boundaries,
            iters: split.iters,
        })
    }
}

impl<I: Clone> SortedGraph<I> {
    /// Creates a [`SortedGraph`] from pre-sorted partition boundaries and
    /// iterators.
    pub fn from_parts(boundaries: Box<[usize]>, iters: Box<[I]>) -> Self {
        SortedGraph { boundaries, iters }
    }
}

impl<I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> SequentialLabeling
    for SortedGraph<I>
{
    type Label = usize;
    type Lender<'node>
        = LeftIterator<arc_list_graph::NodeLabels<(), std::iter::Map<SeqIter<'node, I>, MapFn>>>
    where
        Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        *self.boundaries.last().unwrap_or(&0)
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        let num_nodes = self.num_nodes();
        let map_fn: MapFn = |pair| (pair, ());
        let iter = self.iters.iter().cloned().flatten().map(map_fn);
        let mut lender = LeftIterator(arc_list_graph::NodeLabels::new(num_nodes, iter));
        lender.advance_by(from).unwrap();
        lender
    }
}

impl<I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> SequentialGraph for SortedGraph<I> {}

impl<I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> ParallelLabeling for SortedGraph<I> {
    type ParLender<'node>
        = LeftIterator<arc_list_graph::NodeLabels<(), std::iter::Map<I, MapFn>>>
    where
        Self: 'node;

    fn par_iters(&self) -> (Box<[Self::ParLender<'_>]>, Box<[usize]>) {
        let map_fn: MapFn = |pair| (pair, ());
        let lenders: Box<[_]> = self
            .iters
            .iter()
            .enumerate()
            .map(|(i, iter)| {
                let start = self.boundaries[i];
                let end = self.boundaries[i + 1];
                let num_partition_nodes = end - start;
                let labeled_iter = iter.clone().map(map_fn);
                let node_labels = arc_list_graph::NodeLabels::try_new_from(
                    num_partition_nodes,
                    labeled_iter,
                    start,
                )
                .expect("Iterator should start from the expected first node");
                LeftIterator(node_labels)
            })
            .collect();
        (lenders, self.boundaries.clone())
    }
}

impl<'a, I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> IntoLender
    for &'a SortedGraph<I>
{
    type Lender = <SortedGraph<I> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<'lend, I: Iterator<Item = (usize, usize)> + Clone + Send + Sync> Lending<'lend>
    for &SortedGraph<I>
{
    type Lend = Lend<'lend, <SortedGraph<I> as SequentialLabeling>::Lender<'lend>>;
}
