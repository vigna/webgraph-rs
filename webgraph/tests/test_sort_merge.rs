/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(clippy::type_complexity)]

//! Tests for KMergeIters, ParSortPairs, ParSortIters, and Matrix.

use anyhow::Result;
use dsi_bitstream::prelude::*;
use webgraph::prelude::*;

#[test]
fn test_kmerge_iters_sum() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let a = KMergeIters::new(vec![vec![((0, 1), ()), ((2, 3), ())].into_iter()]);
    let b = KMergeIters::new(vec![vec![((1, 2), ()), ((3, 4), ())].into_iter()]);
    let merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        vec![a, b].into_iter().sum::<KMergeIters<_>>();
    let result: Vec<_> = merged.collect();
    assert_eq!(
        result,
        vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ()), ((3, 4), ())]
    );
}

#[test]
fn test_kmerge_iters_collect() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let iters: Vec<Vec<((usize, usize), ())>> = vec![
        vec![((0, 0), ()), ((1, 1), ())],
        vec![((0, 1), ()), ((2, 0), ())],
    ];
    let merged: KMergeIters<_, ()> = iters.into_iter().collect();
    let result: Vec<_> = merged.collect();
    assert_eq!(
        result,
        vec![((0, 0), ()), ((0, 1), ()), ((1, 1), ()), ((2, 0), ())]
    );
}

#[test]
fn test_kmerge_iters_default_and_extend() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> = KMergeIters::default();
    // Extend with new iterators
    merged.extend(vec![
        vec![((0, 1), ()), ((2, 3), ())].into_iter(),
        vec![((1, 0), ())].into_iter(),
    ]);
    let result: Vec<_> = merged.collect();
    assert_eq!(result, vec![((0, 1), ()), ((1, 0), ()), ((2, 3), ())]);
}

#[test]
fn test_kmerge_iters_add_assign() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut a: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        KMergeIters::new(vec![vec![((0, 0), ()), ((2, 2), ())].into_iter()]);
    let b = KMergeIters::new(vec![vec![((1, 1), ())].into_iter()]);
    a += b;
    let result: Vec<_> = a.collect();
    assert_eq!(result, vec![((0, 0), ()), ((1, 1), ()), ((2, 2), ())]);
}

#[test]
fn test_matrix_basic() {
    use webgraph::utils::Matrix;
    let mut m = Matrix::<i32>::new(3, 4);
    assert_eq!(m[(0, 0)], 0);
    m[(1, 2)] = 42;
    assert_eq!(m[(1, 2)], 42);
    m[(2, 3)] = -7;
    assert_eq!(m[(2, 3)], -7);
    // Other cells unchanged
    assert_eq!(m[(0, 0)], 0);
    assert_eq!(m[(2, 0)], 0);
}

#[test]
fn test_par_sort_pairs_basic() -> Result<()> {
    use rayon::prelude::*;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs = vec![(1, 3), (3, 2), (2, 1), (1, 0), (0, 4)];
    let sorter = ParSortPairs::new(5)?
        .expected_num_pairs(pairs.len())
        .num_partitions(2);

    let split = sorter.sort(pairs.par_iter().copied())?;
    assert_eq!(split.boundaries[0], 0);
    assert_eq!(*split.boundaries.last().unwrap(), 5);

    // Collect all pairs across all partitions
    let mut all_pairs = Vec::new();
    for iter in split.iters.into_vec() {
        let partition_pairs: Vec<_> = iter.into_iter().collect();
        // Sorted within each partition
        for w in partition_pairs.windows(2) {
            assert!(w[0] <= w[1]);
        }
        all_pairs.extend(partition_pairs);
    }
    all_pairs.sort();
    assert_eq!(all_pairs, vec![(0, 4), (1, 0), (1, 3), (2, 1), (3, 2)]);
    Ok(())
}

#[test]
fn test_par_sort_pairs_single_partition() -> Result<()> {
    use rayon::prelude::*;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs = vec![(2, 0), (0, 1), (1, 2)];
    let sorter = ParSortPairs::new(3)?.num_partitions(1);

    let split = sorter.sort(pairs.par_iter().copied())?;
    assert_eq!(split.boundaries.len(), 2); // [0, 3]
    assert_eq!(split.iters.len(), 1);
    let result: Vec<_> = split.iters.into_vec().pop().unwrap().collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (2, 0)]);
    Ok(())
}

#[test]
fn test_par_sort_pairs_with_memory_usage() -> Result<()> {
    use rayon::prelude::*;
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs: Vec<_> = (0..100).map(|i| (i % 10, (i + 1) % 10)).collect();
    let mut expected: Vec<_> = pairs.clone();
    expected.sort();
    expected.dedup();
    let sorter = ParSortPairs::new(10)?
        .expected_num_pairs(pairs.len())
        .num_partitions(3)
        .memory_usage(MemoryUsage::BatchSize(20));

    let split = sorter.sort(pairs.par_iter().copied())?;
    assert_eq!(split.boundaries[0], 0);
    assert_eq!(*split.boundaries.last().unwrap(), 10);

    let mut all_pairs = Vec::new();
    for iter in split.iters.into_vec() {
        let partition_pairs: Vec<_> = iter.into_iter().collect();
        for w in partition_pairs.windows(2) {
            assert!(w[0] <= w[1]);
        }
        all_pairs.extend(partition_pairs);
    }
    all_pairs.sort();
    all_pairs.dedup();
    assert_eq!(all_pairs, expected);
    Ok(())
}

#[test]
fn test_par_sort_iters_basic() -> Result<()> {
    use webgraph::utils::par_sort_iters::ParSortIters;

    let iter1 = vec![(1, 3), (0, 2)];
    let iter2 = vec![(2, 0), (3, 1)];
    let sorter = ParSortIters::new(4)?
        .expected_num_pairs(4)
        .num_partitions(2)
        .memory_usage(webgraph::utils::MemoryUsage::BatchSize(10));

    let split = sorter.sort(vec![iter1, iter2])?;
    assert_eq!(split.boundaries[0], 0);
    assert_eq!(*split.boundaries.last().unwrap(), 4);

    // Collect and verify all pairs are present and sorted within partitions
    let mut all: Vec<_> = Vec::new();
    for iter in split.iters.into_vec() {
        let partition: Vec<_> = iter.into_iter().collect();
        for w in partition.windows(2) {
            assert!(w[0] <= w[1]);
        }
        all.extend(partition);
    }
    all.sort();
    assert_eq!(all, vec![(0, 2), (1, 3), (2, 0), (3, 1)]);
    Ok(())
}

#[test]
fn test_par_sort_iters_single_partition() -> Result<()> {
    use webgraph::utils::par_sort_iters::ParSortIters;

    let iter1 = vec![(2, 0), (0, 1)];
    let iter2 = vec![(1, 2)];
    let sorter = ParSortIters::new(3)?.num_partitions(1);

    let split = sorter.sort(vec![iter1, iter2])?;
    assert_eq!(split.boundaries.len(), 2);
    let result: Vec<_> = split.iters.into_vec().pop().unwrap().collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (2, 0)]);
    Ok(())
}

#[test]
fn test_par_sort_pairs_labeled() -> Result<()> {
    use rayon::prelude::*;
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs = vec![((1, 3), ()), ((0, 2), ()), ((2, 1), ())];
    let sorter = ParSortPairs::new(4)?
        .expected_num_pairs(pairs.len())
        .num_partitions(2)
        .memory_usage(MemoryUsage::BatchSize(20));

    let split = sorter.sort_labeled(
        &<webgraph::utils::DefaultBatchCodec>::default(),
        pairs.par_iter().copied(),
    )?;
    assert_eq!(split.boundaries[0], 0);
    assert_eq!(*split.boundaries.last().unwrap(), 4);

    let mut all_pairs = Vec::new();
    for iter in split.iters.into_vec() {
        let partition: Vec<_> = iter.into_iter().collect();
        for w in partition.windows(2) {
            assert!(w[0].0 <= w[1].0);
        }
        all_pairs.extend(partition.into_iter().map(|(k, _)| k));
    }
    all_pairs.sort();
    assert_eq!(all_pairs, vec![(0, 2), (1, 3), (2, 1)]);
    Ok(())
}

#[test]
fn test_kmerge_iters_default() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let kmerge: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> = KMergeIters::default();
    assert_eq!(kmerge.count(), 0);
}

#[test]
fn test_kmerge_iters_sum_of_into_iterators() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let iters: Vec<Vec<((usize, usize), usize)>> = vec![
        vec![((0, 1), 10), ((2, 3), 30)],
        vec![((1, 2), 20), ((3, 4), 40)],
    ];
    let merged: KMergeIters<_, usize> = iters.into_iter().sum();
    let result: Vec<_> = merged.collect();
    assert_eq!(
        result,
        vec![((0, 1), 10), ((1, 2), 20), ((2, 3), 30), ((3, 4), 40)]
    );
}

#[test]
fn test_kmerge_iters_from_iterator_of_self() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let km1: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        KMergeIters::new(vec![vec![((0, 0), ())].into_iter()]);
    let km2 = KMergeIters::new(vec![vec![((1, 0), ())].into_iter()]);
    let merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        vec![km1, km2].into_iter().collect();
    let keys: Vec<(usize, usize)> = merged.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 0), (1, 0)]);
}

#[test]
fn test_kmerge_iters_add_assign_into_iter() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> = KMergeIters::default();
    let items = vec![((0, 1), ()), ((2, 3), ())];
    merged += items;
    let result: Vec<_> = merged.map(|(k, _)| k).collect();
    assert_eq!(result, vec![(0, 1), (2, 3)]);
}

#[test]
fn test_kmerge_iters_extend_with_kmerge() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        KMergeIters::new(vec![vec![((0, 0), ())].into_iter()]);
    let other1 = KMergeIters::new(vec![vec![((1, 0), ())].into_iter()]);
    let other2 = KMergeIters::new(vec![vec![((2, 0), ())].into_iter()]);
    merged.extend(vec![other1, other2]);
    let keys: Vec<(usize, usize)> = merged.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 0), (1, 0), (2, 0)]);
}

#[test]
fn test_kmerge_iters_extend_with_into_iters() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> = KMergeIters::default();
    let iters: Vec<Vec<((usize, usize), ())>> =
        vec![vec![((0, 0), ()), ((2, 0), ())], vec![((1, 0), ())]];
    merged.extend(iters);
    let keys: Vec<(usize, usize)> = merged.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 0), (1, 0), (2, 0)]);
}

#[test]
fn test_kmerge_iters_exact_size_iterator() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let iter1 = vec![((0, 0), ()), ((1, 0), ())].into_iter();
    let iter2 = vec![((2, 0), ()), ((3, 0), ()), ((4, 0), ())].into_iter();
    let merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        KMergeIters::new(vec![iter1, iter2]);
    assert_eq!(merged.len(), 5);
}

#[test]
fn test_par_sort_pairs_sort_labeled() -> Result<()> {
    use webgraph::utils::grouped_gaps::GroupedGapsCodec;
    use webgraph::utils::{MemoryUsage, ParSortPairs};

    let num_nodes = 5;
    let pairs: Vec<((usize, usize), ())> = vec![
        ((3, 2), ()),
        ((1, 0), ()),
        ((0, 4), ()),
        ((2, 1), ()),
        ((1, 3), ()),
    ];

    let sorter = ParSortPairs::new(num_nodes)?
        .num_partitions(2)
        .memory_usage(MemoryUsage::BatchSize(100));
    let codec = GroupedGapsCodec::<BE, ()>::default();
    use rayon::prelude::*;
    let split = sorter.sort_labeled(&codec, pairs.into_par_iter())?;

    assert_eq!(*split.boundaries.first().unwrap(), 0);
    assert_eq!(*split.boundaries.last().unwrap(), num_nodes);

    let mut all_pairs = Vec::new();
    for iter in split.iters.into_vec() {
        let partition: Vec<_> = iter.into_iter().collect();
        for w in partition.windows(2) {
            assert!(w[0].0 <= w[1].0);
        }
        all_pairs.extend(partition.into_iter().map(|(k, _)| k));
    }
    all_pairs.sort();
    assert_eq!(all_pairs, vec![(0, 4), (1, 0), (1, 3), (2, 1), (3, 2)]);
    Ok(())
}

#[test]
fn test_par_sort_iters() -> Result<()> {
    use webgraph::traits::{SequentialLabeling, SplitLabeling};
    use webgraph::utils::{MemoryUsage, ParSortIters};

    let g =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 4), (1, 0), (1, 3), (2, 1), (3, 2)]);

    let num_nodes = g.num_nodes();
    let pairs: Vec<_> = g
        .split_iter(2)
        .map(|lender| lender.into_pairs().map(|(src, dst)| (dst, src)))
        .collect();

    let sorter = ParSortIters::new(num_nodes)?
        .num_partitions(2)
        .memory_usage(MemoryUsage::BatchSize(100));
    let split = sorter.sort(pairs)?;

    assert_eq!(*split.boundaries.first().unwrap(), 0);
    assert_eq!(*split.boundaries.last().unwrap(), num_nodes);

    // Verify the transpose was sorted correctly
    let mut all_pairs = Vec::new();
    for iter in split.iters.into_vec() {
        let partition: Vec<_> = iter.into_iter().collect();
        for w in partition.windows(2) {
            assert!(w[0] <= w[1]);
        }
        all_pairs.extend(partition);
    }
    all_pairs.sort();
    // Original arcs: (0,4),(1,0),(1,3),(2,1),(3,2) transposed: (4,0),(0,1),(3,1),(1,2),(2,3)
    assert_eq!(all_pairs, vec![(0, 1), (1, 2), (2, 3), (3, 1), (4, 0)]);
    Ok(())
}

#[test]
fn test_kmerge_iters_dedup() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let merged = KMergeIters::new_dedup(vec![
        vec![((0, 1), ()), ((0, 1), ()), ((2, 3), ())].into_iter(),
        vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ())].into_iter(),
    ]);
    let result: Vec<_> = merged.map(|(k, _)| k).collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (2, 3)]);
}

#[test]
fn test_kmerge_iters_dedup_count() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let merged = KMergeIters::new_dedup(vec![
        vec![((0, 1), ()), ((0, 1), ()), ((2, 3), ())].into_iter(),
        vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ())].into_iter(),
    ]);
    assert_eq!(merged.count(), 3);
}

#[test]
fn test_kmerge_iters_dedup_sum() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let a: KMergeIters<std::vec::IntoIter<((usize, usize), ())>, (), true> =
        KMergeIters::new_dedup(vec![vec![((0, 1), ()), ((0, 1), ())].into_iter()]);
    let b: KMergeIters<std::vec::IntoIter<((usize, usize), ())>, (), true> =
        KMergeIters::new_dedup(vec![vec![((0, 1), ()), ((1, 2), ())].into_iter()]);
    let merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>, (), true> =
        vec![a, b].into_iter().sum();
    let result: Vec<_> = merged.map(|(k, _)| k).collect();
    assert_eq!(result, vec![(0, 1), (1, 2)]);
}

#[test]
fn test_par_sort_pairs_dedup() -> Result<()> {
    use rayon::prelude::*;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs = vec![(0, 1), (0, 1), (1, 2), (1, 2), (2, 3), (0, 1)];
    let sorter = ParSortPairs::new_dedup(4)?.num_partitions(2);

    let split = sorter.sort(pairs.par_iter().copied())?;

    let mut all_pairs = Vec::new();
    for iter in split.iters.into_vec() {
        all_pairs.extend(iter);
    }
    all_pairs.sort();
    assert_eq!(all_pairs, vec![(0, 1), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
fn test_par_sort_iters_dedup() -> Result<()> {
    use webgraph::utils::par_sort_iters::ParSortIters;

    // Two iterators with overlapping pairs
    let iter1 = vec![((0usize, 1usize), ()), ((1, 2), ()), ((0, 1), ())];
    let iter2 = vec![((1usize, 2usize), ()), ((2, 3), ()), ((2, 3), ())];
    let sorter = ParSortIters::new_dedup(4)?.num_partitions(2);

    let split = sorter.sort_labeled(
        <webgraph::utils::DefaultBatchCodec<true>>::default(),
        vec![iter1, iter2],
    )?;

    let mut all_pairs = Vec::new();
    for iter in split.iters.into_vec() {
        all_pairs.extend(iter.into_iter().map(|(k, _)| k));
    }
    all_pairs.sort();
    assert_eq!(all_pairs, vec![(0, 1), (1, 2), (2, 3)]);
    Ok(())
}
