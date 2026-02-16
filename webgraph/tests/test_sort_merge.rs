/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for SortPairs, KMergeIters, ParSortPairs, ParSortIters, and Matrix.

use anyhow::Result;
use dsi_bitstream::prelude::*;
use webgraph::prelude::*;

// ── From test_core.rs ──

#[test]
fn test_sort_pairs_basic_v1() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    sp.push(2, 3)?;
    sp.push(0, 1)?;
    sp.push(1, 2)?;
    sp.push(0, 0)?;
    let result: Vec<_> = sp.iter()?.collect();
    assert_eq!(
        result,
        vec![((0, 0), ()), ((0, 1), ()), ((1, 2), ()), ((2, 3), ())]
    );
    Ok(())
}

#[test]
fn test_sort_pairs_sort_method() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs = vec![(3, 0), (1, 2), (0, 1), (2, 3)];
    let result: Vec<_> = sp.sort(pairs)?.collect();
    assert_eq!(
        result,
        vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ()), ((3, 0), ())]
    );
    Ok(())
}

#[test]
fn test_sort_pairs_multiple_batches() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    // Tiny batch size to force multiple batches
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(2), dir.path())?;
    sp.push(5, 0)?;
    sp.push(3, 1)?;
    sp.push(1, 2)?;
    sp.push(0, 3)?;
    sp.push(4, 4)?;
    sp.push(2, 5)?;
    let result: Vec<((usize, usize), ())> = sp.iter()?.collect();
    // Should be sorted by (src, dst) lexicographic order
    assert_eq!(result[0].0, (0, 3));
    assert_eq!(result[1].0, (1, 2));
    assert_eq!(result[2].0, (2, 5));
    assert_eq!(result[3].0, (3, 1));
    assert_eq!(result[4].0, (4, 4));
    assert_eq!(result[5].0, (5, 0));
    Ok(())
}

#[test]
fn test_sort_pairs_non_empty_dir() {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir().unwrap();
    // Create a file in the dir to make it non-empty
    std::fs::write(dir.path().join("dummy"), b"x").unwrap();
    let result = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path());
    assert!(result.is_err());
}

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
fn test_sort_pairs_sort_labeled() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    // Use SortPairs unlabeled (which uses DefaultBatchCodec internally)
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs = vec![((2, 3), ()), ((0, 1), ()), ((1, 2), ())];
    let result: Vec<_> = sp.sort_labeled(pairs)?.collect();
    assert_eq!(result, vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ())]);
    Ok(())
}

#[test]
fn test_sort_pairs_try_sort_labeled() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs: Vec<Result<_, std::convert::Infallible>> =
        vec![Ok(((2, 0), ())), Ok(((0, 1), ())), Ok(((1, 0), ()))];
    let result: Vec<_> = sp.try_sort_labeled(pairs)?.collect();
    assert_eq!(result, vec![((0, 1), ()), ((1, 0), ()), ((2, 0), ())]);
    Ok(())
}

#[test]
fn test_sort_pairs_try_sort_v1() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs: Vec<Result<_, std::convert::Infallible>> = vec![Ok((3, 1)), Ok((1, 2)), Ok((0, 0))];
    let result: Vec<_> = sp.try_sort(pairs)?.collect();
    assert_eq!(result, vec![((0, 0), ()), ((1, 2), ()), ((3, 1), ())]);
    Ok(())
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

// ── From test_coverage.rs ──

#[test]
fn test_par_sort_pairs_basic() -> Result<()> {
    use rayon::prelude::*;
    use std::num::NonZeroUsize;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs = vec![(1, 3), (3, 2), (2, 1), (1, 0), (0, 4)];
    let sorter = ParSortPairs::new(5)?
        .expected_num_pairs(pairs.len())
        .num_partitions(NonZeroUsize::new(2).unwrap());

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
    use std::num::NonZeroUsize;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs = vec![(2, 0), (0, 1), (1, 2)];
    let sorter = ParSortPairs::new(3)?.num_partitions(NonZeroUsize::new(1).unwrap());

    let split = sorter.sort(pairs.par_iter().copied())?;
    assert_eq!(split.boundaries.len(), 2); // [0, 3]
    assert_eq!(split.iters.len(), 1);
    let result: Vec<_> = split.iters.into_vec().pop().unwrap().into_iter().collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (2, 0)]);
    Ok(())
}

#[test]
fn test_par_sort_pairs_with_memory_usage() -> Result<()> {
    use rayon::prelude::*;
    use std::num::NonZeroUsize;
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs: Vec<_> = (0..100).map(|i| (i % 10, (i + 1) % 10)).collect();
    let mut expected: Vec<_> = pairs.clone();
    expected.sort();
    expected.dedup();
    let sorter = ParSortPairs::new(10)?
        .expected_num_pairs(pairs.len())
        .num_partitions(NonZeroUsize::new(3).unwrap())
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
fn test_sort_pairs_labeled() -> Result<()> {
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::sort_pairs::SortPairs;

    let tmp = tempfile::tempdir()?;
    let mut sp = SortPairs::new_labeled(
        MemoryUsage::BatchSize(100),
        tmp.path(),
        webgraph::utils::DefaultBatchCodec::default(),
    )?;
    sp.push_labeled(2, 3, ())?;
    sp.push_labeled(0, 1, ())?;
    sp.push_labeled(1, 2, ())?;

    let iter = sp.iter()?;
    let result: Vec<_> = iter.map(|((s, d), _)| (s, d)).collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
fn test_sort_pairs_try_sort_v2() -> Result<()> {
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::sort_pairs::SortPairs;

    let tmp = tempfile::tempdir()?;
    let mut sp = SortPairs::new(MemoryUsage::BatchSize(100), tmp.path())?;
    let pairs: Vec<Result<(usize, usize), std::convert::Infallible>> =
        vec![Ok((3, 0)), Ok((1, 2)), Ok((0, 1))];
    let iter = sp.try_sort(pairs)?;
    let result: Vec<_> = iter.map(|((s, d), _)| (s, d)).collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (3, 0)]);
    Ok(())
}

#[test]
fn test_par_sort_iters_basic() -> Result<()> {
    use std::num::NonZeroUsize;
    use webgraph::utils::par_sort_iters::ParSortIters;

    let iter1 = vec![(1, 3), (0, 2)];
    let iter2 = vec![(2, 0), (3, 1)];
    let sorter = ParSortIters::new(4)?
        .expected_num_pairs(4)
        .num_partitions(NonZeroUsize::new(2).unwrap())
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
    use std::num::NonZeroUsize;
    use webgraph::utils::par_sort_iters::ParSortIters;

    let iter1 = vec![(2, 0), (0, 1)];
    let iter2 = vec![(1, 2)];
    let sorter = ParSortIters::new(3)?.num_partitions(NonZeroUsize::new(1).unwrap());

    let split = sorter.sort(vec![iter1, iter2])?;
    assert_eq!(split.boundaries.len(), 2);
    let result: Vec<_> = split.iters.into_vec().pop().unwrap().into_iter().collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (2, 0)]);
    Ok(())
}

#[test]
fn test_sort_pairs_labeled_with_values() -> Result<()> {
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::sort_pairs::SortPairs;

    let tmp = tempfile::tempdir()?;
    let mut sp = SortPairs::new_labeled(
        MemoryUsage::BatchSize(100),
        tmp.path(),
        webgraph::utils::DefaultBatchCodec::default(),
    )?;
    sp.push_labeled(2, 3, ())?;
    sp.push_labeled(0, 1, ())?;
    sp.push_labeled(2, 1, ())?;
    sp.push_labeled(1, 2, ())?;
    sp.push_labeled(0, 3, ())?;

    let iter = sp.iter()?;
    let result: Vec<_> = iter.map(|((s, d), _)| (s, d)).collect();
    assert_eq!(result, vec![(0, 1), (0, 3), (1, 2), (2, 1), (2, 3)]);
    Ok(())
}

#[test]
fn test_sort_pairs_push_unlabeled() -> Result<()> {
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::sort_pairs::SortPairs;

    let tmp = tempfile::tempdir()?;
    let mut sp = SortPairs::new(MemoryUsage::BatchSize(100), tmp.path())?;
    sp.push(3, 0)?;
    sp.push(1, 2)?;
    sp.push(0, 1)?;
    sp.push(2, 3)?;

    let iter = sp.iter()?;
    let result: Vec<_> = iter.map(|((s, d), _)| (s, d)).collect();
    assert_eq!(result, vec![(0, 1), (1, 2), (2, 3), (3, 0)]);
    Ok(())
}

#[test]
fn test_par_sort_pairs_labeled() -> Result<()> {
    use rayon::prelude::*;
    use std::num::NonZeroUsize;
    use webgraph::utils::MemoryUsage;
    use webgraph::utils::par_sort_pairs::ParSortPairs;

    let pairs = vec![((1, 3), ()), ((0, 2), ()), ((2, 1), ())];
    let sorter = ParSortPairs::new(4)?
        .expected_num_pairs(pairs.len())
        .num_partitions(NonZeroUsize::new(2).unwrap())
        .memory_usage(MemoryUsage::BatchSize(20));

    let split = sorter.sort_labeled(
        &webgraph::utils::DefaultBatchCodec::default(),
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
fn test_sort_pairs_basic_v2() -> Result<()> {
    use webgraph::utils::{MemoryUsage, SortPairs};

    let dir = tempfile::tempdir()?;
    let mut sort_pairs = SortPairs::new(MemoryUsage::BatchSize(100), dir.path())?;
    sort_pairs.push(2, 3)?;
    sort_pairs.push(0, 1)?;
    sort_pairs.push(1, 2)?;
    sort_pairs.push(0, 2)?;

    let iter = sort_pairs.iter()?;
    let pairs: Vec<((usize, usize), ())> = iter.collect();
    let keys: Vec<(usize, usize)> = pairs.into_iter().map(|(k, _)| k).collect();
    // Should be sorted by (src, dst)
    assert_eq!(keys, vec![(0, 1), (0, 2), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
fn test_sort_pairs_convenience() -> Result<()> {
    use webgraph::utils::{MemoryUsage, SortPairs};

    let dir = tempfile::tempdir()?;
    let mut sort_pairs = SortPairs::new(MemoryUsage::BatchSize(100), dir.path())?;
    let result: Vec<((usize, usize), ())> =
        sort_pairs.sort(vec![(3, 4), (1, 2), (0, 1)])?.collect();
    let keys: Vec<(usize, usize)> = result.into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 1), (1, 2), (3, 4)]);
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
fn test_kmerge_iters_from_iterator_of_into_iters() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let iters = vec![
        vec![((0, 0), ()), ((1, 1), ())],
        vec![((0, 1), ()), ((2, 0), ())],
    ];
    let merged: KMergeIters<_, ()> = iters.into_iter().collect();
    let keys: Vec<(usize, usize)> = merged.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 0), (0, 1), (1, 1), (2, 0)]);
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
fn test_kmerge_iters_add_assign_self() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut merged1: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        KMergeIters::new(vec![vec![((0, 0), ()), ((2, 0), ())].into_iter()]);
    let merged2 = KMergeIters::new(vec![vec![((1, 0), ()), ((3, 0), ())].into_iter()]);
    merged1 += merged2;
    let keys: Vec<(usize, usize)> = merged1.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 0), (1, 0), (2, 0), (3, 0)]);
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
fn test_sort_pairs_labeled_with_gaps_codec() -> Result<()> {
    use webgraph::utils::gaps::GapsCodec;
    use webgraph::utils::{MemoryUsage, SortPairs};

    let dir = tempfile::tempdir()?;
    let codec = GapsCodec::<BE, (), ()>::default();
    let mut sp = SortPairs::new_labeled(MemoryUsage::BatchSize(3), dir.path(), codec)?;

    // Push more than batch_size items to trigger dump
    sp.push_labeled(3, 4, ())?;
    sp.push_labeled(1, 2, ())?;
    sp.push_labeled(0, 1, ())?;
    sp.push_labeled(2, 3, ())?;
    sp.push_labeled(4, 5, ())?;

    let iter = sp.iter()?;
    let items: Vec<_> = iter.collect();
    let keys: Vec<(usize, usize)> = items.into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
    Ok(())
}

#[test]
fn test_sort_pairs_try_sort_fallible() -> Result<()> {
    use webgraph::utils::{MemoryUsage, SortPairs};

    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(MemoryUsage::BatchSize(100), dir.path())?;

    let pairs: Vec<Result<(usize, usize), std::convert::Infallible>> =
        vec![Ok((2, 3)), Ok((0, 1)), Ok((1, 2))];
    let iter = sp.try_sort(pairs)?;
    let keys: Vec<(usize, usize)> = iter.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![(0, 1), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
fn test_par_sort_pairs_sort_labeled() -> Result<()> {
    use std::num::NonZeroUsize;
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
        .num_partitions(NonZeroUsize::new(2).unwrap())
        .memory_usage(MemoryUsage::BatchSize(100));
    let codec = GroupedGapsCodec::<BE, (), ()>::default();
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
    use std::num::NonZeroUsize;
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
        .num_partitions(NonZeroUsize::new(2).unwrap())
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
