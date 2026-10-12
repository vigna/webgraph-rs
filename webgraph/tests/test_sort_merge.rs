/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for KMergeIters, ParSortPairs, ParSortIters, and Matrix.

#[cfg(not(miri))]
use anyhow::Result;
#[cfg(not(miri))]
use dsi_bitstream::prelude::*;
#[cfg(not(miri))]
use dsi_progress_logger::no_logging;
#[cfg(not(miri))]
use rayon::prelude::*;
use webgraph::utils::Matrix;
use webgraph::utils::kmerge_iters::KMergeIters;
#[cfg(not(miri))]
use webgraph::utils::{MemoryUsage, ParSortIters, ParSortPairs, SplitIters};

/// A [`KMergeIters`] on vectors of unlabeled pairs.
type KMerge = KMergeIters<std::vec::IntoIter<((usize, usize), ())>, (), false>;

/// A deduplicating [`KMergeIters`] on vectors of unlabeled pairs.
type KMergeDedup = KMergeIters<std::vec::IntoIter<((usize, usize), ())>, (), true>;

/// Returns an iterator on unlabeled versions of the given pairs.
fn unlabeled(pairs: &[(usize, usize)]) -> std::vec::IntoIter<((usize, usize), ())> {
    pairs
        .iter()
        .map(|&pair| (pair, ()))
        .collect::<Vec<_>>()
        .into_iter()
}

/// Returns the pairs of a merge, without labels.
fn keys<L>(merged: impl Iterator<Item = ((usize, usize), L)>) -> Vec<(usize, usize)> {
    merged.map(|(pair, _)| pair).collect()
}

/// Checks that the split has `num_partitions` partitions, that its
/// boundaries start at zero and end at `num_nodes`, that each partition
/// contains only pairs whose source is within its boundaries, and that the
/// concatenation of the partitions is sorted; returns the concatenation.
#[cfg(not(miri))]
fn collect_split<I: IntoIterator>(
    split: SplitIters<I>,
    num_partitions: usize,
    num_nodes: usize,
    pair: impl Fn(&I::Item) -> (usize, usize),
) -> Vec<I::Item> {
    let boundaries = split.boundaries;
    assert_eq!(boundaries.len(), num_partitions + 1);
    assert_eq!(boundaries[0], 0);
    assert_eq!(*boundaries.last().unwrap(), num_nodes);
    assert!(boundaries.is_sorted());
    assert_eq!(split.iters.len(), boundaries.len() - 1);
    let mut all = vec![];
    for (i, iter) in split.iters.into_vec().into_iter().enumerate() {
        for item in iter {
            let src = pair(&item).0;
            assert!(
                (boundaries[i]..boundaries[i + 1]).contains(&src),
                "source {src} out of partition {i}"
            );
            all.push(item);
        }
    }
    assert!(all.is_sorted_by_key(&pair), "partitions are not sorted");
    all
}

// ── KMergeIters ──

#[test]
fn test_kmerge_iters_sum_and_collect() {
    let a = KMerge::new(vec![unlabeled(&[(0, 1), (2, 3)])]);
    let b = KMerge::new(vec![unlabeled(&[(1, 2), (3, 4)])]);
    let merged: KMerge = vec![a, b].into_iter().sum();
    assert_eq!(keys(merged), [(0, 1), (1, 2), (2, 3), (3, 4)]);

    let a = KMerge::new(vec![unlabeled(&[(0, 0)])]);
    let b = KMerge::new(vec![unlabeled(&[(1, 0)])]);
    let merged: KMerge = vec![a, b].into_iter().collect();
    assert_eq!(keys(merged), [(0, 0), (1, 0)]);

    // Sum and collect of labeled into-iterators
    let iters = vec![
        vec![((0, 1), 10), ((2, 3), 30)],
        vec![((1, 2), 20), ((3, 4), 40)],
    ];
    let merged: KMergeIters<_, usize> = iters.clone().into_iter().sum();
    let expected = [((0, 1), 10), ((1, 2), 20), ((2, 3), 30), ((3, 4), 40)];
    assert_eq!(merged.collect::<Vec<_>>(), expected);
    let merged: KMergeIters<_, usize> = iters.into_iter().collect();
    assert_eq!(merged.collect::<Vec<_>>(), expected);
}

#[test]
fn test_kmerge_iters_default_extend_and_add_assign() {
    let merged = KMerge::default();
    assert_eq!(merged.count(), 0);

    let mut merged = KMerge::default();
    merged.extend(vec![unlabeled(&[(0, 1), (2, 3)]), unlabeled(&[(1, 0)])]);
    assert_eq!(keys(merged), [(0, 1), (1, 0), (2, 3)]);

    let mut merged = KMerge::new(vec![unlabeled(&[(0, 0)])]);
    merged.extend(vec![
        KMerge::new(vec![unlabeled(&[(1, 0)])]),
        KMerge::new(vec![unlabeled(&[(2, 0)])]),
    ]);
    assert_eq!(keys(merged), [(0, 0), (1, 0), (2, 0)]);

    let mut merged = KMerge::default();
    merged.extend(vec![vec![((0, 0), ()), ((2, 0), ())], vec![((1, 0), ())]]);
    assert_eq!(keys(merged), [(0, 0), (1, 0), (2, 0)]);

    let mut merged = KMerge::new(vec![unlabeled(&[(0, 0), (2, 2)])]);
    merged += KMerge::new(vec![unlabeled(&[(1, 1)])]);
    assert_eq!(keys(merged), [(0, 0), (1, 1), (2, 2)]);

    let mut merged = KMerge::default();
    merged += vec![((0, 1), ()), ((2, 3), ())];
    assert_eq!(keys(merged), [(0, 1), (2, 3)]);
}

#[test]
fn test_kmerge_iters_exact_size_iterator() {
    let merged = KMerge::new(vec![
        unlabeled(&[(0, 0), (1, 0)]),
        unlabeled(&[(2, 0), (3, 0), (4, 0)]),
    ]);
    assert_eq!(merged.len(), 5);
}

#[test]
fn test_kmerge_iters_dedup() {
    let iters = || {
        vec![
            unlabeled(&[(0, 1), (0, 1), (2, 3)]),
            unlabeled(&[(0, 1), (1, 2), (2, 3)]),
        ]
    };
    assert_eq!(keys(KMergeDedup::new(iters())), [(0, 1), (1, 2), (2, 3)]);
    assert_eq!(KMergeDedup::new(iters()).count(), 3);

    let a = KMergeDedup::new(vec![unlabeled(&[(0, 1), (0, 1)])]);
    let b = KMergeDedup::new(vec![unlabeled(&[(0, 1), (1, 2)])]);
    let merged: KMergeDedup = vec![a, b].into_iter().sum();
    assert_eq!(keys(merged), [(0, 1), (1, 2)]);
}

// ── Matrix ──

#[test]
fn test_matrix_basic() {
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

// ── ParSortPairs ──

#[test]
#[cfg(not(miri))]
fn test_par_sort_pairs() -> Result<()> {
    let pairs = [(1, 3), (3, 2), (2, 1), (1, 0), (0, 4)];
    let expected = [(0, 4), (1, 0), (1, 3), (2, 1), (3, 2)];
    for num_partitions in [1, 2, 3] {
        // Exercises also the degenerate BatchSize(0) configuration, which is
        // clamped to one element per buffer instead of allocating
        // zero-capacity buffers (whose effective batch size would depend on
        // Vec's growth policy).
        for batch_size in [0, 2, 100] {
            let split = ParSortPairs::new(5)?
                .num_partitions(num_partitions)
                .memory_usage(MemoryUsage::BatchSize(batch_size))
                .sort(pairs.par_iter().copied(), no_logging![])?;
            assert_eq!(collect_split(split, num_partitions, 5, |&p| p), expected);
        }
    }

    // Many duplicates, without deduplication
    let pairs: Vec<_> = (0..100).map(|i| (i % 10, (i + 1) % 10)).collect();
    let mut expected = pairs.clone();
    expected.sort();
    let split = ParSortPairs::new(10)?
        .num_partitions(3)
        .memory_usage(MemoryUsage::BatchSize(20))
        .sort(pairs.par_iter().copied(), no_logging![])?;
    assert_eq!(collect_split(split, 3, 10, |&p| p), expected);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_par_sort_pairs_labeled() -> Result<()> {
    use webgraph::utils::DefaultBatchCodec;
    use webgraph::utils::grouped_gaps::GroupedGapsCodec;

    let pairs = [
        ((3, 2), ()),
        ((1, 0), ()),
        ((0, 4), ()),
        ((2, 1), ()),
        ((1, 3), ()),
    ];
    let expected = [(0, 4), (1, 0), (1, 3), (2, 1), (3, 2)];
    let sorter = || -> Result<_> {
        Ok(ParSortPairs::new(5)?
            .num_partitions(2)
            .memory_usage(MemoryUsage::BatchSize(20)))
    };

    let split = sorter()?.sort_labeled(
        &<DefaultBatchCodec>::default(),
        pairs.par_iter().copied(),
        no_logging![],
    )?;
    assert_eq!(
        keys(collect_split(split, 2, 5, |p| p.0).into_iter()),
        expected
    );

    let split = sorter()?.sort_labeled(
        &GroupedGapsCodec::<BE, ()>::default(),
        pairs.par_iter().copied(),
        no_logging![],
    )?;
    assert_eq!(
        keys(collect_split(split, 2, 5, |p| p.0).into_iter()),
        expected
    );
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_par_sort_pairs_dedup() -> Result<()> {
    let pairs = [(0, 1), (0, 1), (1, 2), (1, 2), (2, 3), (0, 1)];
    let split = ParSortPairs::new_dedup(4)?
        .num_partitions(2)
        .sort(pairs.par_iter().copied(), no_logging![])?;
    assert_eq!(collect_split(split, 2, 4, |&p| p), [(0, 1), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
#[should_panic(expected = "num_partitions must be positive")]
fn test_par_sort_pairs_zero_partitions() {
    let _ = webgraph::utils::ParSortPairs::new(5)
        .unwrap()
        .num_partitions(0);
}

#[test]
#[cfg(not(miri))]
fn test_partition_boundaries_huge_num_nodes() -> Result<()> {
    // Regression: partition boundaries were computed as
    // (i * partition_size).min(num_nodes), which overflows for node counts
    // near usize::MAX; the multiplication now saturates. No pairs are
    // needed: the boundaries are derived from num_nodes alone.
    let split = ParSortPairs::new(usize::MAX)?
        .num_partitions(2)
        .sort(rayon::iter::empty::<(usize, usize)>(), no_logging![])?;
    assert!(collect_split(split, 2, usize::MAX, |&p| p).is_empty());
    Ok(())
}

// ── ParSortIters ──

#[test]
#[cfg(not(miri))]
fn test_par_sort_iters() -> Result<()> {
    let iters = || vec![vec![(1, 3), (0, 2)], vec![(2, 0), (3, 1)]];
    let expected = [(0, 2), (1, 3), (2, 0), (3, 1)];
    for num_partitions in [1, 2] {
        let split = ParSortIters::new(4)?
            .num_partitions(num_partitions)
            .memory_usage(MemoryUsage::BatchSize(10))
            .sort(iters(), no_logging![])?;
        assert_eq!(collect_split(split, num_partitions, 4, |&p| p), expected);
    }

    // Transposing the arcs of the iterators of a split graph
    use webgraph::prelude::*;
    let g =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 4), (1, 0), (1, 3), (2, 1), (3, 2)]);
    let pairs: Vec<_> = g
        .split_iter(2)
        .map(|lender| lender.into_pairs().map(|(src, dst)| (dst, src)))
        .collect();
    let split = ParSortIters::new(g.num_nodes())?
        .num_partitions(2)
        .memory_usage(MemoryUsage::BatchSize(100))
        .sort(pairs, no_logging![])?;
    assert_eq!(
        collect_split(split, 2, g.num_nodes(), |&p| p),
        [(0, 1), (1, 2), (2, 3), (3, 1), (4, 0)]
    );
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_par_sort_iters_dedup() -> Result<()> {
    use webgraph::utils::DefaultBatchCodec;
    // Two iterators with overlapping pairs
    let iter1 = vec![((0usize, 1usize), ()), ((1, 2), ()), ((0, 1), ())];
    let iter2 = vec![((1usize, 2usize), ()), ((2, 3), ()), ((2, 3), ())];
    let split = ParSortIters::new_dedup(4)?.num_partitions(2).sort_labeled(
        <DefaultBatchCodec<true>>::default(),
        vec![iter1, iter2],
        no_logging![],
    )?;
    assert_eq!(
        keys(collect_split(split, 2, 4, |p| p.0).into_iter()),
        [(0, 1), (1, 2), (2, 3)]
    );
    Ok(())
}
