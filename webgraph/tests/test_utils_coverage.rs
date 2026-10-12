/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for utility types: Granularity, MemoryUsage, humanize, SplitIters,
//! RaggedArray, MaskedIter, JavaPermutation, ArcListGraph, par_node_apply,
//! par_map_fold, par_map_fold_ord, and temp_dir.

mod common;

use anyhow::Result;
use webgraph::{graphs::vec_graph::VecGraph, prelude::*, utils::Granularity};

// ── Granularity ──

#[test]
fn test_granularity_default() {
    let g = Granularity::default();
    assert!(matches!(g, Granularity::Nodes(10000)));
}

#[test]
fn test_granularity_conversions() {
    assert_eq!(
        Granularity::Nodes(500).node_granularity(1000, Some(5000)),
        500
    );
    // 100 nodes, 500 arcs => avg degree 5, 1000/5 = 200 nodes
    assert_eq!(
        Granularity::Arcs(1000).node_granularity(100, Some(500)),
        200
    );
    // 1000 nodes, 5000 arcs => avg degree 5, 100*5 = 500 arcs
    assert_eq!(
        Granularity::Nodes(100).arc_granularity(1000, Some(5000)),
        500
    );
    assert_eq!(
        Granularity::Arcs(1000).arc_granularity(100, Some(500)),
        1000
    );
}

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_no_arcs_info() {
    let g = Granularity::Arcs(100);
    let _ = g.node_granularity(50, None);
}

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_nodes_no_arcs_info() {
    let g = Granularity::Nodes(100);
    let _ = g.arc_granularity(1000, None);
}

#[test]
fn test_granularity_zero_clamped() {
    // Regression: zero granularity (reachable from the CLI) used to
    // propagate as a zero divisor/chunk size to par_node_apply/par_apply.
    assert_eq!(Granularity::Nodes(0).node_granularity(10, None), 1);
    assert_eq!(Granularity::Arcs(0).node_granularity(10, Some(20)), 1);
    assert_eq!(Granularity::Nodes(0).arc_granularity(10, Some(20)), 1);
    assert_eq!(Granularity::Arcs(0).arc_granularity(10, Some(20)), 1);
}

// ── par_node_apply ──

#[test]
fn test_par_node_apply() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    use dsi_progress_logger::no_logging;
    let degree_sum = g.par_node_apply(
        |range| range.map(|node| g.outdegree(node) as u64).sum::<u64>(),
        |a, b| a + b,
        Granularity::Nodes(2),
        no_logging![],
    );
    assert_eq!(degree_sum, g.num_arcs());
    Ok(())
}

// ── MemoryUsage ──

#[test]
fn test_memory_usage_batch_size() {
    let mu = MemoryUsage::BatchSize(1000);
    assert_eq!(mu.batch_size::<u64>(), 1000);
    assert_eq!(mu.batch_size::<u32>(), 1000);
}

#[test]
fn test_memory_usage_memory_size() {
    let mu = MemoryUsage::MemorySize(8000);
    assert_eq!(mu.batch_size::<u64>(), 1000); // 8000 / 8
    assert_eq!(mu.batch_size::<u32>(), 2000); // 8000 / 4
}

#[test]
fn test_memory_usage_mul_div() {
    let mu = MemoryUsage::MemorySize(1000);
    let scaled = mu * 3;
    assert_eq!(scaled.batch_size::<u8>(), 3000);

    let halved = mu / 2;
    assert_eq!(halved.batch_size::<u8>(), 500);

    let mu = MemoryUsage::BatchSize(1000);
    let scaled = mu * 2;
    assert_eq!(scaled.batch_size::<u64>(), 2000);

    let halved = mu / 4;
    assert_eq!(halved.batch_size::<u64>(), 250);
}

// ── humanize ──

#[test]
fn test_humanize() {
    use webgraph::utils::humanize;
    assert_eq!(humanize(0.0), "0");
    assert_eq!(humanize(999.0), "999");
    assert_eq!(humanize(1000.0), "1.000K");
    assert_eq!(humanize(1500.0), "1.500K");
    assert_eq!(humanize(1_000_000.0), "1.000M");
    assert_eq!(humanize(2_500_000_000.0), "2.500G");
    assert_eq!(humanize(1_000_000_000_000.0), "1.000T");
}

// ── RaggedArray ──

#[test]
fn test_ragged_array_operations() {
    use webgraph::utils::RaggedArray;
    let mut ra = RaggedArray::<i32>::new();
    assert!(ra.is_empty());
    assert_eq!(ra.num_values(), 0);
    ra.push(vec![1, 2, 3]);
    ra.push(vec![4]);
    assert!(!ra.is_empty());
    assert_eq!(ra.len(), 2);
    assert_eq!(ra.num_values(), 4);
    assert!(ra.values_capacity() >= 4);
    ra.shrink_to_fit();
    ra.shrink_values_to(4);
    assert_eq!(&ra[0], &[1, 2, 3]);
    assert_eq!(&ra[1], &[4]);
}

// ── SplitIters ──

#[test]
fn test_split_iters() {
    use webgraph::utils::SplitIters;
    let boundaries = vec![0, 3, 5].into_boxed_slice();
    let iters = vec![vec![1, 2, 3], vec![4, 5]].into_boxed_slice();
    let si = SplitIters::new(boundaries, iters);
    assert_eq!(&*si.boundaries, &[0, 3, 5]);
    assert_eq!(&*si.iters, &[vec![1, 2, 3], vec![4, 5]]);

    // Test From conversion
    let si2: SplitIters<Vec<i32>> = (
        vec![0, 10].into_boxed_slice(),
        vec![vec![42]].into_boxed_slice(),
    )
        .into();
    assert_eq!(&*si2.boundaries, &[0, 10]);
}

// ── temp_dir ──

#[test]
fn test_temp_dir() -> Result<()> {
    use webgraph::utils::temp_dir;
    let base = tempfile::tempdir()?;
    let dir = temp_dir(base.path())?;
    assert!(dir.exists());
    assert!(dir.is_dir());
    assert!(dir.starts_with(base.path()));
    Ok(())
}

// ── JavaPermutation ──
//
#[cfg(all(target_pointer_width = "64", not(miri)))]
mod java_permutation {
    use super::*;

    use common::write_be_u64s;
    use mmap_rs::MmapFlags;
    use value_traits::slices::{SliceByValue, SliceByValueMut};
    use webgraph::utils::JavaPermutation;

    #[test]
    fn test_java_permutation_round_trip() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("perm.bin");
        let perm_data = [3, 1, 4, 0, 2];
        write_be_u64s(&path, &perm_data)?;
        let jp = JavaPermutation::mmap(&path, MmapFlags::empty())?;
        assert_eq!(jp.perm.as_ref().len(), perm_data.len());
        let values: Vec<_> = (0..perm_data.len())
            .map(|i| jp.index_value(i) as u64)
            .collect();
        assert_eq!(values, perm_data);
        Ok(())
    }

    #[test]
    fn test_java_permutation_mmap_mut() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("perm_mm.bin");
        write_be_u64s(&path, &[0, 0, 0])?;
        let mut perm = JavaPermutation::mmap_mut(&path, MmapFlags::empty())?;
        assert_eq!(perm.len(), 3);
        unsafe {
            perm.set_value_unchecked(0, 10);
            perm.set_value_unchecked(1, 20);
            perm.set_value_unchecked(2, 30);
        }
        assert_eq!(unsafe { perm.get_value_unchecked(0) }, 10);
        assert_eq!(unsafe { perm.get_value_unchecked(2) }, 30);
        assert_eq!(perm.as_ref().len(), 3);

        Ok(())
    }

    #[test]
    fn test_java_permutation_bit_width() -> Result<()> {
        use sux::traits::BitWidth;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("perm_bw.bin");
        write_be_u64s(&path, &[0, 0])?;

        let perm_ro = JavaPermutation::mmap(&path, MmapFlags::empty())?;
        assert_eq!(BitWidth::bit_width(&perm_ro), 64);
        assert_eq!(perm_ro.as_ref().len(), 2);

        let perm_rw = JavaPermutation::mmap_mut(&path, MmapFlags::empty())?;
        assert_eq!(BitWidth::bit_width(&perm_rw), 64);
        assert_eq!(perm_rw.as_ref().len(), 2);

        Ok(())
    }
}

// ── MaskedIter ──

#[test]
fn test_masked_iter() {
    use webgraph::graphs::bvgraph::MaskedIter;
    for (blocks, expected) in [
        // Copy 2, skip 1, copy 2
        (vec![2, 1, 2], vec![10, 20, 40, 50]),
        // No blocks: copy everything
        (vec![], vec![10, 20, 30, 40, 50]),
        // A single copy block
        (vec![2], vec![10, 20]),
    ] {
        let iter = MaskedIter::new(vec![10_usize, 20, 30, 40, 50].into_iter(), blocks);
        assert_eq!(iter.len(), expected.len());
        assert_eq!(iter.collect::<Vec<_>>(), expected);
    }
}

// ── ParMapFold ──

#[test]
fn test_par_map_fold_basic() {
    use webgraph::traits::par_map_fold::ParMapFold;
    let items = vec![1usize, 2, 3, 4, 5];
    let result: usize = items.into_iter().par_map_fold(|x| x * 2, |a, b| a + b);
    assert_eq!(result, 30); // 2+4+6+8+10
}

#[test]
fn test_par_map_fold2_basic() {
    use webgraph::traits::par_map_fold::ParMapFold;
    let items = vec![1usize, 2, 3, 4, 5];
    let result: usize = items
        .into_iter()
        .par_map_fold2(|x| x * x, |acc, v| acc + v, |a, b| a + b);
    assert_eq!(result, 55); // 1+4+9+16+25
}

#[test]
// Excluded under Miri: the ordered `par_map_fold` path passes in isolation, but
// when co-scheduled with the rest of this binary the extra tests perturb Miri's
// cooperative scheduler enough to expose a protector conflict in rayon-core's
// `in_place_scope` teardown, tripping experimental Tree Borrows. See the note in
// `test_par_map_fold_single_thread_pool.rs`.
#[cfg(not(miri))]
fn test_par_map_fold_ord() {
    use webgraph::traits::par_map_fold::ParMapFold;
    // Verifies that results are folded in the original iterator order
    // using a non-commutative fold (Vec::push).
    for &n in &[0, 1, 2, 5, 20, 100, 1000] {
        let result: Vec<usize> = (0..n).par_map_fold_ord(
            |x| x * x,
            Vec::new(),
            |mut acc, val| {
                acc.push(val);
                acc
            },
        );
        let expected: Vec<usize> = (0..n).map(|x: usize| x * x).collect();
        assert_eq!(result, expected);
    }
}

#[test]
// Excluded under Miri for the same reason as `test_par_map_fold_ord` above.
#[cfg(not(miri))]
fn test_par_map_fold_ord_with() {
    use webgraph::traits::par_map_fold::ParMapFold;
    // Verifies that map_init is properly cloned and passed to each worker.
    let result: Vec<usize> = (0..100usize).par_map_fold_ord_with(
        10usize, // map_init: an offset added by the map function
        |offset, x| x + *offset,
        Vec::new(),
        |mut acc, val| {
            acc.push(val);
            acc
        },
    );
    let expected: Vec<usize> = (0..100).map(|x: usize| x + 10).collect();
    assert_eq!(result, expected);
}
