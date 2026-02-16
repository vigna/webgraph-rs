/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for utility types: Granularity, MemoryUsage, humanize, SplitIters,
//! RaggedArray, MaskedIter, JavaPermutation, ArcListGraph, par_node_apply,
//! par_map_fold, and temp_dir.

use anyhow::Result;
use lender::*;
use webgraph::{
    graphs::vec_graph::VecGraph, prelude::*, traits::SequentialLabeling, utils::Granularity,
};

// ── Granularity ──

#[test]
fn test_granularity_default() {
    let g = Granularity::default();
    assert!(matches!(g, Granularity::Nodes(1000)));
}

#[test]
fn test_granularity_node_to_node() {
    let g = Granularity::Nodes(500);
    assert_eq!(g.node_granularity(1000, Some(5000)), 500);
}

#[test]
fn test_granularity_arc_to_node() {
    let g = Granularity::Arcs(1000);
    // 100 nodes, 500 arcs => avg degree 5, 1000/5 = 200 nodes
    assert_eq!(g.node_granularity(100, Some(500)), 200);
}

#[test]
fn test_granularity_node_to_arc() {
    let g = Granularity::Nodes(100);
    // 1000 nodes, 5000 arcs => avg degree 5, 100*5 = 500 arcs
    assert_eq!(g.arc_granularity(1000, Some(5000)), 500);
}

#[test]
fn test_granularity_arc_to_arc() {
    let g = Granularity::Arcs(1000);
    assert_eq!(g.arc_granularity(100, Some(500)), 1000);
}

#[test]
fn test_granularity_node_granularity() {
    let g = Granularity::Nodes(100);
    assert_eq!(g.node_granularity(1000, Some(5000u64)), 100);

    let g2 = Granularity::Arcs(500u64);
    let ng = g2.node_granularity(1000, Some(5000u64));
    // Arcs(500) with 5000 arcs / 1000 nodes = avg 5 arcs/node, so 100 nodes
    assert_eq!(ng, 100);
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

#[test]
fn test_memory_usage_from_perc() {
    let mu = webgraph::utils::MemoryUsage::from_perc(10.0);
    let bs = mu.batch_size::<u64>();
    assert!(bs > 0);
}

#[test]
fn test_memory_usage_default() {
    let mu = MemoryUsage::default();
    match mu {
        MemoryUsage::MemorySize(size) => assert!(size > 0),
        _ => panic!("Expected MemorySize variant for default"),
    }
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

// ── ArcListGraph ──

#[test]
fn test_arc_list_graph_unlabeled() -> Result<()> {
    use webgraph::graphs::arc_list_graph::ArcListGraph;
    let arcs = vec![(0, 1), (0, 2), (1, 0), (2, 1)];
    let g = ArcListGraph::new(3, arcs);
    assert_eq!(g.num_nodes(), 3);
    let mut iter = g.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1, 2]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![0]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1]);
    Ok(())
}

#[test]
fn test_arc_list_graph_labeled() -> Result<()> {
    use webgraph::graphs::arc_list_graph::ArcListGraph;
    let arcs = vec![((0, 1), 10u32), ((0, 2), 20), ((1, 0), 30)];
    let g = ArcListGraph::new_labeled(3, arcs.into_iter());
    assert_eq!(g.num_nodes(), 3);
    let mut iter = g.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(1, 10), (2, 20)]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(0, 30)]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.collect::<Vec<_>>(), Vec::<(usize, u32)>::new());
    Ok(())
}

// ── JavaPermutation ──

#[test]
fn test_java_permutation_round_trip() -> Result<()> {
    use mmap_rs::MmapFlags;
    use std::io::Write;
    use value_traits::slices::SliceByValue;
    use webgraph::utils::JavaPermutation;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("perm.bin");
    let perm_data: Vec<usize> = vec![3, 1, 4, 0, 2];

    // Write as big-endian u64
    {
        let mut file = std::fs::File::create(&path)?;
        for &v in &perm_data {
            file.write_all(&(v as u64).to_be_bytes())?;
        }
    }

    // Read back via mmap
    let jp = JavaPermutation::mmap(&path, MmapFlags::empty())?;
    assert_eq!(jp.perm.as_ref().len(), perm_data.len());
    for (i, &expected) in perm_data.iter().enumerate() {
        assert_eq!(jp.index_value(i), expected);
    }
    Ok(())
}

#[test]
fn test_java_permutation_mmap_mut() -> Result<()> {
    use mmap_rs::MmapFlags;
    use value_traits::slices::{SliceByValue, SliceByValueMut};
    use webgraph::utils::JavaPermutation;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("perm_mm.bin");

    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for v in [0u64, 0, 0] {
            f.write_all(&v.to_be_bytes())?;
        }
    }

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
    use mmap_rs::MmapFlags;
    use sux::traits::BitWidth;
    use webgraph::utils::JavaPermutation;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("perm_bw.bin");

    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for v in [0u64, 0] {
            f.write_all(&v.to_be_bytes())?;
        }
    }

    let perm_ro = JavaPermutation::mmap(&path, MmapFlags::empty())?;
    assert_eq!(BitWidth::bit_width(&perm_ro), 64);
    assert_eq!(perm_ro.as_ref().len(), 2);

    let perm_rw = JavaPermutation::mmap_mut(&path, MmapFlags::empty())?;
    assert_eq!(BitWidth::bit_width(&perm_rw), 64);
    assert_eq!(perm_rw.as_ref().len(), 2);

    Ok(())
}

// ── MaskedIter ──

#[test]
fn test_masked_iter_copy_skip_copy() {
    use webgraph::graphs::bvgraph::MaskedIter;
    // blocks: [2, 1, 2] -> copy 2, skip 1, copy 2
    let parent = vec![10_usize, 20, 30, 40, 50];
    let iter = MaskedIter::new(parent.into_iter(), vec![2, 1, 2]);
    assert_eq!(iter.len(), 4);
    let result: Vec<_> = iter.collect();
    assert_eq!(result, vec![10, 20, 40, 50]);
}

#[test]
fn test_masked_iter_empty_blocks() {
    use webgraph::graphs::bvgraph::MaskedIter;
    let parent = vec![1_usize, 2, 3, 4];
    let iter = MaskedIter::new(parent.into_iter(), vec![]);
    assert_eq!(iter.len(), 4);
    assert_eq!(iter.collect::<Vec<_>>(), vec![1, 2, 3, 4]);
}

#[test]
fn test_masked_iter_single_copy() {
    use webgraph::graphs::bvgraph::MaskedIter;
    let parent = vec![5_usize, 6, 7];
    let iter = MaskedIter::new(parent.into_iter(), vec![2]);
    assert_eq!(iter.len(), 2);
    assert_eq!(iter.collect::<Vec<_>>(), vec![5, 6]);
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
