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

// ═══════════════════════════════════════════════════════════════════════
//  From test_core.rs
// ═══════════════════════════════════════════════════════════════════════

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

// ── par_node_apply ──

#[test]
fn test_par_node_apply_v1() -> Result<()> {
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

// ── Granularity: no arcs info ──

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_no_arcs_info() {
    let g = Granularity::Arcs(100);
    // Panics because arcs-to-nodes conversion requires arc count
    let _ = g.node_granularity(50, None);
}

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_nodes_no_arcs_info_v1() {
    let g = Granularity::Nodes(100);
    // Panics because nodes-to-arcs conversion requires arc count
    let _ = g.arc_granularity(1000, None);
}

// ── MemoryUsage ──

#[test]
fn test_memory_usage_batch_size_v1() {
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
fn test_memory_usage_mul_div_v1() {
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
fn test_memory_usage_display_v1() {
    let mu = MemoryUsage::MemorySize(1024);
    assert_eq!(format!("{}", mu), "1024 bytes");

    let mu = MemoryUsage::BatchSize(500);
    assert_eq!(format!("{}", mu), "500 elements");
}

// ── humanize ──

#[test]
fn test_humanize_v1() {
    use webgraph::utils::humanize;
    assert_eq!(humanize(0.0), "0");
    assert_eq!(humanize(999.0), "999");
    assert_eq!(humanize(1000.0), "1.000K");
    assert_eq!(humanize(1500.0), "1.500K");
    assert_eq!(humanize(1_000_000.0), "1.000M");
    assert_eq!(humanize(2_500_000_000.0), "2.500G");
}

// ── MemoryUsage: from_perc and batch_size ──

#[test]
fn test_memory_usage_from_perc_v1() {
    let mu = webgraph::utils::MemoryUsage::from_perc(10.0);
    // Just verify it creates a MemorySize variant with a reasonable value
    let bs = mu.batch_size::<u64>();
    assert!(bs > 0);
}

#[test]
fn test_memory_usage_batch_size_memory() {
    let mu = webgraph::utils::MemoryUsage::MemorySize(1024);
    // size_of::<u64>() == 8, so 1024 / 8 = 128
    assert_eq!(mu.batch_size::<u64>(), 128);
    // size_of::<u32>() == 4, so 1024 / 4 = 256
    assert_eq!(mu.batch_size::<u32>(), 256);
}

#[test]
fn test_memory_usage_batch_size_batch() {
    let mu = webgraph::utils::MemoryUsage::BatchSize(42);
    // BatchSize just returns the value regardless of T
    assert_eq!(mu.batch_size::<u64>(), 42);
    assert_eq!(mu.batch_size::<u8>(), 42);
}

// ── humanize: edge cases ──

#[test]
fn test_humanize_large() {
    use webgraph::utils::humanize;
    assert_eq!(humanize(0.0), "0");
    assert_eq!(humanize(999.0), "999");
    assert_eq!(humanize(1000.0), "1.000K");
    assert_eq!(humanize(1_500_000.0), "1.500M");
    assert_eq!(humanize(1_000_000_000.0), "1.000G");
    assert_eq!(humanize(2_500_000_000_000.0), "2.500T");
}

// ── RaggedArray: additional methods ──

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

// ── SplitIters: construction ──

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

// ── temp_dir utility ──

#[test]
fn test_temp_dir() -> Result<()> {
    use webgraph::utils::temp_dir;
    let base = tempfile::tempdir()?;
    let dir = temp_dir(base.path())?;
    assert!(dir.exists());
    assert!(dir.is_dir());
    // Should be inside base
    assert!(dir.starts_with(base.path()));
    Ok(())
}

// ── ArcListGraph: construction and iteration ──

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

// ── JavaPermutation round-trip ──

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

// ── JavaPermutation: mutable operations ──

#[test]
fn test_java_permutation_mmap_mut() -> Result<()> {
    use mmap_rs::MmapFlags;
    use value_traits::slices::{SliceByValue, SliceByValueMut};
    use webgraph::utils::JavaPermutation;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("perm_mm.bin");

    // Write a file with big-endian u64 values
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for v in [0u64, 0, 0] {
            f.write_all(&v.to_be_bytes())?;
        }
    }

    // mmap_mut it and write values
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

    // Write a file first so we can mmap it
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for v in [0u64, 0] {
            f.write_all(&v.to_be_bytes())?;
        }
    }

    // Read-only variant
    let perm_ro = JavaPermutation::mmap(&path, MmapFlags::empty())?;
    assert_eq!(BitWidth::bit_width(&perm_ro), 64);
    assert_eq!(perm_ro.as_ref().len(), 2);

    // Read-write variant
    let perm_rw = JavaPermutation::mmap_mut(&path, MmapFlags::empty())?;
    assert_eq!(BitWidth::bit_width(&perm_rw), 64);
    assert_eq!(perm_rw.as_ref().len(), 2);

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
//  From test_coverage.rs
// ═══════════════════════════════════════════════════════════════════════

// ── MemoryUsage, humanize, SplitIters ──

#[test]
fn test_memory_usage_from_perc_v2() {
    use webgraph::utils::MemoryUsage;
    let mu = MemoryUsage::from_perc(10.0);
    match mu {
        MemoryUsage::MemorySize(s) => assert!(s > 0),
        _ => panic!("Expected MemorySize variant"),
    }
}

#[test]
fn test_memory_usage_display_v2() {
    use webgraph::utils::MemoryUsage;
    let ms = MemoryUsage::MemorySize(1024);
    assert!(format!("{}", ms).contains("bytes"));
    let bs = MemoryUsage::BatchSize(100);
    assert!(format!("{}", bs).contains("elements"));
}

#[test]
fn test_memory_usage_mul() {
    use webgraph::utils::MemoryUsage;
    let ms = MemoryUsage::MemorySize(1000) * 3;
    assert_eq!(ms.batch_size::<u8>(), 3000);
    let bs = MemoryUsage::BatchSize(100) * 4;
    assert_eq!(bs.batch_size::<u8>(), 400);
}

#[test]
fn test_memory_usage_div() {
    use webgraph::utils::MemoryUsage;
    let ms = MemoryUsage::MemorySize(1000) / 2;
    assert_eq!(ms.batch_size::<u8>(), 500);
    let bs = MemoryUsage::BatchSize(100) / 5;
    assert_eq!(bs.batch_size::<u8>(), 20);
}

#[test]
fn test_humanize_extra() {
    use webgraph::utils::humanize;
    assert_eq!(humanize(0.0), "0");
    assert_eq!(humanize(999.0), "999");
    assert!(humanize(1000.0).contains("K"));
    assert!(humanize(1_000_000.0).contains("M"));
    assert!(humanize(1e9).contains("G"));
    assert!(humanize(1e12).contains("T"));
    assert!(humanize(1e15).contains("P"));
    assert!(humanize(1e18).contains("E"));
}

#[test]
fn test_split_iters_from_tuple() {
    use webgraph::utils::SplitIters;
    let boundaries: Box<[usize]> = vec![0, 3, 5].into_boxed_slice();
    let iters: Box<[Vec<usize>]> = vec![vec![1, 2, 3], vec![4, 5]].into_boxed_slice();
    let si: SplitIters<Vec<usize>> = (boundaries, iters).into();
    assert_eq!(si.boundaries.len(), 3);
    assert_eq!(si.iters.len(), 2);
}

#[test]
fn test_temp_dir_creates_dir() -> Result<()> {
    let base = tempfile::tempdir()?;
    let created = webgraph::utils::temp_dir(base.path())?;
    assert!(created.exists());
    assert!(created.is_dir());
    assert!(created.starts_with(base.path()));
    Ok(())
}

// ── Granularity ──

#[test]
fn test_granularity_arcs_to_node() {
    use webgraph::utils::Granularity;
    let g = Granularity::Arcs(500_u64);
    let ng = g.node_granularity(1000, Some(5000));
    // arcs_per_node = 5, so node_granularity = 500/5 = 100
    assert_eq!(ng, 100);
}

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_arcs_no_arcs_info() {
    use webgraph::utils::Granularity;
    // When num_arcs is None and Arcs variant needs conversion, it panics
    let g = Granularity::Arcs(500_u64);
    let _ng = g.node_granularity(1000, None);
}

#[test]
fn test_granularity_nodes_to_arcs() {
    use webgraph::utils::Granularity;
    let g = Granularity::Nodes(100);
    let ag = g.arc_granularity(1000, Some(5000));
    // arcs_per_node = 5, arc_gran = 100*5 = 500
    assert_eq!(ag, 500);
}

#[test]
fn test_granularity_arcs_to_arcs() {
    use webgraph::utils::Granularity;
    let g = Granularity::Arcs(500_u64);
    let ag = g.arc_granularity(1000, Some(5000));
    assert_eq!(ag, 500);
}

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_nodes_no_arcs_info_v2() {
    use webgraph::utils::Granularity;
    let g = Granularity::Nodes(100);
    let _ag = g.arc_granularity(1000, None);
}

// ── MaskedIter ──

#[test]
fn test_masked_iter_copy_skip_copy() {
    use webgraph::graphs::bvgraph::MaskedIter;
    // blocks: [2, 1, 2] → copy 2, skip 1, copy 2
    let parent = vec![10_usize, 20, 30, 40, 50];
    let iter = MaskedIter::new(parent.into_iter(), vec![2, 1, 2]);
    assert_eq!(iter.len(), 4);
    let result: Vec<_> = iter.collect();
    assert_eq!(result, vec![10, 20, 40, 50]);
}

#[test]
fn test_masked_iter_empty_blocks() {
    use webgraph::graphs::bvgraph::MaskedIter;
    // Empty blocks → copy all
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

// ── JavaPermutation ──

#[test]
fn test_java_permutation_read_write() -> Result<()> {
    use mmap_rs::MmapFlags;
    use value_traits::slices::SliceByValue;
    use webgraph::utils::JavaPermutation;

    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();

    // Write big-endian u64 data to create the permutation file
    // Permutation [4, 3, 2, 1, 0] in big-endian
    let data: Vec<u8> = (0..5u64)
        .map(|i| 4 - i)
        .flat_map(|v| v.to_be_bytes())
        .collect();
    std::fs::write(path, &data)?;

    // Read it back as read-only via mmap
    let perm_ro = JavaPermutation::mmap(path, MmapFlags::empty())?;
    assert_eq!(perm_ro.len(), 5);
    for i in 0..5 {
        assert_eq!(unsafe { perm_ro.get_value_unchecked(i) }, 4 - i);
    }

    // Check bit_width
    use sux::traits::BitWidth;
    assert_eq!(perm_ro.bit_width(), 64);
    Ok(())
}

#[test]
fn test_java_permutation_mmap_mut_roundtrip() -> Result<()> {
    use mmap_rs::MmapFlags;
    use value_traits::slices::{SliceByValue, SliceByValueMut};
    use webgraph::utils::JavaPermutation;

    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();

    // Write big-endian u64 data: [0, 10, 20]
    let data: Vec<u8> = (0..3u64)
        .map(|i| i * 10)
        .flat_map(|v| v.to_be_bytes())
        .collect();
    std::fs::write(path, &data)?;

    // Re-open via mmap_mut
    let mut perm2 = JavaPermutation::mmap_mut(path, MmapFlags::empty())?;
    assert_eq!(perm2.len(), 3);
    for i in 0..3 {
        assert_eq!(unsafe { perm2.get_value_unchecked(i) }, i * 10);
    }

    // Modify via mmap_mut
    unsafe { perm2.set_value_unchecked(1, 42) };
    assert_eq!(unsafe { perm2.get_value_unchecked(1) }, 42);

    use sux::traits::BitWidth;
    assert_eq!(perm2.bit_width(), 64);
    Ok(())
}

// ── MemoryUsage: batch_size with different types ──

#[test]
fn test_memory_usage_batch_size_for_different_types() {
    use webgraph::utils::MemoryUsage;

    let mu = MemoryUsage::MemorySize(1024);
    // u8: 1024 elements
    assert_eq!(mu.batch_size::<u8>(), 1024);
    // u32: 256 elements
    assert_eq!(mu.batch_size::<u32>(), 256);
    // u64: 128 elements
    assert_eq!(mu.batch_size::<u64>(), 128);
    // (usize, usize): depends on platform, but should be > 0
    assert!(mu.batch_size::<(usize, usize)>() > 0);
}

#[test]
fn test_memory_usage_batch_size_batch_variant() {
    use webgraph::utils::MemoryUsage;

    let mu = MemoryUsage::BatchSize(42);
    // BatchSize variant ignores type size
    assert_eq!(mu.batch_size::<u8>(), 42);
    assert_eq!(mu.batch_size::<u64>(), 42);
    assert_eq!(mu.batch_size::<(usize, usize)>(), 42);
}

#[test]
fn test_memory_usage_default() {
    use webgraph::utils::MemoryUsage;

    let mu = MemoryUsage::default();
    // Default is 50% of physical RAM; should be MemorySize variant
    match mu {
        MemoryUsage::MemorySize(size) => assert!(size > 0),
        _ => panic!("Expected MemorySize variant for default"),
    }
}

// ── humanize ──

#[test]
fn test_humanize_v2() {
    use webgraph::utils::humanize;

    // Small numbers (no prefix)
    assert_eq!(humanize(0.0), "0");
    assert_eq!(humanize(42.0), "42");
    assert_eq!(humanize(999.0), "999");

    // Thousands
    assert_eq!(humanize(1000.0), "1.000K");
    assert_eq!(humanize(1234.0), "1.234K");

    // Millions
    assert_eq!(humanize(1_000_000.0), "1.000M");
    assert_eq!(humanize(1_500_000.0), "1.500M");

    // Billions
    assert_eq!(humanize(1_000_000_000.0), "1.000G");

    // Trillions
    assert_eq!(humanize(1_000_000_000_000.0), "1.000T");
}

// ── Granularity: node_granularity ──

#[test]
fn test_granularity_node_granularity() {
    use webgraph::utils::Granularity;

    let g = Granularity::Nodes(100);
    assert_eq!(g.node_granularity(1000, Some(5000u64)), 100);

    let g2 = Granularity::Arcs(500u64);
    let ng = g2.node_granularity(1000, Some(5000u64));
    // Arcs(500) with 5000 arcs / 1000 nodes = avg 5 arcs/node, so ~100 nodes
    assert!(ng > 0);
}

// ── par_node_apply ──

#[test]
fn test_par_node_apply_v2() -> Result<()> {
    use dsi_progress_logger::concurrent_progress_logger;
    use webgraph::traits::SequentialLabeling;
    use webgraph::utils::Granularity;

    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);

    let mut pl = concurrent_progress_logger![item_name = "node"];
    let total_outdegree: usize = graph.par_node_apply(
        |range| range.map(|node| graph.outdegree(node)).sum::<usize>(),
        |a, b| a + b,
        Granularity::Nodes(2),
        &mut pl,
    );

    assert_eq!(total_outdegree, 4);
    Ok(())
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

// ── MemoryUsage Display ──

#[test]
fn test_memory_usage_display_format() {
    use webgraph::utils::MemoryUsage;
    let m1 = MemoryUsage::MemorySize(1024);
    let s1 = format!("{}", m1);
    assert!(s1.contains("1024"));
    assert!(s1.contains("bytes"));

    let m2 = MemoryUsage::BatchSize(500);
    let s2 = format!("{}", m2);
    assert!(s2.contains("500"));
    assert!(s2.contains("elements"));
}

// ── MemoryUsage: Mul, Div ──

#[test]
fn test_memory_usage_mul_div_v2() {
    use webgraph::utils::MemoryUsage;

    let mu = MemoryUsage::MemorySize(1000);
    let doubled = mu * 2;
    match doubled {
        MemoryUsage::MemorySize(s) => assert_eq!(s, 2000),
        _ => panic!("Expected MemorySize"),
    }

    let halved = mu / 2;
    match halved {
        MemoryUsage::MemorySize(s) => assert_eq!(s, 500),
        _ => panic!("Expected MemorySize"),
    }

    // BatchSize variant
    let mu2 = MemoryUsage::BatchSize(100);
    let tripled = mu2 * 3;
    match tripled {
        MemoryUsage::BatchSize(s) => assert_eq!(s, 300),
        _ => panic!("Expected BatchSize"),
    }

    let quartered = mu2 / 4;
    match quartered {
        MemoryUsage::BatchSize(s) => assert_eq!(s, 25),
        _ => panic!("Expected BatchSize"),
    }
}

#[test]
fn test_memory_usage_mul_div_ops() {
    use webgraph::utils::MemoryUsage;

    let m1 = MemoryUsage::MemorySize(1000);
    let m2 = m1 * 3;
    assert_eq!(format!("{}", m2), "3000 bytes");
    let m3 = m1 / 2;
    assert_eq!(format!("{}", m3), "500 bytes");

    let b1 = MemoryUsage::BatchSize(100);
    let b2 = b1 * 5;
    assert_eq!(format!("{}", b2), "500 elements");
    let b3 = b1 / 4;
    assert_eq!(format!("{}", b3), "25 elements");
}

// ── MemoryUsage batch_size ──

#[test]
fn test_memory_usage_batch_size_v2() {
    use webgraph::utils::MemoryUsage;

    // MemorySize variant divides by element size
    let m = MemoryUsage::MemorySize(1024);
    let bs = m.batch_size::<(usize, usize)>();
    assert_eq!(bs, 1024 / (2 * std::mem::size_of::<usize>()));

    // BatchSize variant returns the value directly
    let b = MemoryUsage::BatchSize(42);
    assert_eq!(b.batch_size::<u8>(), 42);
}
