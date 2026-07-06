/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for BvGraph access patterns: endianness, properties parsing,
//! sequential and random access loading, static dispatch, offsets, and split labeling.

mod common;

use anyhow::Result;
use common::build_ef;
use dsi_bitstream::prelude::*;
use lender::*;
use webgraph::prelude::*;

#[test]
fn test_get_endianness_be() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let e = webgraph::graphs::bvgraph::get_endianness(path)?;
    assert_eq!(e, "big");
    Ok(())
}

#[test]
fn test_get_endianness_le() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<LE>(&graph)?;
    let e = webgraph::graphs::bvgraph::get_endianness(path)?;
    assert_eq!(e, "little");
    Ok(())
}

#[test]
fn test_parse_properties() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    // parse_properties takes the full path to the .properties file
    let props_path = basename.with_extension("properties");
    let (num_nodes, num_arcs, flags) =
        webgraph::graphs::bvgraph::parse_properties::<BE>(&props_path)?;
    assert_eq!(num_nodes, 3);
    assert_eq!(num_arcs, 2);
    assert_eq!(
        flags.compression_window,
        CompFlags::default().compression_window
    );
    Ok(())
}

#[test]
fn test_bvgraph_seq_load_mem() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    let mut count = 0;
    let mut iter = seq.iter();
    while let Some((_node, succ)) = iter.next() {
        count += succ.count();
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_bvgraph_seq_load_mmap() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path)
        .endianness::<BE>()
        .mode::<LoadMmap>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    let mut count = 0;
    let mut iter = seq.iter();
    while let Some((_node, succ)) = iter.next() {
        count += succ.count();
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn test_static_dispatch_seq_load_default_codes() -> Result<()> {
    // Exercises ConstCodesDecoderFactory and ConstCodesDecoder via Static dispatch
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    // Compress with default flags (Gamma/Unary/Gamma/Gamma/Zeta3)
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    // Load with Static dispatch — exercises ConstCodesDecoderFactory::new and
    // ConstCodesDecoder Decode impl
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;
    assert_eq!(seq.num_nodes(), 4);
    assert_eq!(seq.num_arcs_hint(), Some(5));
    // Iterate to exercise all ConstCodesDecoder::read_* methods
    let mut total_arcs = 0;
    let mut iter = seq.iter();
    while let Some((_node, succ)) = iter.next() {
        total_arcs += succ.count();
    }
    assert_eq!(total_arcs, 5);
    Ok(())
}

#[test]
fn test_static_dispatch_seq_load_le() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<LE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<LE>()
        .mode::<LoadMem>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    let mut total_arcs = 0;
    let mut iter = seq.iter();
    while let Some((_node, succ)) = iter.next() {
        total_arcs += succ.count();
    }
    assert_eq!(total_arcs, 3);
    Ok(())
}

#[test]
fn test_static_dispatch_with_load_mem() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    let mut total_arcs = 0;
    let mut iter = seq.iter();
    while let Some((_node, succ)) = iter.next() {
        total_arcs += succ.count();
    }
    assert_eq!(total_arcs, 3);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_static_dispatch_with_load_mmap() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMmap>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    let mut total_arcs = 0;
    let mut iter = seq.iter();
    while let Some((_node, succ)) = iter.next() {
        total_arcs += succ.count();
    }
    assert_eq!(total_arcs, 2);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_static_dispatch_large_graph_with_all_codec_paths() -> Result<()> {
    // Build a graph that exercises reference compression, intervals, blocks, and residuals
    let mut arcs = Vec::new();
    // Consecutive successors trigger intervals
    for i in 10..25 {
        arcs.push((0_usize, i));
    }
    // Overlapping successors trigger reference + blocks
    for i in 12..27 {
        arcs.push((1, i));
    }
    // Pure residuals (non-consecutive, no reference)
    arcs.push((2, 5));
    arcs.push((2, 15));
    arcs.push((2, 25));
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .comp_flags(CompFlags {
            min_interval_length: 4,
            compression_window: 7,
            max_ref_count: 3,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;
    // Verify all arcs are correctly read via static dispatch
    let mut total_arcs = 0;
    let mut iter = seq.iter();
    while let Some((_node, succ)) = iter.next() {
        total_arcs += succ.count();
    }
    assert_eq!(total_arcs as u64, seq.num_arcs_hint().unwrap());
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_static_dispatch_verify_same_as_dynamic() -> Result<()> {
    // Build a graph and verify static and dynamic dispatch produce same results
    let mut arcs = Vec::new();
    for i in 0..30 {
        for j in 0..5 {
            arcs.push((i, (i + j + 1) % 35));
        }
    }
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .comp_flags(CompFlags {
            min_interval_length: 4,
            compression_window: 7,
            max_ref_count: 3,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;

    // Load with dynamic dispatch
    let seq_dyn = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;

    // Load with static dispatch
    let seq_static = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;

    assert_eq!(seq_dyn.num_nodes(), seq_static.num_nodes());
    assert_eq!(seq_dyn.num_arcs_hint(), seq_static.num_arcs_hint());

    // Compare all successor lists
    let mut iter_dyn = seq_dyn.iter();
    let mut iter_static = seq_static.iter();
    while let (Some((n_d, s_d)), Some((n_s, s_s))) = (iter_dyn.next(), iter_static.next()) {
        assert_eq!(n_d, n_s);
        let succs_d: Vec<_> = s_d.into_iter().collect();
        let succs_s: Vec<_> = s_s.into_iter().collect();
        assert_eq!(succs_d, succs_s);
    }
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_bvgraph_random_access_all_paths() -> Result<()> {
    // Build a graph that exercises all decompression paths:
    // intervals, blocks, residuals, references
    let mut arcs = Vec::new();

    // Node 0: many consecutive successors (triggers intervals)
    for i in 1..20 {
        arcs.push((0_usize, i));
    }
    // Node 1: overlaps with node 0 successors (triggers reference + blocks)
    for i in 5..25 {
        arcs.push((1, i));
    }
    // Node 2: sparse successors (triggers residuals)
    arcs.push((2, 3));
    arcs.push((2, 10));
    arcs.push((2, 20));
    arcs.push((2, 30));
    // Node 3: also overlaps with node 2 (reference with blocks)
    arcs.push((3, 3));
    arcs.push((3, 11));
    arcs.push((3, 20));
    arcs.push((3, 31));

    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs.clone());
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .comp_flags(CompFlags {
            min_interval_length: 4,
            compression_window: 7,
            max_ref_count: 3,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;

    // Build Elias–Fano for random access
    build_ef(&basename)?;

    let ra = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;

    // Verify random access matches sequential
    for node in 0..ra.num_nodes() {
        let ra_succs: Vec<_> = ra.successors(node).collect();
        let expected: Vec<_> = arcs
            .iter()
            .filter(|(s, _)| *s == node)
            .map(|(_, d)| *d)
            .collect();
        assert_eq!(ra_succs, expected, "Node {node} mismatch");
    }

    // Verify outdegree
    assert_eq!(ra.outdegree(0), 19);
    assert_eq!(ra.outdegree(2), 4);

    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_bvgraph_random_access_static_dispatch() -> Result<()> {
    let mut arcs = Vec::new();
    for i in 1..15 {
        arcs.push((0_usize, i));
    }
    for i in 5..20 {
        arcs.push((1, i));
    }
    arcs.push((2, 3));
    arcs.push((2, 10));
    arcs.push((2, 25));

    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .comp_flags(CompFlags {
            min_interval_length: 4,
            compression_window: 7,
            max_ref_count: 3,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;

    // Build offsets
    build_ef(&basename)?;

    // Load with static dispatch for random access
    let ra = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;

    assert_eq!(ra.outdegree(0), 14);
    assert_eq!(ra.outdegree(1), 15);
    assert_eq!(ra.outdegree(2), 3);

    // Verify successors
    let succs: Vec<_> = ra.successors(0).collect();
    assert_eq!(succs.len(), 14);
    assert_eq!(succs[0], 1);
    assert_eq!(*succs.last().unwrap(), 14);

    Ok(())
}

#[test]
fn test_bvgraph_le_random_access() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<LE>(&graph)?;

    // Build offsets for random access
    build_ef(&basename)?;

    let ra = BvGraph::with_basename(&basename)
        .endianness::<LE>()
        .mode::<LoadMem>()
        .load()?;

    assert_eq!(ra.num_nodes(), 4);
    assert_eq!(ra.outdegree(0), 2);
    assert_eq!(ra.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(ra.successors(3).collect::<Vec<_>>(), vec![0]);
    Ok(())
}

#[test]
fn test_bvgraph_load_mem_mode() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;

    build_ef(&basename)?;

    let ra = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;

    assert_eq!(ra.num_nodes(), 3);
    assert_eq!(ra.successors(0).collect::<Vec<_>>(), vec![1]);
    Ok(())
}

#[test]
fn test_bvgraph_file_mode() -> Result<()> {
    use webgraph::graphs::bvgraph::File;

    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;

    build_ef(&basename)?;

    let ra = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .mode::<File>()
        .load()?;

    assert_eq!(ra.num_nodes(), 3);
    assert_eq!(ra.successors(0).collect::<Vec<_>>(), vec![1]);
    Ok(())
}

#[test]
fn test_check_offsets_with_static_dispatch() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;
    assert!(webgraph::graphs::bvgraph::check_offsets(&seq, &basename)?);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_split_labeling_bvgraph() -> Result<()> {
    use webgraph::traits::SplitLabeling;

    let basename = common::cnr_2000_basename();
    let graph = BvGraph::with_basename(&basename).load()?;

    // Test that split labeling works
    let how_many = 4;
    let lenders: Vec<_> = graph.split_iter(how_many).collect();
    assert!(lenders.len() <= how_many);

    // Verify we can iterate each split
    let mut total_nodes = 0;
    for lender in lenders {
        let mut count = 0;
        for_!((_node, _succs) in lender {
            count += 1;
        });
        total_nodes += count;
    }
    assert_eq!(total_nodes, graph.num_nodes());
    Ok(())
}

#[test]
fn test_offset_deg_iter() -> Result<()> {
    // Compress and load, then verify check_offsets
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (0, 3), (1, 2), (2, 3)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;
    assert!(webgraph::graphs::bvgraph::check_offsets(&seq, path)?);
    Ok(())
}

#[test]
fn test_iter_from_middle() -> Result<()> {
    use webgraph::traits::SequentialLabeling;

    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);

    let mut count = 0;
    for_!((node, _succs) in graph.iter_from(2) {
        count += 1;
        assert!(node >= 2);
    });
    assert_eq!(count, 3); // nodes 2, 3, 4
    Ok(())
}

#[test]
fn test_offsets_writer() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test.offsets");
    let mut writer = OffsetsWriter::from_path(&path, true)?;
    writer.push(10)?;
    writer.push(20)?;
    writer.push(30)?;
    writer.flush()?;

    // Read back and verify
    let mut reader: BufBitReader<BE, _> =
        BufBitReader::new(WordAdapter::<u32, _>::new(std::fs::File::open(&path)?));
    assert_eq!(reader.read_gamma()?, 0); // first zero
    assert_eq!(reader.read_gamma()?, 10);
    assert_eq!(reader.read_gamma()?, 20);
    assert_eq!(reader.read_gamma()?, 30);
    Ok(())
}

#[test]
fn test_offsets_writer_no_zero() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test_no_zero.offsets");
    let mut writer = OffsetsWriter::from_path(&path, false)?;
    writer.push(5)?;
    writer.push(10)?;
    writer.flush()?;
    drop(writer);

    // Read back: no leading zero, just the pushed gamma values
    let mut reader: BufBitReader<BE, _> =
        BufBitReader::new(WordAdapter::<u32, _>::new(std::fs::File::open(&path)?));
    assert_eq!(reader.read_gamma()?, 5);
    assert_eq!(reader.read_gamma()?, 10);
    Ok(())
}

#[test]
fn test_static_dispatch_non_default_codes() -> Result<()> {
    use dsi_bitstream::dispatch::code_consts;
    // Nontrivial degrees and residual gaps so that delta- and gamma-coded
    // streams differ. Regression: the `Decoder` aliases of
    // `ConstCodesDecoderFactory` dropped the const code parameters, so
    // decoding silently fell back to the default codes.
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([
        (0, 1),
        (0, 5),
        (0, 12),
        (0, 30),
        (1, 0),
        (2, 30),
    ]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .comp_flags(CompFlags {
            outdegrees: Codes::Delta,
            references: Codes::Delta,
            blocks: Codes::Delta,
            intervals: Codes::Delta,
            residuals: Codes::Delta,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;

    type AllDelta = webgraph::graphs::bvgraph::Static<
        { code_consts::DELTA },
        { code_consts::DELTA },
        { code_consts::DELTA },
        { code_consts::DELTA },
        { code_consts::DELTA },
    >;

    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<AllDelta>()
        .load()?;
    webgraph::traits::labels::eq_sorted(&graph, &seq)?;

    build_ef(&basename)?;
    let ra = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<AllDelta>()
        .load()?;
    assert_eq!(ra.outdegree(0), 4);
    assert_eq!(ra.successors(0).collect::<Vec<_>>(), vec![1, 5, 12, 30]);
    assert_eq!(ra.successors(2).collect::<Vec<_>>(), vec![30]);
    Ok(())
}

#[test]
fn test_stale_ef_rejected_at_load() -> Result<()> {
    // Regression: the random-access decoders index the Elias-Fano offsets
    // without bounds checks, and a stale .ef (built for a different graph)
    // was accepted at load time.
    let small = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1)]);
    let large = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let tmp = tempfile::tempdir()?;
    let small_base = tmp.path().join("small");
    let large_base = tmp.path().join("large");
    BvComp::with_basename(&small_base).comp_graph::<BE>(&small)?;
    BvComp::with_basename(&large_base).comp_graph::<BE>(&large)?;
    build_ef(&small_base)?;
    // Use the .ef of the smaller graph for the larger one. read + write
    // instead of fs::copy, whose copy_file_range is unsupported under Miri.
    std::fs::write(
        large_base.with_extension("ef"),
        std::fs::read(small_base.with_extension("ef"))?,
    )?;
    let res = BvGraph::with_basename(&large_base)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load();
    assert!(res.is_err(), "expected stale-offsets error, got Ok");
    // Same validation on the static-dispatch load path.
    let res = BvGraph::with_basename(&large_base)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load();
    assert!(
        res.is_err(),
        "expected stale-offsets error on static dispatch, got Ok"
    );
    Ok(())
}
