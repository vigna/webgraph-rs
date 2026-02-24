/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

mod common;

use anyhow::Result;
use common::build_ef;
use dsi_bitstream::prelude::*;
use webgraph::prelude::*;

#[test]
fn test_bvcomp_default_codes_be() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 4);
    assert_eq!(seq.num_arcs_hint(), Some(5));
    labels::eq_sorted(&graph, &seq)?;
    webgraph::graphs::bvgraph::check_offsets(&seq, path)?;
    Ok(())
}

#[test]
fn test_bvcomp_default_codes_le() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<LE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<LE>().load()?;
    assert_eq!(seq.num_nodes(), 3);
    labels::eq_sorted(&graph, &seq)?;
    webgraph::graphs::bvgraph::check_offsets(&seq, path)?;
    Ok(())
}

#[test]
fn test_bvcomp_delta_codes() -> Result<()> {
    use dsi_bitstream::dispatch::Codes;
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 5), (0, 10), (1, 2), (2, 3)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    let flags = CompFlags {
        outdegrees: Codes::Delta,
        references: Codes::Gamma,
        blocks: Codes::Delta,
        intervals: Codes::Delta,
        residuals: Codes::Delta,
        ..CompFlags::default()
    };
    BvComp::with_basename(path)
        .with_comp_flags(flags)
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 11);
    assert_eq!(seq.num_arcs_hint(), Some(5));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_zeta_codes() -> Result<()> {
    use dsi_bitstream::dispatch::Codes;
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 5), (1, 2), (2, 3)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    // All zeta codes must use the same k value
    let flags = CompFlags {
        outdegrees: Codes::Zeta(3),
        references: Codes::Zeta(3),
        blocks: Codes::Zeta(3),
        intervals: Codes::Zeta(3),
        residuals: Codes::Zeta(3),
        ..CompFlags::default()
    };
    BvComp::with_basename(path)
        .with_comp_flags(flags)
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 6);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_empty_graph() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::empty(5);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 5);
    assert_eq!(seq.num_arcs_hint(), Some(0));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_no_reference_compression() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    let flags = CompFlags {
        compression_window: 0,
        ..CompFlags::default()
    };
    BvComp::with_basename(path)
        .with_comp_flags(flags)
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 4);
    assert_eq!(seq.num_arcs_hint(), Some(5));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_no_intervals() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    let flags = CompFlags {
        min_interval_length: 0,
        ..CompFlags::default()
    };
    BvComp::with_basename(path)
        .with_comp_flags(flags)
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 3);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_interval_encoding() -> Result<()> {
    // Long consecutive ranges trigger interval encoding
    let mut arcs = Vec::new();
    for i in 10..30 {
        arcs.push((0_usize, i));
    }
    arcs.push((1, 5));
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path)
        .with_comp_flags(CompFlags {
            min_interval_length: 4,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 30);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_large_window() -> Result<()> {
    // Exercise reference compression with large window and shared successors
    let mut arcs = Vec::new();
    for i in 0..20 {
        for j in 0..5 {
            arcs.push((i, (i + j + 1) % 25));
        }
    }
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path)
        .with_comp_flags(CompFlags {
            compression_window: 15,
            max_ref_count: 10,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 25);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_par_comp() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).par_comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 4);
    assert_eq!(seq.num_arcs_hint(), Some(5));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_bvcompz() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvCompZ::with_basename(path)
        .with_chunk_size(2)
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 4);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_config_with_tmp_dir() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("test_graph");
    let custom_tmp = tempfile::tempdir()?;
    BvComp::with_basename(&basename)
        .with_tmp_dir(custom_tmp.path())
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_recompress_with_different_flags() -> Result<()> {
    use dsi_bitstream::dispatch::Codes;
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3)]);
    let tmp = tempfile::tempdir()?;
    let basename1 = tmp.path().join("test_orig");
    BvComp::with_basename(&basename1).comp_graph::<BE>(&graph)?;

    // Load and recompress with different flags
    let seq = BvGraphSeq::with_basename(&basename1)
        .endianness::<BE>()
        .load()?;
    let basename2 = tmp.path().join("test_recomp");
    BvComp::with_basename(&basename2)
        .with_comp_flags(CompFlags {
            outdegrees: Codes::Delta,
            residuals: Codes::Delta,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&seq)?;

    let seq2 = BvGraphSeq::with_basename(&basename2)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq2.num_nodes(), 4);
    assert_eq!(seq2.num_arcs_hint(), Some(3));
    labels::eq_sorted(&graph, &seq2)?;
    Ok(())
}

#[test]
fn test_bvcomp_par_comp_lenders() -> Result<()> {
    // Test parallel compression with multiple lenders via SplitLabeling
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("test_par");
    // First compress to get a splittable graph
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    // Now use par_comp_graph which calls par_comp_lenders internally
    let basename2 = tmp.path().join("test_par2");
    BvComp::with_basename(&basename2).par_comp_graph::<BE>(&seq)?;
    let seq2 = BvGraphSeq::with_basename(&basename2)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq2.num_nodes(), 4);
    assert_eq!(seq2.num_arcs_hint(), Some(5));
    labels::eq_sorted(&graph, &seq2)?;
    Ok(())
}

#[test]
fn test_bvcomp_with_chunk_size() -> Result<()> {
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("test_chunk");
    BvComp::with_basename(&basename)
        .with_chunk_size(1)
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_large_graph_with_reference_compression() -> Result<()> {
    // Build a graph with similar successor lists to trigger reference compression
    let mut arcs = Vec::new();
    for i in 0..50 {
        for j in 0..10 {
            arcs.push((i, (i + j + 1) % 55));
        }
    }
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path)
        .with_comp_flags(CompFlags {
            compression_window: 10,
            max_ref_count: 5,
            min_interval_length: 3,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 55);
    labels::eq_sorted(&graph, &seq)?;
    webgraph::graphs::bvgraph::check_offsets(&seq, path)?;
    Ok(())
}

#[test]
fn test_bvcomp_pi_codes() -> Result<()> {
    use dsi_bitstream::dispatch::Codes;
    let mut arcs = Vec::new();
    for i in 0..20 {
        arcs.push((i, (i + 1) % 20));
    }
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    let flags = CompFlags {
        outdegrees: Codes::Pi(2),
        references: Codes::Pi(1),
        blocks: Codes::Pi(3),
        intervals: Codes::Pi(4),
        residuals: Codes::Pi(2),
        ..CompFlags::default()
    };
    BvComp::with_basename(path)
        .with_comp_flags(flags)
        .comp_graph::<LE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<LE>().load()?;
    assert_eq!(seq.num_nodes(), 20);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_unary_codes() -> Result<()> {
    use dsi_bitstream::dispatch::Codes;
    // Small values work well with unary codes
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    let flags = CompFlags {
        outdegrees: Codes::Unary,
        references: Codes::Unary,
        blocks: Codes::Unary,
        intervals: Codes::Unary,
        residuals: Codes::Unary,
        ..CompFlags::default()
    };
    BvComp::with_basename(path)
        .with_comp_flags(flags)
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    assert_eq!(seq.num_nodes(), 4);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_dense_graph() -> Result<()> {
    // Dense graph: every node connects to every other node
    let mut arcs = Vec::new();
    for i in 0..10_usize {
        for j in 0..10_usize {
            if i != j {
                arcs.push((i, j));
            }
        }
    }
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 10);
    assert_eq!(seq.num_arcs_hint(), Some(90));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_chain_graph() -> Result<()> {
    // Long chain 0->1->2->...->99
    let arcs: Vec<_> = (0..100_usize).map(|i| (i, i + 1)).collect();
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 101);
    assert_eq!(seq.num_arcs_hint(), Some(100));
    labels::eq_sorted(&graph, &seq)?;
    // Also test with static dispatch
    let seq_static = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .dispatch::<webgraph::graphs::bvgraph::Static>()
        .load()?;
    assert_eq!(seq_static.num_nodes(), 101);
    labels::eq_sorted(&graph, &seq_static)?;
    Ok(())
}

#[test]
fn test_bvcomp_star_graph() -> Result<()> {
    // Star graph: node 0 connects to all others
    let arcs: Vec<_> = (1..50_usize).map(|i| (0, i)).collect();
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .with_comp_flags(CompFlags {
            min_interval_length: 4,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 50);
    assert_eq!(seq.num_arcs_hint(), Some(49));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_with_delta_codes() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .with_comp_flags(CompFlags {
            outdegrees: dsi_bitstream::dispatch::Codes::Delta,
            references: dsi_bitstream::dispatch::Codes::Delta,
            blocks: dsi_bitstream::dispatch::Codes::Delta,
            intervals: dsi_bitstream::dispatch::Codes::Delta,
            residuals: dsi_bitstream::dispatch::Codes::Delta,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 4);
    assert_eq!(seq.num_arcs_hint(), Some(5));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_with_zeta_codes() -> Result<()> {
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 5), (1, 3), (2, 7), (3, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .with_comp_flags(CompFlags {
            outdegrees: dsi_bitstream::dispatch::Codes::Zeta(5),
            residuals: dsi_bitstream::dispatch::Codes::Zeta(5),
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 8);
    assert_eq!(seq.num_arcs_hint(), Some(5));
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_with_no_intervals() -> Result<()> {
    let mut arcs = Vec::new();
    for i in 0..20 {
        for j in i + 1..20 {
            arcs.push((i, j));
        }
    }
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .with_comp_flags(CompFlags {
            min_interval_length: 0, // disable intervals
            compression_window: 7,
            max_ref_count: 3,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 20);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_with_no_references() -> Result<()> {
    let mut arcs = Vec::new();
    for i in 0..10 {
        for j in i + 1..i + 5 {
            arcs.push((i, j));
        }
    }
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs(arcs);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .with_comp_flags(CompFlags {
            compression_window: 0, // disable references
            min_interval_length: 4,
            ..CompFlags::default()
        })
        .comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 14);
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}

#[test]
fn test_bvcomp_config_basic() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("test_config");
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);

    let bits_written = BvCompConfig::new(&basename).comp_graph::<BE>(&graph)?;
    assert!(bits_written > 0);

    // Verify the graph was written correctly (use sequential access, no EF needed)
    let loaded = webgraph::graphs::bvgraph::BvGraphSeq::with_basename(&basename).load()?;
    assert_eq!(loaded.num_nodes(), 4);
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}

#[test]
fn test_bvcomp_config_with_flags() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("test_flags");
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);

    let flags = CompFlags {
        min_interval_length: 0,
        ..CompFlags::default()
    };

    let bits_written = BvCompConfig::new(&basename)
        .with_comp_flags(flags)
        .comp_graph::<BE>(&graph)?;
    assert!(bits_written > 0);
    let loaded = webgraph::graphs::bvgraph::BvGraphSeq::with_basename(&basename).load()?;
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}

#[test]
fn test_bvcomp_config_with_explicit_tmp_dir() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let tmp = tempfile::tempdir()?;
    let basename = dir.path().join("test_tmpdir");
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);

    let bits_written = BvCompConfig::new(&basename)
        .with_tmp_dir(tmp.path())
        .comp_graph::<BE>(&graph)?;
    assert!(bits_written > 0);
    let loaded = webgraph::graphs::bvgraph::BvGraphSeq::with_basename(&basename).load()?;
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}

#[test]
fn test_par_comp_graph() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("test_par");
    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3), (3, 4)]);

    let bits_written = BvCompConfig::new(&basename).par_comp_graph::<BE>(&graph)?;
    assert!(bits_written > 0);

    // Build EF for loaded graph
    build_ef(&basename)?;
    let loaded = BvGraph::with_basename(&basename).load()?;
    assert_eq!(loaded.num_nodes(), 5);
    assert_eq!(loaded.num_arcs(), 5);
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}

#[test]
fn test_par_comp_lenders() -> Result<()> {
    use std::num::NonZeroUsize;
    use webgraph::traits::SequentialLabeling;
    use webgraph::utils::{MemoryUsage, ParSortPairs};

    let graph =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (0, 3)]);
    let num_nodes = graph.num_nodes();

    // Sort pairs in parallel
    let pairs: Vec<(usize, usize)> = vec![(0, 1), (1, 2), (2, 3), (3, 0), (0, 3)];
    let sorter = ParSortPairs::new(num_nodes)?
        .num_partitions(NonZeroUsize::new(2).unwrap())
        .memory_usage(MemoryUsage::BatchSize(100));
    use rayon::prelude::*;
    let split = sorter.sort(pairs.into_par_iter())?;

    // Convert to lenders
    let lenders: Vec<LeftIterator<_>> = split.into();

    // Compress with par_comp_lenders
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("par_lenders");
    let bits = BvCompConfig::new(&basename).par_comp_lenders::<BE, _>(lenders, num_nodes)?;
    assert!(bits > 0);

    // Build EF and load
    build_ef(&basename)?;
    let loaded = BvGraph::with_basename(&basename).load()?;
    assert_eq!(loaded.num_nodes(), num_nodes);
    assert_eq!(loaded.num_arcs(), 5);
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}

#[test]
fn test_comp_lender() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("test_lender");
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);

    let bits_written = BvCompConfig::new(&basename).comp_lender::<BE, _>(graph.iter(), Some(3))?;
    assert!(bits_written > 0);

    // Verify with sequential access (no EF needed)
    let loaded = webgraph::graphs::bvgraph::BvGraphSeq::with_basename(&basename).load()?;
    assert_eq!(loaded.num_nodes(), 3);
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}

#[test]
fn test_bvcomp_le_endianness() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("test_le");
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);

    // Compress with LE endianness
    let bits_written = BvCompConfig::new(&basename).comp_graph::<LE>(&graph)?;
    assert!(bits_written > 0);

    // Load with sequential access (no EF needed)
    let loaded = webgraph::graphs::bvgraph::BvGraphSeq::with_basename(&basename)
        .endianness::<LE>()
        .load()?;
    assert_eq!(loaded.num_nodes(), 4);
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}

#[test]
fn test_bvcomp_with_custom_comp_flags() -> Result<()> {
    use dsi_bitstream::dispatch::Codes;
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("custom_flags");
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (0, 2)]);

    let flags = CompFlags {
        outdegrees: Codes::Delta,
        references: Codes::Delta,
        blocks: Codes::Delta,
        intervals: Codes::Delta,
        residuals: Codes::Delta,
        min_interval_length: 2,
        compression_window: 7,
        max_ref_count: 3,
    };

    let bits = BvCompConfig::new(&basename)
        .with_comp_flags(flags)
        .comp_graph::<BE>(&graph)?;
    assert!(bits > 0);

    // Load and verify
    let loaded = webgraph::graphs::bvgraph::BvGraphSeq::with_basename(&basename).load()?;
    assert_eq!(loaded.num_nodes(), 3);
    labels::eq_sorted(&graph, &loaded)?;
    Ok(())
}
