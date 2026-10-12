/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod common;

use anyhow::{Context, Result};
use common::{Roundtrip, roundtrip, test_graph, tmp_basename};
use dsi_bitstream::dispatch::Codes;
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use webgraph::graphs::bvgraph::check_offsets;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;

/// Graphs exercising intervals, references, gaps, and residuals.
fn graphs() -> Vec<(&'static str, VecGraph)> {
    let mut graphs = vec![
        ("test", test_graph()),
        ("empty", VecGraph::empty(5)),
        (
            "gaps",
            VecGraph::from_arcs([(0, 1), (0, 5), (0, 10), (1, 2), (2, 3), (3, 0), (2, 7)]),
        ),
        (
            "intervals",
            VecGraph::from_arcs((10..30).map(|i| (0, i)).chain([(1, 5)])),
        ),
        (
            "similar lists",
            VecGraph::from_arcs((0..50).flat_map(|i| (0..10).map(move |j| (i, (i + j + 1) % 55)))),
        ),
        (
            "dense",
            VecGraph::from_arcs(
                (0..10).flat_map(|i| (0..10).filter(move |&j| j != i).map(move |j| (i, j))),
            ),
        ),
        (
            "upper triangle",
            VecGraph::from_arcs((0..20).flat_map(|i| (i + 1..20).map(move |j| (i, j)))),
        ),
        ("chain", VecGraph::from_arcs((0..100).map(|i| (i, i + 1)))),
        ("star", VecGraph::from_arcs((1..50).map(|i| (0, i)))),
    ];
    if cfg!(miri) {
        graphs.truncate(1);
    }
    graphs
}

/// Compression flags exercising all codes and the main parameters.
fn comp_flags() -> Vec<(&'static str, CompFlags)> {
    let all = |code| CompFlags {
        outdegrees: code,
        references: code,
        blocks: code,
        intervals: code,
        residuals: code,
        ..CompFlags::default()
    };
    let mut flags = vec![
        ("default", CompFlags::default()),
        (
            "delta",
            CompFlags {
                references: Codes::Gamma,
                ..all(Codes::Delta)
            },
        ),
        // All ζ codes must use the same k
        ("zeta(3)", all(Codes::Zeta(3))),
        (
            "zeta(5)",
            CompFlags {
                outdegrees: Codes::Zeta(5),
                residuals: Codes::Zeta(5),
                ..CompFlags::default()
            },
        ),
        (
            "pi",
            CompFlags {
                outdegrees: Codes::Pi(2),
                references: Codes::Pi(1),
                blocks: Codes::Pi(3),
                intervals: Codes::Pi(4),
                residuals: Codes::Pi(2),
                ..CompFlags::default()
            },
        ),
        ("unary", all(Codes::Unary)),
        (
            "custom delta",
            CompFlags {
                min_interval_length: 2,
                compression_window: 7,
                max_ref_count: 3,
                ..all(Codes::Delta)
            },
        ),
        (
            "no intervals",
            CompFlags {
                min_interval_length: 0,
                compression_window: 7,
                max_ref_count: 3,
                ..CompFlags::default()
            },
        ),
        (
            "no references",
            CompFlags {
                compression_window: 0,
                min_interval_length: 4,
                ..CompFlags::default()
            },
        ),
        (
            "large window",
            CompFlags {
                compression_window: 15,
                max_ref_count: 10,
                ..CompFlags::default()
            },
        ),
        (
            "window 10",
            CompFlags {
                compression_window: 10,
                max_ref_count: 5,
                min_interval_length: 3,
                ..CompFlags::default()
            },
        ),
    ];
    if cfg!(miri) {
        flags.truncate(3);
    }
    flags
}

/// Checks the roundtrip of all graphs with all compression flags, using
/// sequential and parallel compression with both compressors.
fn test_roundtrip<E: Roundtrip>() -> Result<()>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    BufBitReader<E, WordAdapter<u32, BufReader<File>>>: BitRead<E>,
{
    for (graph_name, graph) in graphs() {
        for (flags_name, flags) in comp_flags() {
            let context = || format!("graph {graph_name}, flags {flags_name}, {}", E::NAME);
            roundtrip::<E>(&graph, |basename| {
                BvComp::with_basename(basename)
                    .comp_flags(flags)
                    .comp_graph::<E>(&graph)
            })
            .with_context(|| format!("comp_graph: {}", context()))?;
            roundtrip::<E>(&graph, |basename| {
                BvComp::with_basename(basename)
                    .comp_flags(flags)
                    .par_comp::<E, _>(&graph)
            })
            .with_context(|| format!("par_comp: {}", context()))?;
            roundtrip::<E>(&graph, |basename| {
                BvCompZ::with_basename(basename)
                    .comp_flags(flags)
                    .chunk_size(1)
                    .comp_graph::<E>(&graph)
            })
            .with_context(|| format!("BvCompZ comp_graph: {}", context()))?;
            roundtrip::<E>(&graph, |basename| {
                BvCompZ::with_basename(basename)
                    .comp_flags(flags)
                    .chunk_size(3)
                    .par_comp::<E, _>(&graph)
            })
            .with_context(|| format!("BvCompZ par_comp: {}", context()))?;
        }
    }
    Ok(())
}

#[test]
fn test_roundtrip_be() -> Result<()> {
    test_roundtrip::<BE>()
}

#[test]
fn test_roundtrip_le() -> Result<()> {
    test_roundtrip::<LE>()
}

#[test]
fn test_static_dispatch() -> Result<()> {
    let graph = test_graph();
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&graph)
    })?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .dispatch::<Static>()
        .mode::<LoadMem>()
        .load()?;
    labels::eq_sorted(&graph, &seq)?;
    let random = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .dispatch::<Static>()
        .mode::<LoadMem>()
        .load()?;
    labels::eq_sorted(&graph, &random)?;
    labels::check_impl(&random)?;
    Ok(())
}

#[test]
fn test_check_offsets_mismatch() -> Result<()> {
    let graph = test_graph();
    let other = VecGraph::from_arcs([(3, 0)]);
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&graph)
    })?;
    let (_other_dir, other_basename) = roundtrip::<BE>(&other, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&other)
    })?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;
    // The offsets of a different graph are a valid γ stream, but wrong
    assert!(!check_offsets(&seq, &other_basename)?);
    Ok(())
}

#[test]
fn test_check_offsets_final_entry() -> Result<()> {
    // Regression: check_offsets never verified the final entry of the
    // offsets file (the total bitstream length), so a corrupted or stale
    // final offset passed the check.
    let graph = test_graph();
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&graph)
    })?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;

    // Rewrite the offsets with the correct per-node entries but a corrupted
    // final length entry.
    let mut offsets = vec![];
    let mut degs_iter = seq.offset_deg_iter();
    for (offset, _degree) in &mut degs_iter {
        offsets.push(offset);
    }
    let end = degs_iter.get_pos();
    let mut writer = buf_bit_writer::from_path::<BE, usize>(basename.with_extension("offsets"))?;
    let mut prev = 0;
    for offset in offsets {
        writer.write_gamma(offset - prev)?;
        prev = offset;
    }
    writer.write_gamma(end - prev + 1)?; // off by one
    writer.flush()?;
    assert!(!check_offsets(&seq, &basename)?);
    Ok(())
}

#[test]
fn test_comp_tmp_dir() -> Result<()> {
    let graph = test_graph();
    let tmp = tempfile::tempdir()?;
    roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename)
            .tmp_dir(tmp.path())
            .par_comp::<BE, _>(&graph)
    })?;
    Ok(())
}

#[test]
fn test_recompress_with_different_flags() -> Result<()> {
    let graph = test_graph();
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&graph)
    })?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;
    let flags = CompFlags {
        outdegrees: Codes::Delta,
        residuals: Codes::Delta,
        ..CompFlags::default()
    };
    // Sequential recompression of a compressed graph
    roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename)
            .comp_flags(flags)
            .comp_graph::<BE>(&seq)
    })?;
    // Parallel recompression, splitting the iterator of a compressed graph
    roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename)
            .comp_flags(flags)
            .par_comp::<BE, _>(&seq)
    })?;
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_par_comp_from_parts() -> Result<()> {
    use rayon::prelude::*;
    use webgraph::graphs::par_sorted_graph::ParSortedGraph;
    use webgraph::utils::{MemoryUsage, ParSortPairs};

    let graph = test_graph();
    let pairs: Vec<(usize, usize)> = common::TEST_GRAPH_ARCS.into();
    let split = ParSortPairs::new(graph.num_nodes())?
        .num_partitions(2)
        .memory_usage(MemoryUsage::BatchSize(3))
        .sort(pairs.into_par_iter(), dsi_progress_logger::no_logging![])?;
    let sorted = ParSortedGraph::from_parts(split.boundaries, split.iters);
    roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).par_comp::<BE, _>(&sorted)
    })?;
    Ok(())
}

#[test]
fn test_comp_lender() -> Result<()> {
    let graph = test_graph();
    roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_lender::<BE, _>(graph.iter(), Some(graph.num_nodes()))
    })?;
    Ok(())
}

#[cfg(not(miri))]
mod labeled {
    use super::*;
    use webgraph::graphs::bvgraph::parse_label_properties;
    use webgraph::graphs::vec_graph::LabeledVecGraph;
    use webgraph::labels::BitStreamLabeling;
    use webgraph::labels::bitstream::BitStreamStoreLabelsConf;
    use webgraph::traits::FixedWidth;

    /// A way of compressing a labeled graph.
    #[derive(Debug, Clone, Copy)]
    enum Mode {
        /// Sequential compression.
        Seq,
        /// Parallel compression.
        Par,
        /// Parallel compression with Zstandard-compressed temporary label
        /// files (sequential compression has no temporary files).
        ParZstd,
    }

    /// Compresses a labeled graph using the given mode and checks that it can
    /// be read back both with sequential and random-access labelings.
    fn labeled_roundtrip(graph: &LabeledVecGraph<u32>, mode: Mode) -> Result<()> {
        let (_dir, basename) = tmp_basename()?;
        let conf = BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u32>::new());
        let mut comp = BvComp::with_basename(&basename);
        match mode {
            Mode::Seq => comp.comp_labeled_graph::<BE, _, _>(graph, conf)?,
            Mode::Par => comp.par_comp_labeled::<BE, _, _>(graph, conf)?,
            Mode::ParZstd => comp.par_comp_labeled::<BE, _, _>(graph, conf.zstd())?,
        };

        let labels_basename = BvCompConf::default_labels_basename(&basename);
        let label_props = parse_label_properties::<BE>(&labels_basename)?;
        assert_eq!(label_props.num_nodes, graph.num_nodes());
        assert_eq!(label_props.num_arcs, graph.num_arcs());
        assert_eq!(label_props.serializer, "FixedWidth<u32>");

        let seq = || {
            BvGraphSeq::with_basename(&basename)
                .endianness::<BE>()
                .mode::<LoadMem>()
                .load()
        };
        // BitStreamLabelingSeq reads the offsets from the .offsets file
        let labeling =
            BitStreamLabelingSeq::<BE, _, _>::load(&labels_basename, FixedWidth::<u32>::new())?;
        graph::eq_labeled(graph, &Zip(seq()?, labeling))?;

        common::build_ef_from_offsets(
            graph.num_nodes(),
            &labels_basename.with_extension("labels"),
            &labels_basename.with_extension("offsets"),
            &labels_basename.with_extension("ef"),
        )?;
        let labeling =
            BitStreamLabeling::<BE, _, _, _>::load(&labels_basename, FixedWidth::<u32>::new())?;
        graph::eq_labeled(graph, &Zip(seq()?, labeling))?;
        Ok(())
    }

    #[test]
    fn test_labeled_roundtrip() -> Result<()> {
        let graph = LabeledVecGraph::from_arcs(
            common::TEST_GRAPH_ARCS.map(|(src, dst)| ((src, dst), (10 * src + dst) as u32)),
        );
        for mode in [Mode::Seq, Mode::Par, Mode::ParZstd] {
            labeled_roundtrip(&graph, mode).with_context(|| format!("mode: {mode:?}"))?;
        }
        Ok(())
    }

    #[cfg_attr(feature = "slow_tests", test)]
    #[cfg_attr(not(feature = "slow_tests"), allow(dead_code))]
    fn test_labeled_roundtrip_cnr2000() -> Result<()> {
        use lender::prelude::*;
        common::init_logger();

        let cnr = BvGraphSeq::with_basename(common::cnr_2000_basename())
            .endianness::<BE>()
            .mode::<LoadMem>()
            .load()?;
        let mut arcs = Vec::new();
        for_!((node, succ) in &cnr {
            for s in succ {
                arcs.push(((node, s), s as u32));
            }
        });
        let graph = LabeledVecGraph::from_arcs(arcs);
        labeled_roundtrip(&graph, Mode::Seq)?;
        labeled_roundtrip(&graph, Mode::Par)?;
        Ok(())
    }
}
