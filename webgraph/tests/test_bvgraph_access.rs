/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for BvGraph access patterns: endianness, properties parsing,
//! sequential and random access loading, load modes, static dispatch, and
//! offsets.

mod common;

use anyhow::Result;
use common::{roundtrip, test_graph};
use dsi_bitstream::prelude::*;
use lender::*;
use webgraph::graphs::bvgraph::{File, Static, check_offsets, get_endianness, parse_properties};
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;

/// A graph exercising all decompression paths: intervals, references, blocks,
/// and residuals (with the flags of [`access_flags`]).
fn access_graph() -> VecGraph {
    VecGraph::from_arcs(
        // Node 0: consecutive successors (intervals)
        (1..20)
            .map(|i| (0, i))
            // Node 1: overlaps with node 0 (reference and blocks)
            .chain((5..25).map(|i| (1, i)))
            // Node 2: sparse successors (residuals)
            .chain([(2, 3), (2, 10), (2, 20), (2, 30)])
            // Node 3: overlaps with node 2 (reference and blocks)
            .chain([(3, 3), (3, 11), (3, 20), (3, 31), (31, 0)]),
    )
}

/// Default codes (so that static dispatch can be used) with nondefault
/// parameters.
fn access_flags() -> CompFlags {
    CompFlags {
        min_interval_length: 4,
        compression_window: 7,
        max_ref_count: 3,
        ..CompFlags::default()
    }
}

/// Loads a compressed graph with endianness `$E` and load mode `$mode`,
/// sequentially and with random access, with dynamic and static dispatch, and
/// checks that it is equal to `$graph`.
macro_rules! check_load_mode {
    ($graph:expr, $basename:expr, $E:ty, $mode:ty) => {{
        let seq = BvGraphSeq::with_basename($basename)
            .endianness::<$E>()
            .mode::<$mode>()
            .load()?;
        labels::eq_sorted($graph, &seq)?;
        let seq = BvGraphSeq::with_basename($basename)
            .endianness::<$E>()
            .mode::<$mode>()
            .dispatch::<Static>()
            .load()?;
        labels::eq_sorted($graph, &seq)?;
        assert!(check_offsets(&seq, $basename)?);
        let random = BvGraph::with_basename($basename)
            .endianness::<$E>()
            .mode::<$mode>()
            .load()?;
        labels::eq_sorted($graph, &random)?;
        labels::check_impl(&random)?;
        let random = BvGraph::with_basename($basename)
            .endianness::<$E>()
            .mode::<$mode>()
            .dispatch::<Static>()
            .load()?;
        labels::eq_sorted($graph, &random)?;
        labels::check_impl(&random)?;
    }};
}

macro_rules! test_load_modes {
    ($name:ident, $E:ty) => {
        #[test]
        fn $name() -> Result<()> {
            let graph = access_graph();
            let (_dir, basename) = roundtrip::<$E>(&graph, |basename| {
                BvComp::with_basename(basename)
                    .comp_flags(access_flags())
                    .comp_graph::<$E>(&graph)
            })?;
            check_load_mode!(&graph, &basename, $E, LoadMem);
            check_load_mode!(&graph, &basename, $E, File);
            #[cfg(not(miri))]
            {
                check_load_mode!(&graph, &basename, $E, LoadMmap);
                check_load_mode!(&graph, &basename, $E, Mmap);
            }
            Ok(())
        }
    };
}

test_load_modes!(test_load_modes_be, BE);
test_load_modes!(test_load_modes_le, LE);

#[test]
fn test_get_endianness() -> Result<()> {
    let graph = test_graph();
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&graph)
    })?;
    assert_eq!(get_endianness(&basename)?, "big");
    let (_dir, basename) = roundtrip::<LE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_graph::<LE>(&graph)
    })?;
    assert_eq!(get_endianness(&basename)?, "little");
    Ok(())
}

#[test]
fn test_parse_properties() -> Result<()> {
    let graph = test_graph();
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename)
            .comp_flags(access_flags())
            .comp_graph::<BE>(&graph)
    })?;
    // parse_properties takes the full path to the .properties file
    let (num_nodes, num_arcs, flags) =
        parse_properties::<BE>(&basename.with_extension("properties"))?;
    assert_eq!(num_nodes, 8);
    assert_eq!(num_arcs, 11);
    let expected = access_flags();
    assert_eq!(flags.compression_window, expected.compression_window);
    assert_eq!(flags.min_interval_length, expected.min_interval_length);
    assert_eq!(flags.max_ref_count, expected.max_ref_count);
    assert_eq!(flags.outdegrees, expected.outdegrees);
    assert_eq!(flags.references, expected.references);
    assert_eq!(flags.blocks, expected.blocks);
    assert_eq!(flags.intervals, expected.intervals);
    assert_eq!(flags.residuals, expected.residuals);
    Ok(())
}

#[test]
fn test_iter_from_middle() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut nodes = vec![];
    for_!((node, _succs) in graph.iter_from(2) {
        nodes.push(node);
    });
    assert_eq!(nodes, [2, 3, 4]);
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
    let graph = VecGraph::from_arcs([(0, 1), (0, 5), (0, 12), (0, 30), (1, 0), (2, 30)]);
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename)
            .comp_flags(CompFlags {
                outdegrees: Codes::Delta,
                references: Codes::Delta,
                blocks: Codes::Delta,
                intervals: Codes::Delta,
                residuals: Codes::Delta,
                ..CompFlags::default()
            })
            .comp_graph::<BE>(&graph)
    })?;

    type AllDelta = Static<
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
    labels::eq_sorted(&graph, &seq)?;
    let random = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .dispatch::<AllDelta>()
        .load()?;
    labels::eq_sorted(&graph, &random)?;
    labels::check_impl(&random)?;
    Ok(())
}

#[test]
fn test_stale_ef_rejected_at_load() -> Result<()> {
    // Regression: the random-access decoders index the Elias-Fano offsets
    // without bounds checks, and a stale .ef (built for a different graph)
    // was accepted at load time.
    let small = VecGraph::from_arcs([(0, 1)]);
    let large = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let (_small_dir, small_base) = roundtrip::<BE>(&small, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&small)
    })?;
    let (_large_dir, large_base) = roundtrip::<BE>(&large, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&large)
    })?;
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
        .dispatch::<Static>()
        .load();
    assert!(
        res.is_err(),
        "expected stale-offsets error on static dispatch, got Ok"
    );
    Ok(())
}
