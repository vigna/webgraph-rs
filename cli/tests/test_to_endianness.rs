/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

// The CLI loads graphs and Elias-Fano structures through mmap, which Miri
// does not support (same convention as the other CLI integration tests).
#![cfg(not(miri))]

use anyhow::Result;
use dsi_bitstream::prelude::*;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;
use webgraph::traits::labels;
use webgraph_cli::cli_main;
use webgraph_cli::init_env_logger;

#[test]
fn test_to_endianness_offsets() -> Result<()> {
    init_env_logger()?;
    // Regression: `to endianness` wrote the end offset of node 0 as the
    // first offsets entry (instead of a leading zero), shifting all offsets
    // by one node and corrupting random access on the converted graph.
    let graph = VecGraph::from_arcs([(0, 1), (0, 5), (0, 12), (1, 0), (2, 12), (4, 3)]);
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src").display().to_string();
    let dst = tmp.path().join("dst").display().to_string();
    BvComp::with_basename(&src).comp_graph::<BE>(&graph)?;

    cli_main(vec!["webgraph", "to", "endianness", &src, &dst])?;

    // The converted offsets must match what `build offsets` derives from the
    // converted graph itself.
    let offsets = format!("{dst}.offsets");
    let offsets_orig = format!("{dst}.offsets.orig");
    std::fs::rename(&offsets, &offsets_orig)?;
    cli_main(vec!["webgraph", "build", "offsets", &dst])?;
    assert_eq!(
        std::fs::read(&offsets_orig)?,
        std::fs::read(&offsets)?,
        "converted offsets differ from rebuilt offsets"
    );

    // And the converted graph must be identical to the source graph.
    let seq = BvGraphSeq::with_basename(&dst)
        .endianness::<LE>()
        .mode::<LoadMem>()
        .load()?;
    labels::eq_sorted(&graph, &seq)?;
    Ok(())
}
