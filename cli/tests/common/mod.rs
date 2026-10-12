/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![allow(dead_code)]

use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;
use webgraph_cli::cli_main;

/// Returns the basename for the cnr-2000 test graph, selecting the
/// platform-appropriate data directory.
pub fn cnr_2000_basename() -> PathBuf {
    #[cfg(target_pointer_width = "64")]
    return PathBuf::from("../data/cnr-2000");
    #[cfg(not(target_pointer_width = "64"))]
    return PathBuf::from("../data/cnr-2000_32/cnr-2000");
}

/// Compresses `graph` with endianness `E` and builds its Elias–Fano offsets
/// using the CLI.
pub fn compress_with_ef<E: Endianness>(graph: &VecGraph, basename: &str) -> Result<()>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
{
    BvComp::with_basename(basename).comp_graph::<E>(graph)?;
    cli_main(vec!["webgraph", "build", "ef", basename])
}

/// Checks that the offsets of a graph are the same as those rebuilt by
/// `webgraph build offsets` from the graph itself.
pub fn check_rebuilt_offsets(basename: &str) -> Result<()> {
    let offsets = format!("{basename}.{OFFSETS_EXTENSION}");
    let original = std::fs::read(&offsets)?;
    std::fs::remove_file(&offsets)?;
    cli_main(vec!["webgraph", "build", "offsets", basename])?;
    assert_eq!(
        original,
        std::fs::read(&offsets)?,
        "offsets of {basename} differ from rebuilt offsets"
    );
    Ok(())
}

/// Reads a file of ASCII floating-point values, one per line.
pub fn read_floats(path: &Path) -> Result<Vec<f64>> {
    Ok(std::fs::read_to_string(path)?
        .lines()
        .map(|l| l.parse::<f64>())
        .collect::<Result<_, _>>()?)
}
