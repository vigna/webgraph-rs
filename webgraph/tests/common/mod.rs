/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(dead_code)]

use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::path::Path;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::EF;

/// Canonical test graph (8 nodes, 11 arcs).
///
/// - Outdegree 0: node 7 (sink)
/// - Outdegree 1: nodes 2, 3, 4, 6
/// - Outdegree 2: nodes 0, 5
/// - Outdegree 3: node 1
/// - Indegree 0: node 0 (source)
/// - Indegree 1: nodes 1, 3, 5, 7
/// - Indegree 2: nodes 2, 4
/// - Indegree 3: node 6
/// - Cycle: 2 → 4 → 6 → 2
pub fn test_graph() -> VecGraph {
    VecGraph::from_arcs([
        (0, 1),
        (0, 2),
        (1, 3),
        (1, 4),
        (1, 5),
        (2, 4),
        (3, 6),
        (4, 6),
        (5, 6),
        (5, 7),
        (6, 2),
    ])
}

/// Builds the Elias-Fano representation of offsets for a graph.
///
/// Replicates the core of `webgraph build ef` by reading the .offsets file.
pub fn build_ef(basename: &Path) -> Result<()> {
    use epserde::ser::Serialize;
    use std::io::{BufWriter, Seek};
    use sux::prelude::*;

    let graph_path = basename.with_extension("graph");
    let mut f = std::fs::File::open(&graph_path)?;
    let file_len = 8 * f.seek(std::io::SeekFrom::End(0))? as usize;

    let properties_path = basename.with_extension("properties");
    let props = std::fs::read_to_string(&properties_path)?;
    let num_nodes: usize = props
        .lines()
        .find(|l| l.starts_with("nodes="))
        .unwrap()
        .strip_prefix("nodes=")
        .unwrap()
        .parse()?;

    // Read from the .offsets file (gamma-coded in BE)
    let offsets_path = basename.with_extension("offsets");
    let of =
        webgraph::utils::MmapHelper::<u32>::mmap(&offsets_path, mmap_rs::MmapFlags::SEQUENTIAL)?;
    let mut reader: BufBitReader<BE, _> = BufBitReader::new(MemWordReader::new(of.as_ref()));

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, file_len);
    let mut offset = 0u64;
    for _ in 0..num_nodes + 1 {
        offset += reader.read_gamma()?;
        efb.push(offset as _);
    }

    let ef = efb.build();
    let ef: EF = unsafe { ef.map_high_bits(SelectAdaptConst::<_, _, 12, 4>::new) };

    let ef_path = basename.with_extension("ef");
    let mut ef_file = BufWriter::new(std::fs::File::create(&ef_path)?);
    unsafe { ef.serialize(&mut ef_file)? };
    Ok(())
}
