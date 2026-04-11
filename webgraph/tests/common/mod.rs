/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![allow(dead_code)]

use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::path::{Path, PathBuf};
use sux::traits::TryIntoUnaligned;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::{EF, LOG2_ONES_PER_INVENTORY, LOG2_WORDS_PER_SUBINVENTORY};

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

/// Builds the Elias–Fano representation of offsets for a graph.
///
/// Replicates the core of `webgraph build ef` by reading the .offsets file.
pub fn build_ef(basename: &Path) -> Result<()> {
    use epserde::ser::Serialize;
    use std::io::{BufWriter, Seek};
    use sux::prelude::*;

    let graph_path = basename.with_extension("graph");
    let mut f = std::fs::File::open(&graph_path)?;
    let file_len = 8 * f.seek(std::io::SeekFrom::End(0))?;

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
    let ef: EF = unsafe {
        ef.map_high_bits(
            SelectAdaptConst::<_, _, LOG2_ONES_PER_INVENTORY, LOG2_WORDS_PER_SUBINVENTORY>::new,
        )
        .try_into_unaligned()?
    };

    let ef_path = basename.with_extension("ef");
    let mut ef_file = BufWriter::new(std::fs::File::create(&ef_path)?);
    unsafe { ef.serialize(&mut ef_file)? };
    Ok(())
}

/// Returns the basename for the cnr-2000 test graph, selecting the
/// platform-appropriate data directory.
///
/// On 64-bit platforms this returns `../data/cnr-2000`; on 32-bit platforms
/// it returns `../data/cnr-2000_32/cnr-2000`, whose `.ef` and `.dcf` files
/// are built with 32-bit `usize`.
pub fn cnr_2000_basename() -> PathBuf {
    #[cfg(target_pointer_width = "64")]
    return PathBuf::from("../data/cnr-2000");
    #[cfg(not(target_pointer_width = "64"))]
    return PathBuf::from("../data/cnr-2000_32/cnr-2000");
}

/// Returns the basename for the cnr-2000-t (transpose) test graph,
/// selecting the platform-appropriate data directory.
///
/// See [`cnr_2000_basename`] for details on the 32-bit strategy.
pub fn cnr_2000_t_basename() -> PathBuf {
    #[cfg(target_pointer_width = "64")]
    return PathBuf::from("../data/cnr-2000-t");
    #[cfg(not(target_pointer_width = "64"))]
    return PathBuf::from("../data/cnr-2000_32/cnr-2000-t");
}
