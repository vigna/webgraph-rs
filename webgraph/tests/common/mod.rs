/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![allow(dead_code)]

use anyhow::{Result, ensure};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::no_logging;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use webgraph::graphs::bvgraph::check_offsets;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;

/// The arcs of the canonical [test graph](test_graph).
pub const TEST_GRAPH_ARCS: [(usize, usize); 11] = [
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
];

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
    VecGraph::from_arcs(TEST_GRAPH_ARCS)
}

/// Returns a graph with the given number of nodes and arcs (duplicate arcs
/// are added once).
pub fn vec_graph(num_nodes: usize, arcs: impl IntoIterator<Item = (usize, usize)>) -> VecGraph {
    let mut graph = VecGraph::empty(num_nodes);
    graph.add_arcs(arcs);
    graph
}

/// Initializes the logger for tests (repeated calls are harmless).
pub fn init_logger() {
    env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Info)
        .try_init()
        .ok();
}

/// Returns a new temporary directory and a basename inside it.
///
/// The directory (and all files sharing the basename) is deleted when the
/// returned [`TempDir`] is dropped.
pub fn tmp_basename() -> Result<(TempDir, PathBuf)> {
    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("graph");
    Ok((dir, basename))
}

/// Compresses `graph` into a temporary basename using `compress`, and checks
/// the result.
///
/// The compressed graph is loaded in memory with endianness `E`, both
/// sequentially and, after building its Elias–Fano offsets, with random
/// access. The function checks that both are equal to `graph` (including the
/// number of arcs), that random access is consistent with sequential access,
/// that the offsets file is correct, and that `compress` returned the length
/// in bits of the graph bitstream.
///
/// Returns the temporary directory and the basename, for further checks.
pub fn roundtrip<E: Roundtrip>(
    graph: &VecGraph,
    compress: impl FnOnce(&Path) -> Result<u64>,
) -> Result<(TempDir, PathBuf)> {
    let (dir, basename) = tmp_basename()?;
    let bits = compress(&basename)?;
    E::check(graph, &basename, bits)?;
    Ok((dir, basename))
}

/// The endianness-dependent part of [`roundtrip`].
///
/// This trait is implemented separately for each endianness because the trait
/// bounds needed to load a graph with generic endianness cannot be expressed
/// conveniently.
pub trait Roundtrip: Endianness {
    fn check(graph: &VecGraph, basename: &Path, bits: u64) -> Result<()>;
}

macro_rules! impl_roundtrip {
    ($E:ty) => {
        impl Roundtrip for $E {
            fn check(graph: &VecGraph, basename: &Path, bits: u64) -> Result<()> {
                let seq = BvGraphSeq::with_basename(basename)
                    .endianness::<$E>()
                    .mode::<LoadMem>()
                    .load()?;
                labels::eq_sorted(graph, &seq)?;
                ensure!(seq.get_num_arcs() == Some(graph.num_arcs()));
                ensure!(check_offsets(&seq, basename)?, "wrong offsets");
                let mut offset_deg_iter = seq.offset_deg_iter();
                for _ in &mut offset_deg_iter {}
                let len = offset_deg_iter.get_pos();
                ensure!(
                    bits == len,
                    "{bits} bits written, but the bitstream is {len} bits long"
                );

                build_ef(basename)?;
                let random = BvGraph::with_basename(basename)
                    .endianness::<$E>()
                    .mode::<LoadMem>()
                    .load()?;
                labels::eq_sorted(graph, &random)?;
                labels::check_impl(&random)?;
                Ok(())
            }
        }
    };
}

impl_roundtrip!(BE);
impl_roundtrip!(LE);

/// Builds the Elias–Fano representation from a γ-coded delta offsets
/// file and serializes it.
pub fn build_ef_from_offsets(
    num_nodes: usize,
    data_path: &Path,
    offsets_path: &Path,
    ef_path: &Path,
) -> Result<()> {
    store_ef_with_data(
        num_nodes,
        data_path,
        offsets_path,
        ef_path,
        &mut no_logging![],
    )
}

/// Builds the Elias–Fano representation of offsets for a graph basename.
///
/// Reads `num_nodes` from the `.properties` file.
pub fn build_ef(basename: &Path) -> Result<()> {
    let properties_path = basename.with_extension("properties");
    let props = std::fs::read_to_string(&properties_path)?;
    let num_nodes: usize = props
        .lines()
        .find(|l| l.starts_with("nodes="))
        .unwrap()
        .strip_prefix("nodes=")
        .unwrap()
        .parse()?;

    build_ef_from_offsets(
        num_nodes,
        &basename.with_extension("graph"),
        &basename.with_extension("offsets"),
        &basename.with_extension("ef"),
    )
}

/// A label store whose [`push_label`] always fails.
///
/// [`push_label`]: StoreLabels::push_label
pub struct FailingStore<L>(pub std::marker::PhantomData<L>);

impl<L> StoreLabels for FailingStore<L> {
    type Label = L;
    fn init(&mut self) -> Result<()> {
        Ok(())
    }
    fn push_node(&mut self) -> Result<()> {
        Ok(())
    }
    fn push_label(&mut self, _label: &L) -> Result<()> {
        anyhow::bail!("label storage failed")
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn label_written_bits(&self) -> u64 {
        0
    }
    fn offsets_written_bits(&self) -> u64 {
        0
    }
}

/// Writes a sequence of big-endian `u64` values to a file.
pub fn write_be_u64s(path: &Path, values: &[u64]) -> Result<()> {
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_be_bytes()).collect();
    std::fs::write(path, bytes)?;
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
