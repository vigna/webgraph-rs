/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for graph equality functions, eq_sorted, eq_succs, check_impl, and Zip verify.

use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::path::Path;
use webgraph::graphs::vec_graph::{LabeledVecGraph, VecGraph};
use webgraph::labels::Zip;
use webgraph::prelude::*;
use webgraph::traits::{
    graph,
    labels::{self, EqError},
};

/// Builds the Elias-Fano representation of offsets for a graph.
///
/// Replicates the core of `webgraph build ef` by reading the .offsets file.
#[allow(dead_code)]
fn build_ef(basename: &Path) -> Result<()> {
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

// ── graph::eq ──

#[test]
fn test_graph_eq_same() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    graph::eq(&g, &g)?;
    Ok(())
}

#[test]
fn test_graph_eq_different_nodes() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::NumNodes { .. }));
}

#[test]
fn test_graph_eq_different_arcs() {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 0), (2, 0)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

#[test]
fn test_graph_eq_different_outdegree() {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Outdegree { .. }));
}

// ── graph::eq_labeled ──

#[test]
fn test_labeled_graph_eq() -> Result<()> {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    graph::eq_labeled(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_labeled_graph_eq_different() {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 99)]);
    let err = graph::eq_labeled(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

// ── labels::eq_sorted ──

#[test]
fn test_eq_sorted_labels() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    labels::eq_sorted(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_eq_sorted_different_nodes() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::NumNodes { .. }));
}

#[test]
fn test_eq_sorted_different_successors() {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 2), (1, 2), (2, 0)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

#[test]
fn test_eq_sorted_different_outdegree() {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 0), (2, 0)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Outdegree { .. }));
}

#[test]
fn test_eq_sorted_empty_graphs() -> Result<()> {
    let g1 = VecGraph::empty(5);
    let g2 = VecGraph::empty(5);
    labels::eq_sorted(&g1, &g2)?;
    Ok(())
}

// ── labels::eq_succs ──

#[test]
fn test_eq_succs_identical() {
    let result = labels::eq_succs(0, vec![1, 2, 3], vec![1, 2, 3]);
    assert!(result.is_ok());
}

#[test]
fn test_eq_succs_different_values() {
    let result = labels::eq_succs(0, vec![1, 2, 3], vec![1, 2, 4]);
    assert!(result.is_err());
    let e = result.unwrap_err();
    assert!(matches!(e, EqError::Successors { .. }));
}

#[test]
fn test_eq_succs_first_shorter() {
    let err = labels::eq_succs(0, vec![1], vec![1, 2]).unwrap_err();
    match err {
        EqError::Outdegree {
            node,
            first,
            second,
        } => {
            assert_eq!(node, 0);
            assert_eq!(first, 1);
            assert_eq!(second, 2);
        }
        _ => panic!("Expected Outdegree error"),
    }
}

#[test]
fn test_eq_succs_second_shorter() {
    let err = labels::eq_succs(0, vec![1, 2], vec![1]).unwrap_err();
    match err {
        EqError::Outdegree {
            node,
            first,
            second,
        } => {
            assert_eq!(node, 0);
            assert_eq!(first, 2);
            assert_eq!(second, 1);
        }
        _ => panic!("Expected Outdegree error"),
    }
}

#[test]
fn test_eq_succs_empty() {
    let result = labels::eq_succs(0, Vec::<usize>::new(), Vec::<usize>::new());
    assert!(result.is_ok());
}

// ── Zip::verify ──

#[test]
fn test_zip_verify_compatible() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let z = Zip(g0, g1);
    assert!(z.verify());
    Ok(())
}

#[test]
fn test_zip_verify_incompatible_succs() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2)]);
    let g1 = VecGraph::from_arcs([(0, 1)]);
    let z = Zip(g0, g1);
    assert!(!z.verify());
    Ok(())
}

#[test]
fn test_zip_verify_incompatible_nodes() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let z = Zip(g0, g1);
    assert!(!z.verify());
    Ok(())
}

#[test]
fn test_zip_verify_different_outdegrees() {
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let g2 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let zipped = Zip(&g1, &g2);
    assert!(!zipped.verify());
}

// ── labels::check_impl ──

#[test]
fn test_check_impl_ok() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    labels::check_impl(&g)?;
    Ok(())
}

#[test]
fn test_check_impl_empty_graph() -> Result<()> {
    let g = VecGraph::empty(3);
    labels::check_impl(&g)?;
    Ok(())
}

#[test]
fn test_check_impl_larger_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    labels::check_impl(&g)?;
    Ok(())
}

#[test]
fn test_check_impl_bvgraph() -> Result<()> {
    let basename = std::path::Path::new("../data/cnr-2000");
    let graph = BvGraph::with_basename(basename).load()?;
    labels::check_impl(&graph)?;
    Ok(())
}

// ── UnitLabelGraph ──

#[test]
fn test_labeled_random_access_graph_has_arc() {
    use webgraph::traits::graph::{LabeledRandomAccessGraph, UnitLabelGraph};
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let labeled = UnitLabelGraph(g);
    assert!(labeled.has_arc(0, 1));
    assert!(!labeled.has_arc(0, 2));
    assert!(labeled.has_arc(2, 0));
}
