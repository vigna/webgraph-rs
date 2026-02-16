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

// ── From test_core.rs ──

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
fn test_labeled_graph_eq() -> Result<()> {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    graph::eq_labeled(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_eq_sorted_labels() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    labels::eq_sorted(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_graph_eq_different_outdegree() {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Outdegree { .. }));
}

#[test]
fn test_eq_sorted_different_nodes() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::NumNodes { .. }));
}

#[test]
fn test_labeled_graph_eq_different() {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 99)]);
    let err = graph::eq_labeled(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

#[test]
fn test_eq_sorted_identical() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    labels::eq_sorted(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_eq_sorted_different_num_nodes_v1() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::NumNodes { .. }));
}

#[test]
fn test_eq_sorted_different_successors_v1() {
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
fn test_graph_eq_function() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    graph::eq(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_graph_eq_function_different() {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 2), (1, 2), (2, 0)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

#[test]
fn test_graph_eq_labeled_same() -> Result<()> {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    graph::eq_labeled(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_graph_eq_labeled_different_labels() {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 99)]);
    let err = graph::eq_labeled(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

#[test]
fn test_eq_error_display_v1() {
    let err = EqError::NumNodes {
        first: 3,
        second: 5,
    };
    assert_eq!(format!("{err}"), "Different number of nodes: 3 != 5");

    let err = EqError::NumArcs {
        first: 10,
        second: 20,
    };
    assert_eq!(format!("{err}"), "Different number of arcs: 10 != 20");

    let err = EqError::Successors {
        node: 1,
        index: 0,
        first: "2".to_string(),
        second: "3".to_string(),
    };
    assert!(format!("{err}").contains("Different successors for node 1"));

    let err = EqError::Outdegree {
        node: 2,
        first: 3,
        second: 1,
    };
    assert!(format!("{err}").contains("Different outdegree for node 2"));
}

#[test]
fn test_eq_succs_first_shorter() {
    // First list shorter than second → Outdegree error
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
    // Second list shorter than first → Outdegree error
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
fn test_zip_verify_compatible_v1() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let z = Zip(g0, g1);
    assert!(z.verify());
    Ok(())
}

#[test]
fn test_zip_verify_incompatible_succs() -> Result<()> {
    // Same structure but different number of successors for node 0
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

// ── From test_coverage.rs ──

#[test]
fn test_eq_sorted_checks() -> Result<()> {
    use webgraph::traits::labels;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    labels::eq_sorted(&g1, &g2)?;
    Ok(())
}

#[test]
fn test_eq_sorted_diff_num_nodes() {
    use webgraph::traits::labels;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    assert!(labels::eq_sorted(&g1, &g2).is_err());
}

#[test]
fn test_check_impl_ok() -> Result<()> {
    use webgraph::traits::labels;
    // check_impl verifies consistency between sequential and random-access
    // implementations of a labeling
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    labels::check_impl(&g)?;
    Ok(())
}

#[test]
fn test_zip_verify_compatible_v2() {
    use webgraph::labels::Zip;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let zipped = Zip(&g1, &g2);
    assert!(zipped.verify());
}

#[test]
fn test_zip_verify_different_successors() {
    use webgraph::labels::Zip;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1)]);
    let zipped = Zip(&g1, &g2);
    // Different number of nodes should make verify fail
    assert!(!zipped.verify());
}

#[test]
fn test_zip_verify_different_outdegrees() {
    use webgraph::labels::Zip;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let zipped = Zip(&g1, &g2);
    // Same num_nodes but node 0 has different outdegree
    assert!(!zipped.verify());
}

#[test]
fn test_eq_sorted_different_successors_v2() {
    use webgraph::traits::labels;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 2), (1, 2)]);
    assert!(labels::eq_sorted(&g1, &g2).is_err());
}

#[test]
fn test_eq_sorted_different_outdegrees() {
    use webgraph::traits::labels;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert!(labels::eq_sorted(&g1, &g2).is_err());
}

#[test]
fn test_eq_sorted_empty_graphs() -> Result<()> {
    use webgraph::traits::labels;
    let g1 = webgraph::graphs::vec_graph::VecGraph::empty(5);
    let g2 = webgraph::graphs::vec_graph::VecGraph::empty(5);
    labels::eq_sorted(&g1, &g2)?;
    Ok(())
}

#[test]
fn test_check_impl_empty_graph() -> Result<()> {
    use webgraph::traits::labels;
    let g = webgraph::graphs::vec_graph::VecGraph::empty(3);
    labels::check_impl(&g)?;
    Ok(())
}

#[test]
fn test_check_impl_larger_graph() -> Result<()> {
    use webgraph::traits::labels;
    let g =
        webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    labels::check_impl(&g)?;
    Ok(())
}

#[test]
fn test_eq_sorted_identical_graphs() -> Result<()> {
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert!(webgraph::traits::eq_sorted(&g1, &g2).is_ok());
    Ok(())
}

#[test]
fn test_eq_sorted_different_num_nodes_v2() -> Result<()> {
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    g2.add_node(5); // More nodes
    let err = webgraph::traits::eq_sorted(&g1, &g2);
    assert!(err.is_err());
    let e = err.unwrap_err();
    assert!(matches!(e, webgraph::traits::EqError::NumNodes { .. }));
    Ok(())
}

#[test]
fn test_eq_sorted_different_successors_detail() -> Result<()> {
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 2), (1, 2), (2, 0)]);
    let err = webgraph::traits::eq_sorted(&g1, &g2);
    assert!(err.is_err());
    Ok(())
}

#[test]
fn test_eq_succs_identical() {
    let result = webgraph::traits::eq_succs(0, vec![1, 2, 3], vec![1, 2, 3]);
    assert!(result.is_ok());
}

#[test]
fn test_eq_succs_different_values() {
    let result = webgraph::traits::eq_succs(0, vec![1, 2, 3], vec![1, 2, 4]);
    assert!(result.is_err());
    let e = result.unwrap_err();
    assert!(matches!(e, webgraph::traits::EqError::Successors { .. }));
}

#[test]
fn test_eq_succs_different_lengths_first_shorter() {
    let result = webgraph::traits::eq_succs(0, vec![1, 2], vec![1, 2, 3]);
    assert!(result.is_err());
    let e = result.unwrap_err();
    assert!(matches!(e, webgraph::traits::EqError::Outdegree { .. }));
}

#[test]
fn test_eq_succs_different_lengths_second_shorter() {
    let result = webgraph::traits::eq_succs(0, vec![1, 2, 3], vec![1, 2]);
    assert!(result.is_err());
    let e = result.unwrap_err();
    assert!(matches!(e, webgraph::traits::EqError::Outdegree { .. }));
}

#[test]
fn test_eq_succs_empty() {
    let result = webgraph::traits::eq_succs(0, Vec::<usize>::new(), Vec::<usize>::new());
    assert!(result.is_ok());
}

#[test]
fn test_check_impl_vecgraph() -> Result<()> {
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert!(webgraph::traits::check_impl(&g).is_ok());
    Ok(())
}

#[test]
fn test_check_impl_empty_graph_no_arcs() -> Result<()> {
    let mut g = webgraph::graphs::vec_graph::VecGraph::new();
    g.add_node(2);
    assert!(webgraph::traits::check_impl(&g).is_ok());
    Ok(())
}

#[test]
fn test_check_impl_bvgraph() -> Result<()> {
    let basename = std::path::Path::new("../data/cnr-2000");
    let graph = BvGraph::with_basename(basename).load()?;
    assert!(webgraph::traits::check_impl(&graph).is_ok());
    Ok(())
}

#[test]
fn test_eq_error_display_v2() {
    let e1 = webgraph::traits::EqError::NumNodes {
        first: 10,
        second: 20,
    };
    let s = format!("{}", e1);
    assert!(s.contains("10") && s.contains("20"));

    let e2 = webgraph::traits::EqError::NumArcs {
        first: 100,
        second: 200,
    };
    let s = format!("{}", e2);
    assert!(s.contains("100") && s.contains("200"));

    let e3 = webgraph::traits::EqError::Successors {
        node: 5,
        index: 3,
        first: "1".to_string(),
        second: "2".to_string(),
    };
    let s = format!("{}", e3);
    assert!(s.contains("5"));

    let e4 = webgraph::traits::EqError::Outdegree {
        node: 7,
        first: 3,
        second: 5,
    };
    let s = format!("{}", e4);
    assert!(s.contains("7"));
}

#[test]
fn test_graph_eq_identical() -> Result<()> {
    use webgraph::traits::graph;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert!(graph::eq(&g1, &g2).is_ok());
    Ok(())
}

#[test]
fn test_graph_eq_different_num_nodes() -> Result<()> {
    use webgraph::traits::graph;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let err = graph::eq(&g1, &g2);
    assert!(err.is_err());
    Ok(())
}

#[test]
fn test_graph_eq_labeled_identical() -> Result<()> {
    use webgraph::traits::graph;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let labeled1 = graph::UnitLabelGraph(g1);
    let labeled2 = graph::UnitLabelGraph(g2);
    assert!(graph::eq_labeled(&labeled1, &labeled2).is_ok());
    Ok(())
}

#[test]
fn test_graph_eq_labeled_different() -> Result<()> {
    use webgraph::traits::graph;
    let g1 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g2 = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let labeled1 = graph::UnitLabelGraph(g1);
    let labeled2 = graph::UnitLabelGraph(g2);
    assert!(graph::eq_labeled(&labeled1, &labeled2).is_err());
    Ok(())
}

#[test]
fn test_labeled_random_access_graph_has_arc() {
    use webgraph::traits::graph::{LabeledRandomAccessGraph, UnitLabelGraph};
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let labeled = UnitLabelGraph(g);
    assert!(labeled.has_arc(0, 1));
    assert!(!labeled.has_arc(0, 2));
    assert!(labeled.has_arc(2, 0));
}

#[test]
fn test_eq_succs_first_longer_than_second() {
    use webgraph::traits::labels;
    let result = labels::eq_succs(0, vec![1, 2, 3], vec![1, 2]);
    assert!(result.is_err());
    match result.unwrap_err() {
        labels::EqError::Outdegree {
            node,
            first,
            second,
        } => {
            assert_eq!(node, 0);
            assert_eq!(first, 3);
            assert_eq!(second, 2);
        }
        other => panic!("Expected Outdegree error, got {:?}", other),
    }
}

#[test]
fn test_eq_succs_second_longer_than_first() {
    use webgraph::traits::labels;
    let result = labels::eq_succs(0, vec![1, 2], vec![1, 2, 3]);
    assert!(result.is_err());
    match result.unwrap_err() {
        labels::EqError::Outdegree {
            node,
            first,
            second,
        } => {
            assert_eq!(node, 0);
            assert_eq!(first, 2);
            assert_eq!(second, 3);
        }
        other => panic!("Expected Outdegree error, got {:?}", other),
    }
}
