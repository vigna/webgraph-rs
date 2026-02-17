/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for [`SequentialLabeling::build_dcf`] and its overrides in
//! [`BvGraphSeq`], [`CsrGraph`], and [`CsrSortedGraph`].

mod common;

use anyhow::Result;
use common::test_graph;
use dsi_bitstream::prelude::*;
use lender::*;
use sux::traits::IndexedSeq;
use webgraph::graphs::bvgraph::DCF;
use webgraph::prelude::*;

/// DCF for the canonical test graph (8 nodes, 11 arcs).
///
/// Outdegrees: 2, 3, 1, 1, 1, 2, 1, 0.
const EXPECTED_DCF: [usize; 9] = [0, 2, 5, 6, 7, 8, 10, 11, 11];

/// Checks that the given DCF matches the expected cumulative degree sequence.
fn verify_dcf(dcf: &DCF, expected: &[usize]) {
    assert_eq!(dcf.len(), expected.len());
    for (i, &expected_val) in expected.iter().enumerate() {
        assert_eq!(
            dcf.get(i),
            expected_val,
            "DCF mismatch at index {i}: expected {expected_val}, got {}",
            dcf.get(i),
        );
    }
}

/// Tests the default trait implementation of `build_dcf` using a [`VecGraph`].
#[test]
fn test_build_dcf_vec_graph() {
    let graph = test_graph();
    let dcf = graph.build_dcf();
    verify_dcf(&dcf, &EXPECTED_DCF);
}

/// Tests the [`BvGraphSeq`] override of `build_dcf`, which uses
/// [`OffsetDegIter::next_degree`] internally.
#[test]
fn test_build_dcf_bvgraph_seq() -> Result<()> {
    let graph = test_graph();
    let tmp = tempfile::NamedTempFile::new()?;
    BvComp::with_basename(tmp.path()).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(tmp.path())
        .endianness::<BE>()
        .load()?;
    let dcf = seq.build_dcf();
    verify_dcf(&dcf, &EXPECTED_DCF);
    Ok(())
}

/// Tests the [`CsrGraph`] override of `build_dcf`, which iterates the stored
/// degree cumulative function directly.
#[test]
fn test_build_dcf_csr_graph() {
    let graph = test_graph();
    let csr = CsrGraph::from_seq_graph(&graph);
    let dcf = csr.build_dcf();
    verify_dcf(&dcf, &EXPECTED_DCF);
}

/// Tests the [`CsrSortedGraph`] override of `build_dcf`, which delegates to
/// [`CsrGraph::build_dcf`].
#[test]
fn test_build_dcf_csr_sorted_graph() {
    let graph = test_graph();
    let csr = CsrSortedGraph::from_seq_graph(&graph);
    let dcf = csr.build_dcf();
    verify_dcf(&dcf, &EXPECTED_DCF);
}

/// Tests that all implementations produce the same DCF for the same graph
/// (cross-check).
#[test]
fn test_build_dcf_cross_check() -> Result<()> {
    let graph = test_graph();
    let csr = CsrGraph::from_seq_graph(&graph);
    let csr_sorted = CsrSortedGraph::from_seq_graph(&graph);

    let tmp = tempfile::NamedTempFile::new()?;
    BvComp::with_basename(tmp.path()).comp_graph::<BE>(&graph)?;
    let bv_seq = BvGraphSeq::with_basename(tmp.path())
        .endianness::<BE>()
        .load()?;

    let dcf_vec = graph.build_dcf();
    let dcf_csr = csr.build_dcf();
    let dcf_csr_sorted = csr_sorted.build_dcf();
    let dcf_bv_seq = bv_seq.build_dcf();

    for i in 0..EXPECTED_DCF.len() {
        let v = dcf_vec.get(i);
        assert_eq!(v, dcf_csr.get(i), "VecGraph vs CsrGraph at index {i}");
        assert_eq!(
            v,
            dcf_csr_sorted.get(i),
            "VecGraph vs CsrSortedGraph at index {i}"
        );
        assert_eq!(v, dcf_bv_seq.get(i), "VecGraph vs BvGraphSeq at index {i}");
    }
    Ok(())
}

/// Tests `build_dcf` on the cnr-2000 graph, verifying the [`BvGraphSeq`]
/// override against a DCF computed via sequential iteration.
#[test]
fn test_build_dcf_cnr_2000() -> Result<()> {
    let seq = BvGraphSeq::with_basename("../data/cnr-2000")
        .endianness::<BE>()
        .load()?;
    let n = seq.num_nodes();
    let dcf = seq.build_dcf();

    assert_eq!(dcf.len(), n + 1);
    assert_eq!(dcf.get(0), 0);

    // Verify against sequential iteration
    let mut cumul = 0usize;
    let mut node_idx = 0usize;
    let mut lender = seq.iter();
    while let Some((_node, succs)) = lender.next() {
        cumul += succs.into_iter().count();
        node_idx += 1;
        assert_eq!(dcf.get(node_idx), cumul, "DCF mismatch at node {node_idx}");
    }
    assert_eq!(node_idx, n);
    assert_eq!(cumul, seq.num_arcs_hint().unwrap() as usize);
    Ok(())
}
