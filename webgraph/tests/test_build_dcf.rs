/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for [`SequentialLabeling::build_dcf`] and its overrides in
//! [`BvGraphSeq`], [`CsrGraph`], and [`CsrSortedGraph`].

mod common;

use anyhow::Result;
use common::{roundtrip, test_graph};
use dsi_bitstream::prelude::*;
use value_traits::slices::SliceByValue;
use webgraph::graphs::bvgraph::DCF;
use webgraph::prelude::*;

/// DCF for the canonical test graph (8 nodes, 11 arcs).
///
/// Outdegrees: 2, 3, 1, 1, 1, 2, 1, 0.
const EXPECTED_DCF: [u64; 9] = [0, 2, 5, 6, 7, 8, 10, 11, 11];

/// Returns the values of a DCF.
fn dcf_values(dcf: &DCF) -> Vec<u64> {
    (0..dcf.len()).map(|i| dcf.index_value(i)).collect()
}

/// Checks that the given DCF matches the expected cumulative degree sequence.
fn verify_dcf(dcf: &DCF, expected: &[u64]) {
    assert_eq!(dcf_values(dcf), expected);
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
    let (_dir, basename) = roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&graph)
    })?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
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

/// Tests `build_dcf` on the cnr-2000 graph, verifying the [`BvGraphSeq`]
/// override against the default implementation (used by [`VecGraph`]).
#[test]
#[cfg(not(miri))]
fn test_build_dcf_cnr_2000() -> Result<()> {
    let seq = BvGraphSeq::with_basename(common::cnr_2000_basename())
        .endianness::<BE>()
        .load()?;
    let dcf = seq.build_dcf();
    assert_eq!(
        *dcf_values(&dcf).last().unwrap(),
        seq.get_num_arcs().unwrap()
    );
    assert_eq!(
        dcf_values(&dcf),
        dcf_values(&webgraph::graphs::vec_graph::VecGraph::from_lender(&seq).build_dcf())
    );
    Ok(())
}
