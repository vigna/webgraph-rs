/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for the graph transformations, checked against graphs built directly
//! from the transformed arcs.

#![cfg(not(miri))]

mod common;

use anyhow::Result;
use common::{TEST_GRAPH_ARCS, test_graph, vec_graph as graph};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::no_logging;
use webgraph::graphs::vec_graph::{LabeledVecGraph, VecGraph};
use webgraph::prelude::*;

const BATCH: MemoryUsage = MemoryUsage::BatchSize(2);

/// Compresses the test graph into a temporary directory, so that parallel
/// transformations can be applied also to a sequential compressed graph, whose
/// split lenders are different from those of a [`VecGraph`].
macro_rules! compressed_test_graph {
    ($dir:ident, $seq:ident) => {
        let g = test_graph();
        let ($dir, basename) = common::roundtrip::<BE>(&g, |basename| {
            BvComp::with_basename(basename).comp_graph::<BE>(&g)
        })?;
        let $seq = BvGraphSeq::with_basename(&basename)
            .endianness::<BE>()
            .load()?;
    };
}

/// Collects the arcs returned by a set of parallel lenders, sorted.
fn par_arcs(
    graph: impl IntoParLenders<ParLender: for<'a> NodeLabelsLender<'a, Label = usize>>,
) -> Vec<(usize, usize)> {
    let mut arcs: Vec<_> = graph
        .into_par_lenders()
        .0
        .into_iter()
        .flat_map(|lender| lender.into_pairs())
        .collect();
    arcs.sort();
    arcs
}

/// Returns the arcs of a graph.
fn arcs(graph: &VecGraph) -> Vec<(usize, usize)> {
    graph.iter().into_pairs().collect()
}

// ── transpose ──

#[test]
fn test_transpose() -> Result<()> {
    let g = test_graph();
    let expected = graph(g.num_nodes(), TEST_GRAPH_ARCS.map(|(s, d)| (d, s)));

    let t = transpose_seq(&g, BATCH, no_logging![])?;
    let t = VecGraph::from_lender(&t);
    graph::eq(&t, &expected)?;

    let tt = transpose_seq(&t, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&tt), &g)?;

    let t = transpose_par(&g, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&t), &expected)?;
    compressed_test_graph!(_dir, seq);
    let t = transpose_par(&seq, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&t), &expected)?;
    Ok(())
}

#[test]
fn test_transpose_labeled() -> Result<()> {
    let label = |s: usize, d: usize| (10 * s + d) as u32;
    let g = LabeledVecGraph::from_arcs(TEST_GRAPH_ARCS.map(|(s, d)| ((s, d), label(s, d))));
    let expected = LabeledVecGraph::from_arcs(TEST_GRAPH_ARCS.map(|(s, d)| ((d, s), label(s, d))));
    let t = transpose_labeled_seq(&g, BATCH, FixedWidth::<u32>::new(), no_logging![])?;
    graph::eq_labeled(&LabeledVecGraph::from_lender(&t), &expected)?;

    // Unit labels, with the unit serializer
    let g = LabeledVecGraph::<()>::from_arcs(TEST_GRAPH_ARCS.map(|arc| (arc, ())));
    let expected = LabeledVecGraph::<()>::from_arcs(TEST_GRAPH_ARCS.map(|(s, d)| ((d, s), ())));
    let t = transpose_labeled_seq(&g, BATCH, (), no_logging![])?;
    graph::eq_labeled(&LabeledVecGraph::from_lender(&t), &expected)?;
    Ok(())
}

#[test]
fn test_transpose_par_cnr_2000() -> Result<()> {
    let graph = BvGraph::with_basename(common::cnr_2000_basename()).load()?;
    let transpose = BvGraphSeq::with_basename(common::cnr_2000_t_basename()).load()?;
    let sorted = transpose_par(&graph, MemoryUsage::from_perc(1.0), no_logging![])?;
    graph::eq(&sorted, &transpose)?;
    Ok(())
}

// ── permute ──

#[test]
fn test_permute() -> Result<()> {
    let g = test_graph();
    let perm = [3, 7, 0, 5, 1, 6, 2, 4];
    let expected = graph(
        g.num_nodes(),
        TEST_GRAPH_ARCS.map(|(s, d)| (perm[s], perm[d])),
    );

    let p = permute_seq(&g, &perm, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&p), &expected)?;

    let p = permute_par(&g, &perm, BATCH, no_logging![])?;
    assert_eq!(par_arcs(&p), arcs(&expected));
    compressed_test_graph!(_dir, seq);
    let p = permute_par(&seq, &perm, BATCH, no_logging![])?;
    assert_eq!(par_arcs(&p), arcs(&expected));

    let id: Vec<_> = (0..g.num_nodes()).collect();
    let p = permute_seq(&g, &id, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&p), &g)?;
    Ok(())
}

#[test]
fn test_permute_rejects_wrong_permutations() {
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    // 2 elements but graph has 3 nodes
    assert!(permute_seq(&g, &[0, 1], BATCH, no_logging![]).is_err());
    let g = VecGraph::from_arcs([(0, 1)]);
    assert!(permute_seq(&g, &[0, 999], BATCH, no_logging![]).is_err());
}

// ── symmetrize ──

/// Checks all symmetrization functions against the symmetrized arcs.
///
/// The functions for sorted graphs need a graph with cloneable split lenders,
/// so they are applied to a compressed copy of `g`.
fn check_symmetrize<const NO_LOOPS: bool>(g: &VecGraph) -> Result<()> {
    let expected = graph(
        g.num_nodes(),
        arcs(g)
            .into_iter()
            .flat_map(|(s, d)| [(s, d), (d, s)])
            .filter(|(s, d)| !NO_LOOPS || s != d),
    );

    let s = symmetrize_seq::<NO_LOOPS>(g, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&s), &expected)?;

    let s = symmetrize_par::<NO_LOOPS, _>(g, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&s), &expected)?;

    let (_dir, basename) = common::roundtrip::<BE>(g, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(g)
    })?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;

    let s = symmetrize_par::<NO_LOOPS, _>(&seq, BATCH, no_logging![])?;
    graph::eq(&VecGraph::from_lender(&s), &expected)?;

    let s = symmetrize_sorted_seq::<NO_LOOPS, _>(&seq, BATCH, no_logging![])?;
    assert_eq!(par_arcs(s), arcs(&expected));

    let s = symmetrize_sorted_par::<NO_LOOPS, _>(&seq, BATCH, no_logging![])?;
    assert_eq!(par_arcs(s), arcs(&expected));
    Ok(())
}

#[test]
fn test_symmetrize() -> Result<()> {
    // The test graph with two self-loops and a node of outdegree 0
    let g = VecGraph::from_arcs(TEST_GRAPH_ARCS.into_iter().chain([(3, 3), (6, 6), (9, 0)]));
    check_symmetrize::<true>(&g)?;
    check_symmetrize::<false>(&g)?;
    Ok(())
}

// ── map ──

#[test]
fn test_map() -> Result<()> {
    let g = test_graph();
    compressed_test_graph!(_dir, seq);
    for (map, num_nodes) in [
        // Identity
        (vec![0, 1, 2, 3, 4, 5, 6, 7], 8),
        // A permutation
        (vec![3, 7, 0, 5, 1, 6, 2, 4], 8),
        // Merges nodes (creating loops and duplicate arcs)
        (vec![0, 0, 1, 1, 2, 2, 3, 3], 4),
        // Enlarges the node space
        (vec![5, 10, 0, 3, 7, 1, 12, 2], 13),
    ] {
        let expected = graph(num_nodes, TEST_GRAPH_ARCS.map(|(s, d)| (map[s], map[d])));
        let m = map_seq(&g, &map, num_nodes, BATCH, no_logging![])?;
        graph::eq(&VecGraph::from_lender(&m), &expected)?;
        let m = map_par(&g, &map, num_nodes, BATCH, no_logging![])?;
        assert_eq!(par_arcs(&m), arcs(&expected));
        let m = map_par(&seq, &map, num_nodes, BATCH, no_logging![])?;
        assert_eq!(par_arcs(&m), arcs(&expected));
    }
    Ok(())
}

#[test]
fn test_map_rejects_wrong_maps() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    // 2 elements but graph has 3 nodes
    assert!(map_seq(&g, &[0, 1], 3, BATCH, no_logging![]).is_err());

    // Regression: the documented contract that num_nodes exceeds every map
    // value was unenforced, and only mapped sources were bounds-checked, so
    // an out-of-range mapped destination silently produced a graph whose
    // successors exceeded num_nodes.
    let g = VecGraph::from_arcs([(0, 1)]);
    assert!(map_seq(&g, &[0, 999], 2, BATCH, no_logging![]).is_err());
    assert!(map_par(&g, &[0, 999], 2, BATCH, no_logging![]).is_err());

    // The same through a compressed graph
    let (_dir, basename) = common::roundtrip::<BE>(&g, |basename| {
        BvComp::with_basename(basename).comp_graph::<BE>(&g)
    })?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert!(map_par(&seq, &[0, 999], 2, BATCH, no_logging![]).is_err());
    Ok(())
}
