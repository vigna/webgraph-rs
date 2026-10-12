/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

mod common;

use anyhow::Result;
use common::symm_graph;
use predicates::Predicate;
use predicates::prelude::PredicateBooleanExt;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::SequentialLabeling;
use webgraph::utils::Granularity;
use webgraph_algo::llp;
use webgraph_algo::llp::preds::*;

/// Runs LLP with the identity functional permutation and checks that the
/// result contains a valid label for each node.
fn run_llp(
    graph: &VecGraph,
    gammas: Vec<f64>,
    seed: u64,
    predicate: impl Predicate<PredParams>,
) -> Result<Box<[usize]>> {
    let dir = tempfile::tempdir()?;
    let labels = llp::layered_label_propagation(
        graph,
        &graph.build_dcf(),
        gammas,
        Granularity::Nodes(100),
        seed,
        predicate,
        |_: usize, _: u64, _: u64| |x: u64| x,
        dir.path(),
    )?;
    check_labels(&labels, graph.num_nodes());
    Ok(labels)
}

/// Checks that there is a label smaller than the number of nodes for each node.
fn check_labels(labels: &[usize], num_nodes: usize) {
    assert_eq!(labels.len(), num_nodes);
    assert!(labels.iter().all(|&label| label < num_nodes), "{labels:?}");
}

#[test]
fn test_llp() -> Result<()> {
    // A square: 0 — 1, 0 — 2, 1 — 3, 2 — 3
    run_llp(
        &symm_graph(4, [(0, 1), (0, 2), (1, 3), (2, 3)]),
        vec![0.0, 1.0],
        42,
        MaxUpdates::from(1_usize),
    )?;
    // A star with center 0
    run_llp(
        &symm_graph(5, [(0, 1), (0, 2), (0, 3), (0, 4)]),
        vec![0.0, 0.5, 1.0, 2.0],
        7,
        MaxUpdates::from(2_usize),
    )?;
    // The empty graph
    run_llp(&VecGraph::new(), vec![0.0], 42, MaxUpdates::from(1_usize))?;
    Ok(())
}

#[test]
fn test_llp_labels_only_and_combine() -> Result<()> {
    // A path: 0 — 1 — 2 — 3 — 4
    let graph = symm_graph(5, [(0, 1), (1, 2), (2, 3), (3, 4)]);
    let dir = tempfile::tempdir()?;
    llp::layered_label_propagation_labels_only(
        &graph,
        &graph.build_dcf(),
        vec![0.0],
        Granularity::Nodes(100),
        123,
        MaxUpdates::from(1_usize),
        |_: usize, _: u64, _: u64| |x: u64| x,
        dir.path(),
    )?;
    check_labels(&llp::combine_labels(dir.path())?, graph.num_nodes());
    Ok(())
}

#[test]
fn test_llp_complete_graph_murmur() -> Result<()> {
    // K4
    let graph = symm_graph(4, [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
    let dir = tempfile::tempdir()?;
    let labels = llp::layered_label_propagation(
        &graph,
        &graph.build_dcf(),
        vec![0.0],
        Granularity::Arcs(100),
        0,
        MinGain::try_from(0.001)?.or(MaxUpdates::from(3_usize)),
        |n: usize, s0: u64, s1: u64| {
            let funcperm = funcperm::murmur(n as u64, s0, s1);
            move |x| funcperm.get(x)
        },
        dir.path(),
    )?;
    check_labels(&labels, 4);
    Ok(())
}
