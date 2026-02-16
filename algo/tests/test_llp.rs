/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use lender::prelude::*;
use predicates::prelude::PredicateBooleanExt;
use sux::prelude::*;
use webgraph::graphs::bvgraph::DCF;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::SequentialLabeling;
use webgraph_algo::llp;
use webgraph_algo::llp::preds::*;

/// Builds a degree cumulative function (DCF) from a graph.
///
/// The DCF is an Elias-Fano representation of the sequence
/// 0, d₀, d₀+d₁, ..., total_arcs where dᵢ is the outdegree of node i.
fn build_dcf(graph: &VecGraph) -> DCF {
    let num_nodes = graph.num_nodes();
    let num_arcs = graph.num_arcs_hint().unwrap_or(0) as usize;

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, num_arcs);
    efb.push(0);
    let mut cumul = 0usize;
    let mut lender = graph.iter();
    while let Some((_node, succs)) = lender.next() {
        cumul += succs.into_iter().count();
        efb.push(cumul);
    }

    let ef = efb.build();
    unsafe {
        ef.map_high_bits(|bits| {
            SelectZeroAdaptConst::<_, _, 12, 4>::new(SelectAdaptConst::<_, _, 12, 4>::new(bits))
        })
    }
}

#[test]
fn test_llp_small_symmetric_graph() -> Result<()> {
    use webgraph::utils::Granularity;

    // Create a small symmetric graph (square)
    //   0 — 1
    //   |   |
    //   2 — 3
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (0, 2),
        (2, 0),
        (1, 3),
        (3, 1),
        (2, 3),
        (3, 2),
    ]);
    let num_nodes = graph.num_nodes();
    assert_eq!(num_nodes, 4);

    let deg_cumul = build_dcf(&graph);

    let dir = tempfile::tempdir()?;
    let gammas = vec![0.0, 1.0];

    let predicate = MaxUpdates::from(1_usize);

    let labels = llp::layered_label_propagation(
        &graph,
        &deg_cumul,
        gammas,
        Some(100),
        Granularity::Nodes(100),
        42,
        predicate,
        dir.path(),
    )?;

    assert_eq!(labels.len(), num_nodes);
    for &label in labels.iter() {
        assert!(label < num_nodes, "Label {label} >= num_nodes {num_nodes}");
    }
    Ok(())
}

#[test]
fn test_llp_labels_only_and_combine() -> Result<()> {
    use webgraph::utils::Granularity;

    // Small path graph: 0 — 1 — 2 — 3 — 4
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 3),
        (3, 2),
        (3, 4),
        (4, 3),
    ]);
    let num_nodes = graph.num_nodes();
    let deg_cumul = build_dcf(&graph);

    let dir = tempfile::tempdir()?;

    llp::layered_label_propagation_labels_only(
        &graph,
        &deg_cumul,
        vec![0.0],
        None,
        Granularity::Nodes(100),
        123,
        MaxUpdates::from(1_usize),
        dir.path(),
    )?;

    let labels = llp::combine_labels(dir.path())?;
    assert_eq!(labels.len(), num_nodes);
    Ok(())
}

#[test]
fn test_llp_multiple_gammas() -> Result<()> {
    use webgraph::utils::Granularity;

    // Star graph: 0 connected to all others
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (0, 2),
        (2, 0),
        (0, 3),
        (3, 0),
        (0, 4),
        (4, 0),
    ]);
    let deg_cumul = build_dcf(&graph);

    let dir = tempfile::tempdir()?;
    let gammas = vec![0.0, 0.5, 1.0, 2.0];

    let labels = llp::layered_label_propagation(
        &graph,
        &deg_cumul,
        gammas,
        Some(100),
        Granularity::Nodes(100),
        7,
        MaxUpdates::from(2_usize),
        dir.path(),
    )?;

    assert_eq!(labels.len(), 5);
    Ok(())
}

#[test]
fn test_llp_complete_graph() -> Result<()> {
    use webgraph::utils::Granularity;

    // K4 complete graph (all symmetric)
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (0, 2),
        (2, 0),
        (0, 3),
        (3, 0),
        (1, 2),
        (2, 1),
        (1, 3),
        (3, 1),
        (2, 3),
        (3, 2),
    ]);
    let deg_cumul = build_dcf(&graph);

    let dir = tempfile::tempdir()?;

    let predicate = MinGain::try_from(0.001)?.or(MaxUpdates::from(3_usize));

    let labels = llp::layered_label_propagation(
        &graph,
        &deg_cumul,
        vec![0.0],
        Some(10),
        Granularity::Arcs(100),
        0,
        predicate,
        dir.path(),
    )?;

    assert_eq!(labels.len(), 4);
    Ok(())
}
