/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod common;

use common::graph_and_transpose;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::{RandomAccessGraph, RandomAccessLabeling, SequentialLabeling};
use webgraph_algo::rank::birank::{BiRank, preds};

/// U = {0, 1}, P = {2, 3}; edges: 0 → 2, 1 → 2, 1 → 3.
fn small_bipartite() -> (VecGraph, VecGraph) {
    graph_and_transpose(4, [(0, 2), (1, 2), (1, 3)])
}

/// Asserts that two vectors are equal up to `tolerance` in each component.
fn assert_close(expected: &[f64], actual: &[f64], tolerance: f64) {
    assert_eq!(expected.len(), actual.len());
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert!(
            (e - a).abs() < tolerance,
            "Mismatch at node {i}: got {a}, expected {e}"
        );
    }
}

/// A naive sequential implementation of BiRank.
fn naive_birank(
    graph: &VecGraph,
    transpose: &VecGraph,
    num_u: usize,
    alpha: f64,
    beta: f64,
    pref: &[f64],
    iters: usize,
) -> Vec<f64> {
    let n = graph.num_nodes();
    // Inverse square-root degrees
    let inv_sqrt_d: Vec<f64> = (0..n)
        .map(|i| {
            let d = if i < num_u {
                graph.outdegree(i)
            } else {
                transpose.outdegree(i)
            };
            if d > 0 { 1.0 / (d as f64).sqrt() } else { 0.0 }
        })
        .collect();
    let mut rank = pref.to_vec();
    for _ in 0..iters {
        // Phase 1: update P nodes
        for j in num_u..n {
            let sigma: f64 = transpose
                .successors(j)
                .map(|i| inv_sqrt_d[i] * rank[i])
                .sum();
            rank[j] = alpha * inv_sqrt_d[j] * sigma + (1.0 - alpha) * pref[j];
        }
        // Phase 2: update U nodes
        for i in 0..num_u {
            let sigma: f64 = graph.successors(i).map(|j| inv_sqrt_d[j] * rank[j]).sum();
            rank[i] = beta * inv_sqrt_d[i] * sigma + (1.0 - beta) * pref[i];
        }
    }
    rank
}

#[test]
fn test_empty_graph() {
    let graph = VecGraph::empty(0);
    let mut br = BiRank::new(&graph, &graph, 0);
    br.run(preds::MaxIter::from(10));
    assert_eq!(br.rank().len(), 0);
}

#[test]
fn test_single_edge() -> anyhow::Result<()> {
    // U = {0}, P = {1}, edge: 0→1
    let (graph, transpose) = graph_and_transpose(2, [(0, 1)]);
    let mut br = BiRank::new(&graph, &transpose, 1);
    br.alpha(0.85).beta(0.85);
    br.run(preds::L1Norm::try_from(1E-12)?);
    // With symmetric degrees (both = 1), the normalized weight is 1.
    // By symmetry of the graph, both nodes should have the same rank.
    let r = br.rank();
    assert_close(&[r[0]], &[r[1]], 1E-10);
    Ok(())
}

/// Verifies the analytical steady-state ratios for α = β = 1.
///
/// Degrees: d₀ = 1, d₁ = 2, d₂ = 2, d₃ = 1.
///
/// At equilibrium: rank[0] : rank[1] : rank[2] : rank[3] = 1 : √2 : √2 : 1.
#[test]
fn test_analytical_ratios() -> anyhow::Result<()> {
    let (graph, transpose) = small_bipartite();
    let mut br = BiRank::new(&graph, &transpose, 2);
    br.alpha(1.0).beta(1.0);
    br.run(preds::L1Norm::try_from(1E-12)?);
    let r = br.rank();
    let sqrt_2 = std::f64::consts::SQRT_2;
    assert_close(
        &[sqrt_2, sqrt_2, 1.0, 1.0],
        &[r[1] / r[0], r[2] / r[3], r[0] / r[3], r[1] / r[2]],
        1E-6,
    );
    Ok(())
}

/// Verifies BiRank against a naive sequential reference implementation,
/// with uniform and nonuniform preference.
#[test]
fn test_against_naive() -> anyhow::Result<()> {
    // U = {0, 1, 2}, P = {3, 4, 5}
    let (graph, transpose) =
        graph_and_transpose(6, [(0, 3), (0, 4), (1, 3), (1, 5), (2, 4), (2, 5)]);
    let uniform = vec![1.0 / 6.0; 6];
    let mut br = BiRank::new(&graph, &transpose, 3);
    br.alpha(0.85).beta(0.85);
    br.run(preds::MaxIter::from(100));
    assert_close(
        &naive_birank(&graph, &transpose, 3, 0.85, 0.85, &uniform, 100),
        br.rank(),
        1E-10,
    );

    // A preference that strongly favors node 0 and node 3
    let (graph, transpose) = small_bipartite();
    let pref: &[f64] = &[0.4, 0.1, 0.1, 0.4];
    let mut br = BiRank::new(&graph, &transpose, 2).preference(pref);
    br.alpha(0.5).beta(0.5);
    br.run(preds::MaxIter::from(100));
    assert_close(
        &naive_birank(&graph, &transpose, 2, 0.5, 0.5, pref, 100),
        br.rank(),
        1E-10,
    );
    Ok(())
}

/// Checks ranking order on a graph where one target node has strictly more
/// connections than another.
#[test]
fn test_ranking_order() -> anyhow::Result<()> {
    // U = {0, 1, 2}, P = {3, 4}
    // Node 3 has 3 incoming arcs, node 4 has 1.
    let (graph, transpose) = graph_and_transpose(5, [(0, 3), (1, 3), (2, 3), (2, 4)]);
    let mut br = BiRank::new(&graph, &transpose, 3);
    br.alpha(0.85).beta(0.85);
    br.run(preds::L1Norm::try_from(1E-9)?);
    let r = br.rank();
    // Node 3 should rank higher than node 4 among target nodes
    assert!(r[3] > r[4], "Expected rank[3] > rank[4], got {r:?}");
    // Node 2 should rank highest among source nodes (it has 2 outgoing arcs,
    // while nodes 0 and 1 have 1 each)
    assert!(
        r[2] > r[0] && r[2] > r[1],
        "Expected rank[2] maximal, got {r:?}"
    );
    Ok(())
}

/// Checks that re-using the structure (cached degrees) gives the same result.
#[test]
fn test_reuse() -> anyhow::Result<()> {
    let (graph, transpose) = small_bipartite();
    let mut br = BiRank::new(&graph, &transpose, 2);
    br.alpha(0.85).beta(0.85);
    br.run(preds::L1Norm::try_from(1E-9)?);
    let first_run = br.rank().to_vec();
    // Run again on the same structure
    br.run(preds::L1Norm::try_from(1E-9)?);
    assert_close(&first_run, br.rank(), 1E-12);
    Ok(())
}

/// Checks that α = β = 0 returns the preference vector unchanged.
#[test]
fn test_zero_damping() -> anyhow::Result<()> {
    let (graph, transpose) = small_bipartite();
    let pref: &[f64] = &[0.3, 0.2, 0.35, 0.15];
    let mut br = BiRank::new(&graph, &transpose, 2).preference(pref);
    br.alpha(0.0).beta(0.0);
    br.run(preds::MaxIter::from(10));
    // With α = β = 0, the update is: p[j] = p⁰[j], u[i] = u⁰[i]
    assert_close(pref, br.rank(), 1E-12);
    Ok(())
}

#[test]
fn test_degenerate_bipartitions() {
    // Regression: with all nodes on one side, run() returned before
    // initializing the rank vector from the preference, leaving all zeros.
    for num_sources in [0, 1] {
        let graph = VecGraph::empty(1);
        let mut br = BiRank::new(&graph, &graph, num_sources);
        br.run(preds::MaxIter::from(1));
        // Zero iterations happen; the rank is the (uniform) preference.
        assert_eq!(br.rank(), &[1.0]);
    }
}

#[test]
#[should_panic(expected = "negative or non-finite")]
fn test_preference_rejects_nan() {
    // Regression: NaN query vectors were accepted and made threshold-based
    // stopping criteria unsatisfiable.
    let (graph, transpose) = graph_and_transpose(2, [(0, 1)]);
    let _ = BiRank::new(&graph, &transpose, 1).preference([f64::NAN, 0.5].as_slice());
}
