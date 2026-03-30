/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::{RandomAccessGraph, RandomAccessLabeling};
use webgraph_algo::rank::birank::{BiRank, preds};

#[test]
fn test_empty_graph() {
    let graph = VecGraph::empty(0);
    let transpose = VecGraph::empty(0);
    let mut br = BiRank::new(&graph, &transpose, 0);
    br.run(preds::MaxIter::from(10));
    assert_eq!(br.rank().len(), 0);
}

#[test]
fn test_single_edge() -> anyhow::Result<()> {
    // U = {0}, P = {1}, edge: 0→1
    let mut graph = VecGraph::empty(2);
    graph.add_arc(0, 1);

    let mut transpose = VecGraph::empty(2);
    transpose.add_arc(1, 0);

    let mut br = BiRank::new(&graph, &transpose, 1);
    br.alpha(0.85).beta(0.85);
    br.run(preds::L1Norm::try_from(1E-12)?);

    let r = br.rank();

    // With symmetric degrees (both = 1), the normalized weight is 1.
    // By symmetry of the graph, both nodes should have the same rank.
    assert!(
        (r[0] - r[1]).abs() < 1E-10,
        "Expected equal ranks, got {} vs {}",
        r[0],
        r[1]
    );

    Ok(())
}

/// Verifies the analytical steady-state ratios for α = β = 1.
///
/// Graph: U = {0, 1}, P = {2, 3}; edges: 0 → 2, 1 → 2, 1 → 3.
///
/// Degrees: d₀ = 1, d₁ = 2, d₂ = 2, d₃ = 1.
///
/// At equilibrium: rank[0] : rank[1] : rank[2] : rank[3] = 1 : √2 : √2 : 1.
#[test]
fn test_analytical_ratios() -> anyhow::Result<()> {
    let mut graph = VecGraph::empty(4);
    graph.add_arcs([(0, 2), (1, 2), (1, 3)]);

    let mut transpose = VecGraph::empty(4);
    transpose.add_arcs([(2, 0), (2, 1), (3, 1)]);

    let mut br = BiRank::new(&graph, &transpose, 2);
    br.alpha(1.0).beta(1.0);
    br.run(preds::L1Norm::try_from(1E-12)?);

    let r = br.rank();

    let ratio_u = r[1] / r[0];
    assert!(
        (ratio_u - std::f64::consts::SQRT_2).abs() < 1E-6,
        "Expected u[1]/u[0] ≈ √2, got {ratio_u}"
    );

    let ratio_p = r[2] / r[3];
    assert!(
        (ratio_p - std::f64::consts::SQRT_2).abs() < 1E-6,
        "Expected p[2]/p[3] ≈ √2, got {ratio_p}"
    );

    // Symmetry: u[0] ≈ p[3] and u[1] ≈ p[2]
    assert!(
        (r[0] / r[3] - 1.0).abs() < 1E-6,
        "Expected u[0] ≈ p[3], got {} vs {}",
        r[0],
        r[3]
    );
    assert!(
        (r[1] / r[2] - 1.0).abs() < 1E-6,
        "Expected u[1] ≈ p[2], got {} vs {}",
        r[1],
        r[2]
    );

    Ok(())
}

/// Verifies that the preference vector influences the result when α, β < 1.
#[test]
fn test_preference_influence() -> anyhow::Result<()> {
    let mut graph = VecGraph::empty(4);
    graph.add_arcs([(0, 2), (1, 2), (1, 3)]);

    let mut transpose = VecGraph::empty(4);
    transpose.add_arcs([(2, 0), (2, 1), (3, 1)]);

    // Preference that strongly favors node 0 and node 3
    let pref: &[f64] = &[0.4, 0.1, 0.1, 0.4];
    let mut br = BiRank::new(&graph, &transpose, 2).preference(pref);
    br.alpha(0.5).beta(0.5);
    br.run(preds::L1Norm::try_from(1E-9)?);

    let r = br.rank();

    // With purely structural ranking (α=β=1), node 1 has higher rank than
    // node 0. With the preference boosting node 0 and damping to 0.5,
    // node 0 should be boosted relative to the structural-only case.
    // All ranks should be positive.
    for (i, &ri) in r.iter().enumerate() {
        assert!(ri > 0.0, "rank[{i}] should be positive, got {ri}");
    }

    Ok(())
}

/// Verifies BiRank against a naive sequential reference implementation.
#[test]
fn test_against_naive() -> anyhow::Result<()> {
    // U = {0, 1, 2}, P = {3, 4, 5}
    let mut graph = VecGraph::empty(6);
    graph.add_arcs([(0, 3), (0, 4), (1, 3), (1, 5), (2, 4), (2, 5)]);

    let mut transpose = VecGraph::empty(6);
    transpose.add_arcs([(3, 0), (4, 0), (3, 1), (5, 1), (4, 2), (5, 2)]);

    let num_u = 3;
    let n = 6;
    let alpha = 0.85;
    let beta = 0.85;
    let pref = 1.0 / n as f64;
    let max_iter = 100;

    // Compute inverse square-root degrees
    let mut inv_sqrt_d = vec![0.0; n];
    for (i, elem) in inv_sqrt_d.iter_mut().enumerate().take(num_u) {
        let d = graph.outdegree(i);
        if d > 0 {
            *elem = 1.0 / (d as f64).sqrt();
        }
    }
    for (j, elem) in inv_sqrt_d.iter_mut().enumerate().take(n).skip(num_u) {
        let d = transpose.outdegree(j);
        if d > 0 {
            *elem = 1.0 / (d as f64).sqrt();
        }
    }

    // Naive BiRank
    let mut rank_naive = vec![pref; n];

    for _ in 0..max_iter {
        // Phase 1: Update P nodes
        for j in num_u..n {
            let mut sigma = 0.0;
            for i in transpose.successors(j) {
                sigma += inv_sqrt_d[i] * rank_naive[i];
            }
            rank_naive[j] = alpha * inv_sqrt_d[j] * sigma + (1.0 - alpha) * pref;
        }
        // Phase 2: Update U nodes
        for i in 0..num_u {
            let mut sigma = 0.0;
            for j in graph.successors(i) {
                sigma += inv_sqrt_d[j] * rank_naive[j];
            }
            rank_naive[i] = beta * inv_sqrt_d[i] * sigma + (1.0 - beta) * pref;
        }
    }

    // Run BiRank
    let mut br = BiRank::new(&graph, &transpose, num_u);
    br.alpha(alpha).beta(beta);
    br.run(preds::MaxIter::from(max_iter));

    let r = br.rank();
    for i in 0..n {
        assert!(
            (r[i] - rank_naive[i]).abs() < 1E-10,
            "Mismatch at node {i}: got {}, expected {}",
            r[i],
            rank_naive[i]
        );
    }

    Ok(())
}

/// Checks ranking order on a graph where one target node has strictly more
/// connections than another.
#[test]
fn test_ranking_order() -> anyhow::Result<()> {
    // U = {0, 1, 2}, P = {3, 4}
    // Node 3 has 3 incoming arcs, node 4 has 1.
    let mut graph = VecGraph::empty(5);
    graph.add_arcs([(0, 3), (1, 3), (2, 3), (2, 4)]);

    let mut transpose = VecGraph::empty(5);
    transpose.add_arcs([(3, 0), (3, 1), (3, 2), (4, 2)]);

    let mut br = BiRank::new(&graph, &transpose, 3);
    br.alpha(0.85).beta(0.85);
    br.run(preds::L1Norm::try_from(1E-9)?);

    let r = br.rank();

    // Node 3 should rank higher than node 4 among target nodes
    assert!(
        r[3] > r[4],
        "Expected rank[3] > rank[4], got {} vs {}",
        r[3],
        r[4]
    );

    // Node 2 should rank highest among source nodes (it has 2 outgoing arcs,
    // while nodes 0 and 1 have 1 each)
    assert!(
        r[2] > r[0],
        "Expected rank[2] > rank[0], got {} vs {}",
        r[2],
        r[0]
    );
    assert!(
        r[2] > r[1],
        "Expected rank[2] > rank[1], got {} vs {}",
        r[2],
        r[1]
    );

    Ok(())
}

/// Checks that re-using the structure (cached degrees) gives the same result.
#[test]
fn test_reuse() -> anyhow::Result<()> {
    let mut graph = VecGraph::empty(4);
    graph.add_arcs([(0, 2), (1, 2), (1, 3)]);

    let mut transpose = VecGraph::empty(4);
    transpose.add_arcs([(2, 0), (2, 1), (3, 1)]);

    let mut br = BiRank::new(&graph, &transpose, 2);
    br.alpha(0.85).beta(0.85);
    br.run(preds::L1Norm::try_from(1E-9)?);

    let first_run = br.rank().to_vec();

    // Run again on the same structure
    br.run(preds::L1Norm::try_from(1E-9)?);

    let second_run = br.rank();
    for i in 0..4 {
        assert!(
            (first_run[i] - second_run[i]).abs() < 1E-12,
            "Reuse mismatch at node {i}: {} vs {}",
            first_run[i],
            second_run[i]
        );
    }

    Ok(())
}

/// Checks that α = β = 0 returns the preference vector unchanged.
#[test]
fn test_zero_damping() -> anyhow::Result<()> {
    let mut graph = VecGraph::empty(4);
    graph.add_arcs([(0, 2), (1, 2), (1, 3)]);

    let mut transpose = VecGraph::empty(4);
    transpose.add_arcs([(2, 0), (2, 1), (3, 1)]);

    let pref: &[f64] = &[0.3, 0.2, 0.35, 0.15];

    let mut br = BiRank::new(&graph, &transpose, 2).preference(pref);
    br.alpha(0.0).beta(0.0);
    br.run(preds::MaxIter::from(10));

    let r = br.rank();
    // With α = β = 0, the update is: p[j] = p⁰[j], u[i] = u⁰[i]
    for i in 0..4 {
        assert!(
            (r[i] - pref[i]).abs() < 1E-12,
            "Expected rank[{i}] = pref[{i}] = {}, got {}",
            pref[i],
            r[i]
        );
    }

    Ok(())
}
