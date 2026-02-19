/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use webgraph::graphs::random::ErdosRenyi;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::{RandomAccessGraph, RandomAccessLabeling, SequentialLabeling};
use webgraph_algo::rank::pagerank::{PageRank, preds};

/// Builds a transpose graph consisting of a k-clique (nodes 0..k) and a
/// p-cycle (nodes k..k+p) with optional bridge arcs between node k−1 and
/// node k.
///
/// The bridge type is one of:
/// - `"bi"`:      arcs k−1 → k and k → k−1 (in the transpose)
/// - `"back"`:    arc k−1 → k only
/// - `"forward"`: arc k → k−1 only
/// - `"none"`:    no bridge
fn build_clique_cycle_transpose(p: usize, k: usize, bridge: &str) -> VecGraph {
    let n = p + k;
    let mut arcs = Vec::new();

    // Complete directed clique on nodes 0..k (in transpose)
    for i in 0..k {
        for j in 0..k {
            if i != j {
                arcs.push((i, j));
            }
        }
    }

    // Directed cycle on nodes k..k+p (in transpose):
    // arc from k+(i+1)%p to k+i
    for i in 0..p {
        arcs.push((k + (i + 1) % p, k + i));
    }

    // Bridge arcs
    match bridge {
        "bi" => {
            arcs.push((k - 1, k));
            arcs.push((k, k - 1));
        }
        "back" => {
            arcs.push((k - 1, k));
        }
        "forward" => {
            arcs.push((k, k - 1));
        }
        "none" => {}
        _ => panic!("Unknown bridge type: {bridge}"),
    }

    let mut g = VecGraph::empty(n);
    g.add_arcs(arcs);
    g
}

/// Returns the 𝓁-∞ distance (maximum absolute difference) between two vectors.
fn l_inf_distance(a: &[f64], b: &[f64]) -> f64 {
    assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).abs())
        .fold(0.0, f64::max)
}

/// Computes the transpose of a graph.
fn transpose(g: &VecGraph) -> VecGraph {
    let n = g.num_nodes();
    let mut arcs = Vec::new();
    for i in 0..n {
        for j in g.successors(i) {
            arcs.push((j, i));
        }
    }
    let mut t = VecGraph::empty(n);
    t.add_arcs(arcs);
    t
}

#[test]
fn test_empty() {
    let g = VecGraph::empty(0);
    let mut pr = PageRank::new(&g);
    pr.run(preds::L1Norm::try_from(1E-15).unwrap());
    assert!(pr.rank().is_empty());
}

/// Tests PageRank on a graph (given as its transpose) made of a *k*-clique
/// (nodes 0..*k*) and a directed *p*-cycle (nodes *k*..*k* + *p*), connected
/// by a bidirectional bridge between nodes *k* − 1 and *k*. The expected
/// rank values are derived analytically in terms of the rank at node *k* − 1.
#[test]
fn test_clique_bidi_bridge_cycle() {
    for threshold in (1..=9).map(|e| f64::powi(10.0, -e)) {
        for &p in &[10, 50, 100] {
            for &k in &[10, 50, 100] {
                let g = build_clique_cycle_transpose(p, k, "bi");
                let n = g.num_nodes();
                assert_eq!(n, p + k);

                for &alpha in &[0.25, 0.50, 0.75] {
                    let mut pr = PageRank::new(&g);
                    pr.alpha(alpha);
                    pr.run(preds::L1Norm::try_from(threshold / 10.0).unwrap());

                    let r = pr.rank()[k - 1] * n as f64;
                    let mut expected = vec![0.0; n];

                    expected[k - 1] = r;
                    let clique_rank = (k - 1) as f64 * (k as f64 - alpha * k as f64 + alpha * r)
                        / (k as f64 * ((k - 1) as f64 - alpha * (k - 2) as f64));
                    expected[..k - 1].fill(clique_rank);
                    expected[k] = 2.0
                        + 2.0 * (alpha * r - k as f64) / (k as f64 * (2.0 - alpha.powi(p as i32)));
                    for d in 1..p {
                        expected[k + d] = 1.0
                            + alpha.powi(d as i32) * (alpha * r - k as f64)
                                / (k as f64 * (2.0 - alpha.powi(p as i32)));
                    }
                    for v in expected.iter_mut() {
                        *v /= n as f64;
                    }

                    assert!(
                        l_inf_distance(&expected, pr.rank()) < threshold,
                        "bidi bridge p={p} k={k} alpha={alpha} threshold={threshold}: L∞={}",
                        l_inf_distance(&expected, pr.rank())
                    );
                }
            }
        }
    }
}

/// Tests PageRank on a graph (given as its transpose) made of a *k*-clique
/// (nodes 0..*k*) and a directed *p*-cycle (nodes *k*..*k* + *p*), connected
/// by a single arc from node *k* − 1 to node *k* in the transpose (i.e., from
/// *k* to *k* − 1 in the original graph—a "backward" bridge from the cycle to
/// the clique). The expected rank values are fully analytical.
#[test]
fn test_clique_back_bridge_cycle() {
    for threshold in (1..=9).map(|e| f64::powi(10.0, -e)) {
        for &p in &[10, 50, 100] {
            for &k in &[10, 50, 100] {
                let g = build_clique_cycle_transpose(p, k, "back");
                let n = g.num_nodes();

                for &alpha in &[0.25, 0.50, 0.75] {
                    let mut pr = PageRank::new(&g);
                    pr.alpha(alpha);
                    pr.run(preds::L1Norm::try_from(threshold / 10.0).unwrap());

                    let mut expected = vec![0.0; n];

                    let kf = k as f64;

                    let clique_rank = (2.0 * (kf - 1.0) - 2.0 * (kf - 2.0) * alpha - alpha * alpha)
                        / (2.0 * (1.0 - alpha) * (kf - 1.0 + alpha))
                        - alpha.powi(p as i32 + 2)
                            / (2.0
                                * (1.0 - alpha)
                                * (kf - 1.0 + alpha)
                                * (2.0 - alpha.powi(p as i32)));
                    expected[..k - 1].fill(clique_rank);

                    expected[k - 1] = (2.0 * (kf - 1.0) - (kf - 3.0) * alpha - alpha * alpha * kf)
                        / (2.0 * (1.0 - alpha) * (kf - 1.0 + alpha))
                        - alpha.powi(p as i32 + 1) * (kf - 1.0 - alpha * (kf - 2.0))
                            / (2.0
                                * (1.0 - alpha)
                                * (kf - 1.0 + alpha)
                                * (2.0 - alpha.powi(p as i32)));

                    for d in 0..p {
                        let exp = if d == 0 { p } else { d };
                        expected[k + d] =
                            1.0 - alpha.powi(exp as i32) / (2.0 - alpha.powi(p as i32));
                    }

                    for v in expected.iter_mut() {
                        *v /= n as f64;
                    }

                    assert!(
                        l_inf_distance(&expected, pr.rank()) < threshold,
                        "backbridge p={p} k={k} alpha={alpha} threshold={threshold}: L∞={}",
                        l_inf_distance(&expected, pr.rank())
                    );
                }
            }
        }
    }
}

/// Tests PageRank on a graph (given as its transpose) made of a *k*-clique
/// (nodes 0..*k*) and a directed *p*-cycle (nodes *k*..*k* + *p*), connected
/// by a single arc from node *k* to node *k* − 1 in the transpose (i.e., from
/// *k* − 1 to *k* in the original graph—a "forward" bridge from the clique to
/// the cycle). The expected rank values are fully analytical.
#[test]
fn test_clique_forward_bridge_cycle() {
    for threshold in (1..=9).map(|e| f64::powi(10.0, -e)) {
        for &p in &[10, 50, 100] {
            for &k in &[10, 50, 100] {
                let g = build_clique_cycle_transpose(p, k, "forward");
                let n = g.num_nodes();

                for &alpha in &[0.25, 0.50, 0.75] {
                    let mut pr = PageRank::new(&g);
                    pr.alpha(alpha);
                    pr.run(preds::L1Norm::try_from(threshold / 10.0).unwrap());

                    let mut expected = vec![0.0; n];

                    let kf = k as f64;
                    let denom = (kf - alpha * alpha) * (kf - 1.0) - alpha * kf * (kf - 2.0);

                    let clique_rank = (1.0 - alpha) * (alpha + kf) * (kf - 1.0) / denom;
                    expected[..k - 1].fill(clique_rank);

                    expected[k - 1] = kf * (1.0 - alpha) * (kf - 1.0 + alpha) / denom;

                    for d in 0..p {
                        expected[k + d] = 1.0
                            + (alpha.powi(d as i32 + 1) * (1.0 - alpha) * (kf - 1.0 + alpha))
                                / ((1.0 - alpha.powi(p as i32)) * denom);
                    }

                    for v in expected.iter_mut() {
                        *v /= n as f64;
                    }

                    assert!(
                        l_inf_distance(&expected, pr.rank()) < threshold,
                        "forward_bridge p={p} k={k} alpha={alpha} threshold={threshold}: L∞={}",
                        l_inf_distance(&expected, pr.rank())
                    );
                }
            }
        }
    }
}

/// Tests PageRank on a graph (given as its transpose) made of a *k*-clique
/// (nodes 0..*k*) and a directed *p*-cycle (nodes *k*..*k* + *p*), with no
/// bridge between the two components. Since the graph is disconnected (both
/// components are strongly connected individually), the expected PageRank is
/// uniform: 1 / (*k* + *p*) for every node.
#[test]
fn test_clique_no_bridge_cycle() {
    for threshold in (1..=9).map(|e| f64::powi(10.0, -e)) {
        for &p in &[10, 50, 100] {
            for &k in &[10, 50, 100] {
                let g = build_clique_cycle_transpose(p, k, "none");
                let n = g.num_nodes();

                for &alpha in &[0.25, 0.50, 0.75] {
                    let mut pr = PageRank::new(&g);
                    pr.alpha(alpha);
                    pr.run(preds::L1Norm::try_from(threshold / 10.0).unwrap());

                    let expected = vec![1.0 / n as f64; n];

                    assert!(
                        l_inf_distance(&expected, pr.rank()) < threshold,
                        "no_bridge p={p} k={k} alpha={alpha} threshold={threshold}: L∞={}",
                        l_inf_distance(&expected, pr.rank())
                    );
                }
            }
        }
    }
}

// ── Erdős–Rényi tests against the power method ──────────────────────────

/// Generates a random stochastic vector of length *n*.
fn random_stochastic_vector(n: usize, rng: &mut impl Rng) -> Vec<f64> {
    let mut v: Vec<f64> = (0..n).map(|_| rng.random::<f64>()).collect();
    let sum: f64 = v.iter().sum();
    for x in &mut v {
        *x /= sum;
    }
    v
}

/// Computes PageRank using the standard power method (sequential,
/// non-in-place) on the original (non-transposed) graph.
///
/// Returns the rank vector after convergence.
fn power_method(
    graph: &VecGraph,
    alpha: f64,
    preference: Option<&[f64]>,
    dangling_distribution: Option<&[f64]>,
    pseudo_rank: bool,
) -> Vec<f64> {
    let n = graph.num_nodes();
    let inv_n = 1.0 / n as f64;
    let threshold = 1E-15;

    let mut rank = match preference {
        Some(v) => v.to_vec(),
        None => vec![inv_n; n],
    };

    // Resolve effective dangling distribution
    let effective_dangling: Option<&[f64]> = if pseudo_rank {
        None
    } else {
        dangling_distribution.or(preference)
    };

    loop {
        let mut new_rank = vec![0.0; n];

        // Scatter rank[j] / outdegree(j) to each successor of j
        let mut dangling_rank: f64 = 0.0;
        for j in 0..n {
            let d = graph.outdegree(j);
            if d == 0 {
                if !pseudo_rank {
                    dangling_rank += rank[j];
                }
            } else {
                let contrib = rank[j] / d as f64;
                for i in graph.successors(j) {
                    new_rank[i] += contrib;
                }
            }
        }

        // Add dangling-node and preference contributions
        for i in 0..n {
            let v_i = match preference {
                Some(v) => v[i],
                None => inv_n,
            };

            if !pseudo_rank {
                let u_i = match effective_dangling {
                    Some(u) => u[i],
                    None => inv_n,
                };
                new_rank[i] += dangling_rank * u_i;
            }

            new_rank[i] = (1.0 - alpha) * v_i + alpha * new_rank[i];
        }

        let diff: f64 = rank
            .iter()
            .zip(new_rank.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();
        rank = new_rank;
        if diff * alpha / (1.0 - alpha) < threshold {
            break;
        }
    }

    rank
}

/// Tests Gauss–Seidel PageRank against the power method on random
/// Erdős–Rényi directed graphs of size 10 (*p* = 0.5), 100 (*p* = 0.1), and
/// 1000 (*p* = 0.01). Five configurations are exercised for each graph:
///
/// 1. **Weakly preferential, uniform preference** (default): the
///    dangling-node distribution and preference are both uniform.
/// 2. **Strongly preferential, non-uniform preference**: a random stochastic
///    preference vector is used as both the preference and the dangling-node
///    distribution.
/// 3. **Weakly preferential, non-uniform preference**: a random stochastic
///    preference vector is used, but the dangling-node distribution is
///    explicitly set to uniform.
/// 4. **Pseudo-rank, uniform preference**: dangling-node contribution is
///    zeroed out.
/// 5. **Pseudo-rank, non-uniform preference**: dangling-node contribution is
///    zeroed out with a non-uniform preference.
#[test]
fn test_erdos_renyi_vs_power_method() {
    let tolerance = 1E-15;
    let gs_threshold = 1E-15;

    for &(n, arc_p, seed) in &[(10, 0.5, 0u64), (100, 0.1, 1), (1000, 0.01, 2)] {
        let mut rng = SmallRng::seed_from_u64(seed);
        // g is the original graph; gt is its transpose (passed to PageRank)
        let g = VecGraph::from_lender(ErdosRenyi::new(n, arc_p, seed).iter());
        let gt = transpose(&g);
        let pref = random_stochastic_vector(n, &mut rng);
        let uniform: Vec<f64> = vec![1.0 / n as f64; n];

        for &alpha in &[0.25, 0.50, 0.85] {
            // 1. Weakly preferential, uniform preference (default)
            {
                let expected = power_method(&g, alpha, None, None, false);
                let mut pr = PageRank::new(&gt);
                pr.alpha(alpha);
                pr.run(preds::L1Norm::try_from(gs_threshold).unwrap());
                assert!(
                    l_inf_distance(&expected, pr.rank()) < tolerance,
                    "weakly-uniform n={n} alpha={alpha}: L∞={}",
                    l_inf_distance(&expected, pr.rank())
                );
            }

            // 2. Strongly preferential, non-uniform preference
            {
                let expected = power_method(&g, alpha, Some(&pref), None, false);
                let mut pr = PageRank::new(&gt);
                pr.alpha(alpha).preference(Some(&pref));
                pr.run(preds::L1Norm::try_from(gs_threshold).unwrap());
                assert!(
                    l_inf_distance(&expected, pr.rank()) < tolerance,
                    "strongly-nonuniform n={n} alpha={alpha}: L∞={}",
                    l_inf_distance(&expected, pr.rank())
                );
            }

            // 3. Weakly preferential, non-uniform preference
            {
                let expected = power_method(&g, alpha, Some(&pref), Some(&uniform), false);
                let mut pr = PageRank::new(&gt);
                pr.alpha(alpha)
                    .preference(Some(&pref))
                    .dangling_distribution(Some(&uniform));
                pr.run(preds::L1Norm::try_from(gs_threshold).unwrap());
                assert!(
                    l_inf_distance(&expected, pr.rank()) < tolerance,
                    "weakly-nonuniform n={n} alpha={alpha}: L∞={}",
                    l_inf_distance(&expected, pr.rank())
                );
            }

            // 4. Pseudo-rank, uniform preference
            {
                let expected = power_method(&g, alpha, None, None, true);
                let mut pr = PageRank::new(&gt);
                pr.alpha(alpha).pseudo_rank(true);
                pr.run(preds::L1Norm::try_from(gs_threshold).unwrap());
                assert!(
                    l_inf_distance(&expected, pr.rank()) < tolerance,
                    "pseudo-uniform n={n} alpha={alpha}: L∞={}",
                    l_inf_distance(&expected, pr.rank())
                );
            }

            // 5. Pseudo-rank, non-uniform preference
            {
                let expected = power_method(&g, alpha, Some(&pref), None, true);
                let mut pr = PageRank::new(&gt);
                pr.alpha(alpha).preference(Some(&pref)).pseudo_rank(true);
                pr.run(preds::L1Norm::try_from(gs_threshold).unwrap());
                assert!(
                    l_inf_distance(&expected, pr.rank()) < tolerance,
                    "pseudo-nonuniform n={n} alpha={alpha}: L∞={}",
                    l_inf_distance(&expected, pr.rank())
                );
            }
        }
    }
}
