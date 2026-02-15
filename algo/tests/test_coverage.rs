/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests targeting uncovered code paths to increase code coverage.

use anyhow::Result;
use dsi_progress_logger::no_logging;
use lender::prelude::*;
use predicates::Predicate;
use predicates::prelude::PredicateBooleanExt;
use sux::prelude::*;
use webgraph::graphs::bvgraph::DCF;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::SequentialLabeling;
use webgraph::transform;
use webgraph::utils::MemoryUsage;
use webgraph_algo::distances::exact_sum_sweep::{self, Level};
use webgraph_algo::llp;
use webgraph_algo::llp::preds::*;
use webgraph_algo::prelude::*;
use webgraph_algo::sccs::{self, Sccs};

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

// ==================== LLP Predicates ====================

#[test]
fn test_max_updates_eval_below() {
    let pred = MaxUpdates::from(10_usize);
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 5,
    };
    assert!(!pred.eval(&params));
}

#[test]
fn test_max_updates_eval_at_limit() {
    let pred = MaxUpdates::from(10_usize);
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 9, // 9 + 1 >= 10
    };
    assert!(pred.eval(&params));
}

#[test]
fn test_max_updates_eval_above() {
    let pred = MaxUpdates::from(10_usize);
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 15,
    };
    assert!(pred.eval(&params));
}

#[test]
fn test_max_updates_from_none() {
    let pred = MaxUpdates::from(None);
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 1_000_000,
    };
    // With None (default = usize::MAX), should not trigger easily
    assert!(!pred.eval(&params));
}

#[test]
fn test_max_updates_default() {
    let pred = MaxUpdates::default();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 0,
    };
    assert!(!pred.eval(&params));
}

#[test]
fn test_max_updates_display() {
    let pred = MaxUpdates::from(42_usize);
    let s = format!("{}", pred);
    assert!(s.contains("42"));
}

#[test]
fn test_min_gain_eval_below_threshold() {
    let pred = MinGain::try_from(0.01).unwrap();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.005,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 0,
    };
    assert!(pred.eval(&params)); // gain <= threshold → stop
}

#[test]
fn test_min_gain_eval_above_threshold() {
    let pred = MinGain::try_from(0.01).unwrap();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.05,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 0,
    };
    assert!(!pred.eval(&params)); // gain > threshold → continue
}

#[test]
fn test_min_gain_default() {
    let pred = MinGain::default();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0001,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 0,
    };
    assert!(pred.eval(&params)); // 0.0001 <= 0.001 (default threshold)
}

#[test]
fn test_min_gain_from_none() {
    let pred = MinGain::try_from(None).unwrap();
    // None → default threshold (0.001)
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0005,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 0,
    };
    assert!(pred.eval(&params));
}

#[test]
fn test_min_gain_negative_threshold() {
    assert!(MinGain::try_from(-0.01).is_err());
}

#[test]
fn test_min_gain_nan_threshold() {
    assert!(MinGain::try_from(f64::NAN).is_err());
}

#[test]
fn test_min_gain_display() {
    let pred = MinGain::try_from(0.05).unwrap();
    let s = format!("{}", pred);
    assert!(s.contains("0.05"));
}

#[test]
fn test_min_avg_improv_eval() {
    let pred = MinAvgImprov::try_from(0.1).unwrap();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.05, // below threshold
        modified: 0,
        update: 0,
    };
    assert!(pred.eval(&params)); // 0.05 <= 0.1 → stop
}

#[test]
fn test_min_avg_improv_above_threshold() {
    let pred = MinAvgImprov::try_from(0.1).unwrap();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.5, // above threshold
        modified: 0,
        update: 0,
    };
    assert!(!pred.eval(&params)); // 0.5 > 0.1 → continue
}

#[test]
fn test_min_avg_improv_default() {
    let pred = MinAvgImprov::default();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.05,
        modified: 0,
        update: 0,
    };
    assert!(pred.eval(&params)); // 0.05 <= 0.1 (default)
}

#[test]
fn test_min_avg_improv_from_none() {
    let pred = MinAvgImprov::try_from(None).unwrap();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.05,
        modified: 0,
        update: 0,
    };
    assert!(pred.eval(&params));
}

#[test]
fn test_min_avg_improv_nan() {
    assert!(MinAvgImprov::try_from(f64::NAN).is_err());
}

#[test]
fn test_min_avg_improv_display() {
    let pred = MinAvgImprov::try_from(0.25).unwrap();
    let s = format!("{}", pred);
    assert!(s.contains("0.25"));
}

#[test]
fn test_min_modified_eval_below_sqrt() {
    let pred = MinModified::default();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 5, // 5 <= sqrt(100) = 10
        update: 0,
    };
    assert!(pred.eval(&params));
}

#[test]
fn test_min_modified_eval_above_sqrt() {
    let pred = MinModified::default();
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 50, // 50 > sqrt(100) = 10
        update: 0,
    };
    assert!(!pred.eval(&params));
}

#[test]
fn test_min_modified_display() {
    let pred = MinModified::default();
    let s = format!("{}", pred);
    assert!(s.contains("√n"));
}

#[test]
fn test_perc_modified_eval() {
    let pred = PercModified::try_from(10.0).unwrap(); // 10%
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 5, // 5 <= 100 * 0.1 = 10
        update: 0,
    };
    assert!(pred.eval(&params));
}

#[test]
fn test_perc_modified_above() {
    let pred = PercModified::try_from(10.0).unwrap(); // 10%
    let params = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0,
        avg_gain_impr: 0.0,
        modified: 20, // 20 > 100 * 0.1 = 10
        update: 0,
    };
    assert!(!pred.eval(&params));
}

#[test]
fn test_perc_modified_display() {
    let pred = PercModified::try_from(15.0).unwrap();
    let s = format!("{}", pred);
    assert!(s.contains("15"));
}

#[test]
fn test_perc_modified_negative() {
    assert!(PercModified::try_from(-1.0).is_err());
}

#[test]
fn test_perc_modified_over_100() {
    assert!(PercModified::try_from(101.0).is_err());
}

#[test]
fn test_predicate_or_combination() {
    use predicates::prelude::*;
    let pred = MinGain::try_from(0.001)
        .unwrap()
        .or(MaxUpdates::from(5_usize));

    // First condition: gain <= threshold → should stop
    let params1 = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 0.0001,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 0,
    };
    assert!(pred.eval(&params1));

    // Second condition: update + 1 >= 5 → should stop
    let params2 = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 1.0,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 4,
    };
    assert!(pred.eval(&params2));

    // Neither condition: should continue
    let params3 = PredParams {
        num_nodes: 100,
        num_arcs: 500,
        gain: 1.0,
        avg_gain_impr: 0.0,
        modified: 0,
        update: 1,
    };
    assert!(!pred.eval(&params3));
}

// ==================== SCC Methods ====================

#[test]
fn test_sccs_par_sort_by_size() -> Result<()> {
    let mut sccs = Sccs::new(3, vec![0, 1, 1, 1, 0, 2].into_boxed_slice());
    let sizes = sccs.par_sort_by_size();
    // Should give the same result as sort_by_size
    assert_eq!(sizes, vec![3, 2, 1].into_boxed_slice());
    assert_eq!(sccs.components().to_owned(), vec![1, 0, 0, 0, 1, 2]);
    Ok(())
}

#[test]
fn test_sccs_num_components() {
    let sccs = Sccs::new(5, vec![0, 1, 2, 3, 4].into_boxed_slice());
    assert_eq!(sccs.num_components(), 5);
}

#[test]
fn test_sccs_single_component() {
    let sccs = Sccs::new(1, vec![0, 0, 0, 0].into_boxed_slice());
    assert_eq!(sccs.compute_sizes(), vec![4].into_boxed_slice());
}

#[test]
fn test_sccs_par_sort_single_component() {
    let mut sccs = Sccs::new(1, vec![0, 0, 0].into_boxed_slice());
    let sizes = sccs.par_sort_by_size();
    assert_eq!(sizes, vec![3].into_boxed_slice());
}

// ==================== LLP Utility Functions ====================

#[test]
fn test_invert_permutation() {
    let perm = [3, 1, 0, 2];
    let mut inv = vec![0; 4];
    llp::invert_permutation(&perm, &mut inv);
    // perm[0] = 3 → inv[3] = 0
    // perm[1] = 1 → inv[1] = 1
    // perm[2] = 0 → inv[0] = 2
    // perm[3] = 2 → inv[2] = 3
    assert_eq!(inv, vec![2, 1, 3, 0]);
}

#[test]
fn test_invert_permutation_identity() {
    let perm = [0, 1, 2, 3, 4];
    let mut inv = vec![0; 5];
    llp::invert_permutation(&perm, &mut inv);
    assert_eq!(inv, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_invert_permutation_reverse() {
    let perm = [4, 3, 2, 1, 0];
    let mut inv = vec![0; 5];
    llp::invert_permutation(&perm, &mut inv);
    assert_eq!(inv, vec![4, 3, 2, 1, 0]);
}

#[test]
fn test_labels_to_ranks() {
    // labels = [2, 0, 1] → sorted by label: node 1 (label 0), node 2 (label 1), node 0 (label 2)
    // perm = [1, 2, 0] → ranks = inv(perm) = [2, 0, 1]
    let labels = [2, 0, 1];
    let ranks = llp::labels_to_ranks(&labels);
    assert_eq!(ranks.as_ref(), &[2, 0, 1]);
}

#[test]
fn test_labels_to_ranks_identity() {
    let labels = [0, 1, 2, 3];
    let ranks = llp::labels_to_ranks(&labels);
    assert_eq!(ranks.as_ref(), &[0, 1, 2, 3]);
}

#[test]
fn test_labels_to_ranks_reverse() {
    let labels = [3, 2, 1, 0];
    let ranks = llp::labels_to_ranks(&labels);
    assert_eq!(ranks.as_ref(), &[3, 2, 1, 0]);
}

// ==================== ExactSumSweep Levels ====================

#[test]
fn test_ess_diameter_only() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::Diameter::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.diameter, 4);
    Ok(())
}

#[test]
fn test_ess_radius_only() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::Radius::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.radius, 3);
    Ok(())
}

#[test]
fn test_ess_all_forward() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::AllForward::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.diameter, 4);
    assert_eq!(result.radius, 3);
    assert_eq!(result.forward_eccentricities.as_ref(), &[3, 3, 3, 4, 0]);
    Ok(())
}

#[test]
fn test_ess_radius_diameter() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);

    let result = exact_sum_sweep::RadiusDiameter::run(&graph, &transpose, None, no_logging![]);

    assert_eq!(result.diameter, 4);
    assert_eq!(result.radius, 3);
    Ok(())
}

// ==================== ExactSumSweep Symmetric Variants ====================

#[test]
fn test_ess_all_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
    assert!(result.eccentricities.len() == 5);
}

#[test]
fn test_ess_all_forward_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::AllForward::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_diameter_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::Diameter::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
}

#[test]
fn test_ess_radius_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::Radius::run_symm(&graph, no_logging![]);

    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_radius_diameter_symm() {
    let graph = VecGraph::from_arcs([
        (0, 1),
        (1, 0),
        (1, 2),
        (2, 1),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 3),
    ]);

    let result = exact_sum_sweep::RadiusDiameter::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 1);
    assert_eq!(result.radius, 1);
}

#[test]
fn test_ess_symm_path() {
    // A path graph: 0 - 1 - 2 - 3
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 2)]);

    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);

    assert_eq!(result.diameter, 3);
    assert_eq!(result.radius, 2);
}

// ==================== Acyclicity Edge Cases ====================

#[test]
fn test_acyclic_single_node() {
    let graph = VecGraph::from_arcs([] as [(usize, usize); 0]);
    assert!(is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_acyclic_dag() {
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3), (3, 4)]);
    assert!(is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_not_acyclic_self_loop() {
    let graph = VecGraph::from_arcs([(0, 0)]);
    assert!(!is_acyclic(&graph, no_logging![]));
}

#[test]
fn test_not_acyclic_mutual() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0)]);
    assert!(!is_acyclic(&graph, no_logging![]));
}

// ==================== Top Sort Edge Cases ====================

#[test]
fn test_top_sort_single_node() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let ts = top_sort(g, no_logging![]);
    assert_eq!(ts.as_ref(), &[0]);
}

#[test]
fn test_top_sort_no_edges() {
    let mut g = VecGraph::new();
    for i in 0..5 {
        g.add_node(i);
    }
    let ts = top_sort(g, no_logging![]);
    assert_eq!(ts.len(), 5);
    // All nodes should be present
    let mut sorted = ts.to_vec();
    sorted.sort();
    assert_eq!(sorted, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_top_sort_diamond() {
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let ts = top_sort(graph, no_logging![]);
    // 0 must come before 1 and 2, 1 and 2 must come before 3
    let pos: std::collections::HashMap<usize, usize> =
        ts.iter().enumerate().map(|(i, &n)| (n, i)).collect();
    assert!(pos[&0] < pos[&1]);
    assert!(pos[&0] < pos[&2]);
    assert!(pos[&1] < pos[&3]);
    assert!(pos[&2] < pos[&3]);
}

// ==================== SCC Algorithms - Additional Tests ====================

#[test]
fn test_tarjan_empty_graph() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let sccs = sccs::tarjan(g, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_tarjan_self_loop() {
    let graph = VecGraph::from_arcs([(0, 0)]);
    let sccs = sccs::tarjan(graph, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_tarjan_two_cycles() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]);
    let sccs = sccs::tarjan(graph, no_logging![]);
    assert_eq!(sccs.num_components(), 2);
}

#[test]
fn test_tarjan_chain() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let sccs = sccs::tarjan(graph, no_logging![]);
    assert_eq!(sccs.num_components(), 4);
}

#[test]
fn test_kosaraju_chain() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let transpose =
        VecGraph::from_lender(transform::transpose(&graph, MemoryUsage::BatchSize(10000))?.iter());
    let sccs = sccs::kosaraju(&graph, &transpose, no_logging![]);
    assert_eq!(sccs.num_components(), 4);
    Ok(())
}

#[test]
fn test_symm_seq_disconnected() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]);
    let sccs = sccs::symm_seq(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 2);
}

#[test]
fn test_symm_par_disconnected() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]);
    let sccs = sccs::symm_par(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 2);
}

#[test]
fn test_symm_seq_single_node() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let sccs = sccs::symm_seq(g, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_symm_par_single_node() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let sccs = sccs::symm_par(g, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_symm_seq_triangle() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (0, 2), (2, 0)]);
    let sccs = sccs::symm_seq(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_symm_par_triangle() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (0, 2), (2, 0)]);
    let sccs = sccs::symm_par(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

// ==================== ExactSumSweep Edge Cases ====================

#[test]
fn test_ess_diameter_cycle() {
    // Simple 4-cycle
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3)]);
    let result = exact_sum_sweep::Diameter::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.diameter, 3);
}

#[test]
fn test_ess_all_forward_cycle() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3)]);
    let result = exact_sum_sweep::AllForward::run(&graph, &transpose, None, no_logging![]);
    assert_eq!(result.diameter, 3);
    assert_eq!(result.radius, 3);
    // All nodes in a cycle have equal eccentricity
    for &ecc in result.forward_eccentricities.iter() {
        assert_eq!(ecc, 3);
    }
}

#[test]
fn test_ess_all_star_graph() {
    // Star graph: 0 → 1, 0 → 2, 0 → 3 (all symmetric)
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (0, 2), (2, 0), (0, 3), (3, 0)]);

    let result = exact_sum_sweep::All::run_symm(&graph, no_logging![]);
    assert_eq!(result.diameter, 2);
    assert_eq!(result.radius, 1);
    assert_eq!(result.radial_vertex, 0);
}

// ==================== Sccs epserde roundtrip ====================

#[test]
fn test_sccs_compute_sizes_empty() {
    let sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
    let sizes = sccs.compute_sizes();
    assert_eq!(sizes, vec![1, 1, 1].into_boxed_slice());
}

#[test]
fn test_sccs_sort_by_size_all_equal() {
    let mut sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
    let sizes = sccs.sort_by_size();
    // All components have size 1, so sorting is stable
    assert_eq!(sizes, vec![1, 1, 1].into_boxed_slice());
}

#[test]
fn test_sccs_par_sort_by_size_all_equal() {
    let mut sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
    let sizes = sccs.par_sort_by_size();
    assert_eq!(sizes, vec![1, 1, 1].into_boxed_slice());
}

// ==================== LLP Full Algorithm ====================

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

    let work_dir = webgraph::utils::temp_dir(std::env::temp_dir())?;
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
        &work_dir,
    )?;

    assert_eq!(labels.len(), num_nodes);
    for &label in labels.iter() {
        assert!(label < num_nodes, "Label {label} >= num_nodes {num_nodes}");
    }
    std::fs::remove_dir_all(&work_dir).ok();
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

    let work_dir = webgraph::utils::temp_dir(std::env::temp_dir())?;

    llp::layered_label_propagation_labels_only(
        &graph,
        &deg_cumul,
        vec![0.0],
        None,
        Granularity::Nodes(100),
        123,
        MaxUpdates::from(1_usize),
        &work_dir,
    )?;

    let labels = llp::combine_labels(&work_dir)?;
    assert_eq!(labels.len(), num_nodes);
    std::fs::remove_dir_all(&work_dir).ok();
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

    let work_dir = webgraph::utils::temp_dir(std::env::temp_dir())?;
    let gammas = vec![0.0, 0.5, 1.0, 2.0];

    let labels = llp::layered_label_propagation(
        &graph,
        &deg_cumul,
        gammas,
        Some(100),
        Granularity::Nodes(100),
        7,
        MaxUpdates::from(2_usize),
        &work_dir,
    )?;

    assert_eq!(labels.len(), 5);
    std::fs::remove_dir_all(&work_dir).ok();
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

    let work_dir = webgraph::utils::temp_dir(std::env::temp_dir())?;

    let predicate = MinGain::try_from(0.001)?.or(MaxUpdates::from(3_usize));

    let labels = llp::layered_label_propagation(
        &graph,
        &deg_cumul,
        vec![0.0],
        Some(10),
        Granularity::Arcs(100),
        0,
        predicate,
        &work_dir,
    )?;

    assert_eq!(labels.len(), 4);
    std::fs::remove_dir_all(&work_dir).ok();
    Ok(())
}
