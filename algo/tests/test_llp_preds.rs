/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use predicates::Predicate;
use webgraph_algo::llp;
use webgraph_algo::llp::preds::*;

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
