/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::graphs::vec_graph::LabeledVecGraph;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;

#[test]
fn test_eq() {
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g0 = VecGraph::from_arcs(arcs.iter().copied());
    let mut g1 = g0.clone();
    assert!(labels::eq_sorted(&g0, &g1));
    assert!(graph::eq(&g0, &g1));
    g1.add_arc(0, 3);
    assert!(!labels::eq_sorted(&g0, &g1));
    assert!(!graph::eq(&g0, &g1));

    let arcs = vec![
        (0, 1, 0),
        (0, 2, 1),
        (1, 2, 2),
        (1, 3, 3),
        (2, 4, 4),
        (3, 4, 5),
    ];
    let g0 = LabeledVecGraph::<usize>::from_arcs(arcs.iter().copied());
    let mut g1 = g0.clone();
    assert!(labels::eq_sorted(&g0, &g1));
    assert!(graph::eq_labeled(&g0, &g1));
    g1.add_arc(0, 3, 6);
    assert!(!labels::eq_sorted(&g0, &g1));
    assert!(!graph::eq_labeled(&g0, &g1));
}
