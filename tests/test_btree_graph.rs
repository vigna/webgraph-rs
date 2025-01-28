/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::graphs::btree_graph::LabeledBTreeGraph;

#[test]
fn test_remove() {
    let mut g = LabeledBTreeGraph::<_>::from_arcs([(0, 1, 1), (0, 2, 2), (1, 2, 3)]);
    assert!(g.remove_arc(0, 2));
    assert!(!g.remove_arc(0, 2));
}

#[cfg(feature = "serde")]
#[test]
fn test_serde() {
    let arcs = [(0, 1, 1), (0, 2, 2), (1, 2, 3)];

    let g = LabeledBTreeGraph::<usize>::from_arcs(arcs);
    let res = serde_json::to_string(&g).unwrap();
    let p: LabeledBTreeGraph<usize> = serde_json::from_str(&res).unwrap();
    assert_eq!(g, p);
}
