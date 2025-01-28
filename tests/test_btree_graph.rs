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
    let mut g = LabeledBTreeGraph::<_>::from_arcs([(0, 1, 1), (0, 2, 2), (1, 2, 3)]);
    // TODO
}
