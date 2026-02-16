/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::graphs::btree_graph::LabeledBTreeGraph;

#[cfg(feature = "serde")]
#[test]
fn test_serde() -> anyhow::Result<()> {
    use webgraph::traits::graph;

    let arcs = [((0, 1), 1), ((0, 2), 2), ((1, 2), 3)];

    let g = LabeledBTreeGraph::<usize>::from_arcs(arcs);
    let res = serde_json::to_string(&g).unwrap();
    let p: LabeledBTreeGraph<usize> = serde_json::from_str(&res).unwrap();
    graph::eq_labeled(&g, &p)?;
    Ok(())
}
