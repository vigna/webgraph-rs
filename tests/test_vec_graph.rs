/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use epserde::prelude::*;
use webgraph::graphs::vec_graph::LabeledVecGraph;

#[cfg(feature = "serde")]
#[test]
fn test_serde() {
    let arcs = [(0, 1, 1), (0, 2, 2), (1, 2, 3)];

    let g = LabeledVecGraph::<usize>::from_arcs(arcs);
    let res = serde_json::to_string(&g).unwrap();
    let p: LabeledVecGraph<usize> = serde_json::from_str(&res).unwrap();
    assert_eq!(g, p);
}

#[test]
fn test_epserde() {
    let arcs = [(0, 1, 1), (0, 2, 2), (1, 2, 3)];

    let g = LabeledVecGraph::<usize>::from_arcs(arcs);

    let mut file = std::io::Cursor::new(vec![]);
    g.serialize(&mut file).unwrap();
    let data = file.into_inner();
    let g2 = <LabeledVecGraph<usize>>::deserialize_eps(&data).unwrap();
    assert_eq!(g, g2);
}
