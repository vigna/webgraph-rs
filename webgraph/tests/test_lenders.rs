/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::{
    graphs::vec_graph::LabeledVecGraph,
    prelude::VecGraph,
    traits::{NodeLabelsLender, SequentialLabeling},
};

#[test]
fn test() -> anyhow::Result<()> {
    let arcs = [(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = VecGraph::from_arcs(arcs.iter().copied());

    g.iter()
        .into_pairs()
        .enumerate()
        .for_each(|(i, (src, succ))| {
            assert_eq!(arcs[i], (src, succ));
        });
    Ok(())
}

#[test]
fn test_labeled() -> anyhow::Result<()> {
    let arcs = [
        ((0, 1), 0),
        ((0, 2), 1),
        ((1, 2), 2),
        ((1, 3), 3),
        ((2, 4), 4),
        ((3, 4), 5),
    ];
    let g = LabeledVecGraph::<usize>::from_arcs(arcs.iter().copied());

    g.iter()
        .into_labeled_pairs()
        .enumerate()
        .for_each(|(i, (pair, label))| {
            assert_eq!(arcs[i], (pair, label));
        });
    Ok(())
}
