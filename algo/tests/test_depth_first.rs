/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Fontana Tommaso
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use no_break::NoBreak;
use webgraph::{prelude::VecGraph, traits::SequentialLabeling};
use webgraph_algo::{prelude::depth_first, visits::Sequential};

#[test]
fn test_depth() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
    depth_first::SeqNoPred::new(&graph)
        .visit([0], |event| {
            if let depth_first::EventNoPred::Previsit { node, depth, .. } = event {
                assert_eq!(node, depth);
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
}

#[test]
fn test_dfs_order() {
    //     0         7
    //    / \        |
    //   1   2       6
    //  / \   \      |
    // 3   4   5     8
    //
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (6, 8), (7, 6)]);

    let mut order = vec![0; graph.num_nodes()];
    for (i, node) in depth_first::SeqNoPred::new(&graph).into_iter().enumerate() {
        order[i] = node;
    }

    assert_eq!(order, vec![0, 1, 3, 4, 2, 5, 6, 8, 7]);
}
