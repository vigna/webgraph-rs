/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::prelude::PermutedGraph;
use webgraph::traits::SequentialGraph;

#[test]
fn test_permuted() {
    // 4 -> 0 -> 2
    //       `-> 3
    // 1 -> 5
    let mut graph = webgraph::graph::vec_graph::VecGraph::new();

    for i in 0..=5 {
        graph.add_node(i);
    }
    graph.add_arc(4, 0);
    graph.add_arc(0, 2);
    graph.add_arc(0, 3);
    graph.add_arc(1, 5);

    let perm = [1, 2, 3, 4, 5, 0]; // Shift every node by +1

    // 5 -> 1 -> 3
    //       `-> 4
    // 2 -> 0
    let permuted_graph = PermutedGraph {
        graph: &graph,
        perm: &perm,
    };

    assert_eq!(
        permuted_graph
            .iter_nodes()
            .map(|(node, successors)| (node, successors.collect()))
            .collect::<Vec<_>>(),
        vec![
            (1, vec![3, 4]),
            (2, vec![0]),
            (3, vec![]),
            (4, vec![]),
            (5, vec![1]),
            (0, vec![]),
        ]
    );
}
