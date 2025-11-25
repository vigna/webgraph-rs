/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_progress_logger::no_logging;
use no_break::NoBreak;
use webgraph::prelude::VecGraph;
use webgraph::visits::{Sequential, depth_first};
use webgraph_algo::prelude::{is_acyclic, top_sort};

#[test]
fn test_top_sort() {
    assert_eq!(
        vec![0, 1, 2].into_boxed_slice(),
        top_sort(VecGraph::from_arcs([(1, 2), (0, 1)]), no_logging![])
    );

    assert_eq!(
        vec![0, 1, 2].into_boxed_slice(),
        top_sort(VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]), no_logging![])
    );

    assert_eq!(
        vec![0, 2, 1, 3].into_boxed_slice(),
        top_sort(
            VecGraph::from_arcs([(0, 1), (0, 2), (2, 3), (1, 3)]),
            no_logging![]
        )
    );
}

#[test]
fn test_acyclicity() {
    let graph = VecGraph::from_arcs([(1, 2), (0, 1)]);

    assert!(is_acyclic(&graph, no_logging![]));

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);

    assert!(!is_acyclic(&graph, no_logging![]));

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (2, 3), (1, 3)]);

    assert!(is_acyclic(&graph, no_logging![]));
}

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
