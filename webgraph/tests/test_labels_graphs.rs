/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::graphs::vec_graph::LabeledVecGraph;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;

#[test]
fn test_eq() -> anyhow::Result<()> {
    let arcs = [(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g0 = VecGraph::from_arcs(arcs.iter().copied());
    let mut g1 = g0.clone();
    labels::eq_sorted(&g0, &g1)?;
    graph::eq(&g0, &g1)?;
    g1.add_arc(0, 3);
    assert!(labels::eq_sorted(&g0, &g1).is_err());
    assert!(graph::eq(&g0, &g1).is_err());

    let arcs = [
        ((0, 1), 0),
        ((0, 2), 1),
        ((1, 2), 2),
        ((1, 3), 3),
        ((2, 4), 4),
        ((3, 4), 5),
    ];
    let g0 = LabeledVecGraph::<usize>::from_arcs(arcs.iter().copied());
    let mut g1 = g0.clone();
    labels::eq_sorted(&g0, &g1)?;
    graph::eq_labeled(&g0, &g1)?;
    g1.add_arc(0, 3, 6);
    assert!(labels::eq_sorted(&g0, &g1).is_err());
    assert!(graph::eq_labeled(&g0, &g1).is_err());
    Ok(())
}

#[test]
fn test_graph_eq_error() -> anyhow::Result<()> {
    // Test eq function with different successors
    let arcs1 = [(0, 0), (0, 2), (1, 2)];
    let arcs2 = [(0, 0), (0, 1), (1, 2)]; // Different successor for node 0
    let g1 = VecGraph::from_arcs(arcs1.iter().copied());
    let mut g2 = VecGraph::from_arcs(arcs2.iter().copied());

    let result = graph::eq(&g1, &g2);
    if let Err(EqError::Successors { node, index, .. }) = result {
        assert_eq!(node, 0);
        assert_eq!(index, 1);
    } else {
        panic!("Expected Successors error, got: {:?}", result);
    }

    g2.add_node(3);
    let result = graph::eq(&g1, &g2);
    if let Err(EqError::NumNodes {
        first: 3,
        second: 4,
    }) = result
    {
    } else {
        panic!("Expected NumNodes error, got: {:?}", result);
    }

    // Test eq_labeled function with different labels
    let labeled_arcs1 = [((0, 1), "a"), ((0, 2), "b"), ((1, 2), "c")];
    let labeled_arcs2 = [((0, 1), "a"), ((0, 2), "x"), ((1, 2), "c")]; // Different label for arc (0,2)
    let lg1 = LabeledVecGraph::from_arcs(labeled_arcs1.iter().copied());
    let lg2 = LabeledVecGraph::from_arcs(labeled_arcs2.iter().copied());

    let result = graph::eq_labeled(&lg1, &lg2);
    assert!(result.is_err());

    if let Err(EqError::Successors { node, index, .. }) = result {
        assert_eq!(node, 0);
        assert_eq!(index, 1);
    } else {
        panic!("Expected Successors error, got: {:?}", result);
    }

    // Test successful equality
    graph::eq(&g1, &g1)?;

    graph::eq_labeled(&lg1, &lg1)?;
    Ok(())
}
