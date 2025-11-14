/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::{
    graphs::{random::ErdosRenyi, vec_graph::LabeledVecGraph},
    labels::Zip,
    prelude::VecGraph,
    traits::{SequentialLabeling, graph, labels},
};

#[test]
fn test_random() {
    let graph = ErdosRenyi::new(10, 0.1, 0);
    let a = VecGraph::from_lender(graph.iter());
    let b = VecGraph::from_sorted_lender(graph.iter());
    let c = VecGraph::from_exact_lender(b.iter());
    assert!(graph::eq(&graph, &a).is_ok());
    assert_eq!(a, b);
    assert_eq!(b, c);

    let graph = Zip(a, b);
    let a = LabeledVecGraph::from_lender(graph.iter());
    let b = LabeledVecGraph::from_sorted_lender(graph.iter());
    let c = LabeledVecGraph::from_exact_lender(b.iter());
    assert!(graph::eq_labeled(&graph, &a).is_ok());
    assert_eq!(a, b);
    assert_eq!(b, c);
}

#[cfg(feature = "serde")]
#[test]
fn test_serde() -> anyhow::Result<()> {
    use webgraph::graphs::vec_graph::LabeledVecGraph;
    let arcs = [((0, 1), 1), ((0, 2), 2), ((1, 2), 3)];

    let g = LabeledVecGraph::<usize>::from_arcs(arcs);
    let res = serde_json::to_string(&g)?;
    let p: LabeledVecGraph<usize> = serde_json::from_str(&res)?;
    graph::eq_labeled(&g, &p)?;
    Ok(())
}

#[test]
fn test_epserde() -> anyhow::Result<()> {
    use epserde::prelude::*;
    use webgraph::graphs::vec_graph::LabeledVecGraph;
    let arcs = [((0, 1), 1), ((0, 2), 2), ((1, 2), 3)];

    let g = LabeledVecGraph::<usize>::from_arcs(arcs);

    let mut file = std::io::Cursor::new(vec![]);
    unsafe { g.serialize(&mut file) }?;
    let data = file.into_inner();
    let g2 = unsafe { <LabeledVecGraph<usize>>::deserialize_eps(&data) }?;
    graph::eq_labeled(&g, &g2)?;
    Ok(())
}

#[test]
fn test_sorted() -> anyhow::Result<()> {
    // This is just to test that we implemented correctly
    // the SortedIterator and SortedLender traits.
    let er = VecGraph::new();
    labels::eq_sorted(&er, &er)?;
    // This is just to test that we implemented correctly
    // the SortedIterator and SortedLender traits.
    let er = LabeledVecGraph::<usize>::new();
    labels::eq_sorted(&er, &er)?;
    Ok(())
}
