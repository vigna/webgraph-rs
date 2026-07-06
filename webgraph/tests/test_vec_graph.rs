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

#[test]
fn test_add_exact_lender_partial() -> anyhow::Result<()> {
    use lender::Lender;
    // The lender yields only node 0, whose successor 5 must be added as a node
    let b = VecGraph::from_arcs([(0, 5)]);
    let mut c = VecGraph::new();
    c.add_exact_lender(b.iter().take(1));
    graph::eq(&b, &c)?;

    let b = LabeledVecGraph::<usize>::from_arcs([((0, 5), 1)]);
    let mut c = LabeledVecGraph::<usize>::new();
    c.add_exact_lender(b.iter().take(1));
    graph::eq_labeled(&b, &c)?;
    Ok(())
}

#[test]
fn test_add_arcs_dedups() {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    use webgraph::traits::{RandomAccessGraph, RandomAccessLabeling};
    // Regression: duplicate arcs in the bulk methods panicked with a
    // misleading "successor is not increasing" message, while BTreeGraph
    // dedups them.
    let g = VecGraph::from_arcs([(0, 1), (0, 1), (1, 0)]);
    assert_eq!(g.num_arcs(), 2);
    assert_eq!(g.successors(0).into_iter().collect::<Vec<_>>(), vec![1]);

    // The label of the last occurrence in the input wins, as in
    // LabeledBTreeGraph, even when the duplicates are not adjacent.
    let arcs = [((0, 1), 10u32), ((0, 2), 99), ((0, 1), 20)];
    let lg = LabeledVecGraph::<u32>::from_arcs(arcs);
    assert_eq!(lg.num_arcs(), 2);
    assert_eq!(
        RandomAccessLabeling::labels(&lg, 0).collect::<Vec<_>>(),
        vec![(1, 20), (2, 99)]
    );
    let bg = LabeledBTreeGraph::<u32>::from_arcs(arcs);
    assert_eq!(
        RandomAccessLabeling::labels(&bg, 0).collect::<Vec<_>>(),
        vec![(1, 20), (2, 99)]
    );
}
