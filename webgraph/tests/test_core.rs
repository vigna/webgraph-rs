/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for core graph types, traits, and transforms.

use anyhow::Result;
use lender::*;
use webgraph::{
    graphs::{
        no_selfloops_graph::NoSelfLoopsGraph,
        random::ErdosRenyi,
        union_graph::UnionGraph,
        vec_graph::{LabeledVecGraph, VecGraph},
    },
    labels::{Left, Right, Zip},
    prelude::*,
    traits::{
        RandomAccessLabeling, SequentialLabeling,
        graph::{self, UnitLabelGraph},
        labels::{self, EqError},
    },
    transform,
    utils::Granularity,
    visits::Sequential,
};

// ── VecGraph: construction, add_node, add_arc, iter, iter_from ──

#[test]
fn test_vec_graph_empty() -> Result<()> {
    let g = VecGraph::empty(5);
    assert_eq!(g.num_nodes(), 5);
    assert_eq!(g.num_arcs(), 0);
    for node in 0..5 {
        assert_eq!(g.outdegree(node), 0);
        assert!(g.successors(node).next().is_none());
    }
    Ok(())
}

#[test]
fn test_vec_graph_add_node_arc() -> Result<()> {
    let mut g = VecGraph::new();
    g.add_node(3);
    assert_eq!(g.num_nodes(), 4); // nodes 0..=3
    g.add_arc(0, 1);
    g.add_arc(0, 2);
    g.add_arc(2, 3);
    assert_eq!(g.num_arcs(), 3);
    assert_eq!(g.outdegree(0), 2);
    assert_eq!(g.outdegree(1), 0);
    assert_eq!(g.outdegree(2), 1);
    assert_eq!(g.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(g.successors(2).collect::<Vec<_>>(), vec![3]);
    Ok(())
}

#[test]
fn test_vec_graph_from_arcs() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (3, 1)]);
    assert_eq!(g.num_nodes(), 4);
    assert_eq!(g.num_arcs(), 4);
    assert!(g.has_arc(0, 1));
    assert!(g.has_arc(2, 0));
    assert!(!g.has_arc(0, 3));
    Ok(())
}

#[test]
fn test_vec_graph_iter_from() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut iter = g.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![2]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.collect::<Vec<_>>(), vec![0]);
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn test_vec_graph_from_lender_variants() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 2), (0, 1), (1, 0), (2, 1)]);
    let a = VecGraph::from_lender(g.iter());
    let b = VecGraph::from_sorted_lender(g.iter());
    let c = VecGraph::from_exact_lender(g.iter());
    graph::eq(&g, &a)?;
    assert_eq!(a, b);
    assert_eq!(b, c);
    Ok(())
}

// ── LabeledVecGraph ──

#[test]
fn test_labeled_vec_graph() -> Result<()> {
    let g = LabeledVecGraph::<f64>::from_arcs([((0, 1), 1.0), ((0, 2), 2.5), ((1, 2), 3.0)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 3);
    let succs: Vec<_> = RandomAccessLabeling::labels(&g, 0).collect();
    assert_eq!(succs, vec![(1, 1.0), (2, 2.5)]);
    Ok(())
}

// ── Graph equality functions ──

#[test]
fn test_graph_eq_same() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    graph::eq(&g, &g)?;
    Ok(())
}

#[test]
fn test_graph_eq_different_nodes() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::NumNodes { .. }));
}

#[test]
fn test_graph_eq_different_arcs() {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 0), (2, 0)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

#[test]
fn test_labeled_graph_eq() -> Result<()> {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    graph::eq_labeled(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_eq_sorted_labels() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    labels::eq_sorted(&g0, &g1)?;
    Ok(())
}

// ── UnitLabelGraph ──

#[test]
fn test_unit_label_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let u = UnitLabelGraph(g);
    assert_eq!(u.num_nodes(), 3);
    assert_eq!(u.num_arcs(), 3);

    let succs: Vec<_> = RandomAccessLabeling::labels(&u, 0).collect();
    assert_eq!(succs, vec![(1, ()), (2, ())]);

    let mut iter = u.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(1, ()), (2, ())]);
    Ok(())
}

// ── UnionGraph ──

#[test]
fn test_union_graph() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g1 = VecGraph::from_arcs([(0, 2), (2, 0)]);
    let u = UnionGraph(g0, g1);
    assert_eq!(u.num_nodes(), 3);

    let mut iter = u.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    let s: Vec<_> = succ.collect();
    assert_eq!(s, vec![1, 2]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![2]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.collect::<Vec<_>>(), vec![0]);

    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn test_union_graph_different_sizes() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let u = UnionGraph(g0, g1);
    assert_eq!(u.num_nodes(), 4);

    let nodes: Vec<_> = {
        let mut iter = u.iter();
        let mut result = vec![];
        while let Some((node, succ)) = iter.next() {
            result.push((node, succ.collect::<Vec<_>>()));
        }
        result
    };
    assert_eq!(nodes[0], (0, vec![1]));
    assert_eq!(nodes[1], (1, vec![]));
    assert_eq!(nodes[2], (2, vec![3]));
    assert_eq!(nodes[3], (3, vec![]));
    Ok(())
}

// ── NoSelfLoopsGraph ──

#[test]
fn test_no_selfloops_complete() -> Result<()> {
    // A graph where every node has a self-loop
    let g = VecGraph::from_arcs([(0, 0), (0, 1), (1, 0), (1, 1), (2, 2)]);
    let nsl = NoSelfLoopsGraph(g);
    assert_eq!(nsl.num_nodes(), 3);

    let mut iter = nsl.iter();
    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 0);
    assert_eq!(s.collect::<Vec<_>>(), vec![1]);

    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 1);
    assert_eq!(s.collect::<Vec<_>>(), vec![0]);

    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 2);
    assert_eq!(s.collect::<Vec<_>>(), Vec::<usize>::new());

    assert!(iter.next().is_none());
    Ok(())
}

// ── Zip labeling ──

#[test]
fn test_zip_labeling() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let z = Zip(g0, g1);
    assert_eq!(z.num_nodes(), 3);

    let mut iter = z.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    let s: Vec<_> = succ.collect();
    assert_eq!(s, vec![(1, 1), (2, 2)]);
    Ok(())
}

// ── Transforms: transpose, permute, simplify ──

#[test]
fn test_transpose() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let t = transform::transpose(&g, MemoryUsage::default())?;
    let t = VecGraph::from_lender(&t);
    assert_eq!(t.num_nodes(), 3);

    let s0: Vec<_> = t.successors(0).collect();
    assert!(s0.is_empty());

    let s1: Vec<_> = t.successors(1).collect();
    assert_eq!(s1, vec![0]);

    let s2: Vec<_> = t.successors(2).collect();
    assert_eq!(s2, vec![0, 1]);
    Ok(())
}

#[test]
fn test_permute() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let perm = [2, 0, 1]; // 0->2, 1->0, 2->1
    let p = transform::permute(&g, &perm, MemoryUsage::default())?;
    let p = VecGraph::from_lender(&p);
    assert_eq!(p.num_nodes(), 3);

    // node 0 maps to 2, arcs (0,1) -> (2,0), (0,2) -> (2,1)
    assert_eq!(p.successors(2).collect::<Vec<_>>(), vec![0, 1]);
    // node 1 maps to 0, arc (1,2) -> (0,1)
    assert_eq!(p.successors(0).collect::<Vec<_>>(), vec![1]);
    // node 2 maps to 1, no outgoing arcs
    assert_eq!(
        p.successors(1).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    Ok(())
}

#[test]
fn test_simplify() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (0, 0)]); // includes self-loop
    let s = transform::simplify(&g, MemoryUsage::default())?;
    let s = VecGraph::from_lender(&s);
    assert_eq!(s.num_nodes(), 3);

    // Self-loop should be removed, all edges bidirectional
    let mut s0: Vec<_> = s.successors(0).collect();
    s0.sort();
    assert_eq!(s0, vec![1]);
    let mut s1: Vec<_> = s.successors(1).collect();
    s1.sort();
    assert_eq!(s1, vec![0, 2]);
    Ok(())
}

// ── Granularity ──

#[test]
fn test_granularity_default() {
    let g = Granularity::default();
    assert!(matches!(g, Granularity::Nodes(1000)));
}

#[test]
fn test_granularity_node_to_node() {
    let g = Granularity::Nodes(500);
    assert_eq!(g.node_granularity(1000, Some(5000)), 500);
}

#[test]
fn test_granularity_arc_to_node() {
    let g = Granularity::Arcs(1000);
    // 100 nodes, 500 arcs => avg degree 5, 1000/5 = 200 nodes
    assert_eq!(g.node_granularity(100, Some(500)), 200);
}

#[test]
fn test_granularity_node_to_arc() {
    let g = Granularity::Nodes(100);
    // 1000 nodes, 5000 arcs => avg degree 5, 100*5 = 500 arcs
    assert_eq!(g.arc_granularity(1000, Some(5000)), 500);
}

#[test]
fn test_granularity_arc_to_arc() {
    let g = Granularity::Arcs(1000);
    assert_eq!(g.arc_granularity(100, Some(500)), 1000);
}

// ── par_node_apply ──

#[test]
fn test_par_node_apply() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    use dsi_progress_logger::no_logging;
    let degree_sum = g.par_node_apply(
        |range| range.map(|node| g.outdegree(node) as u64).sum::<u64>(),
        |a, b| a + b,
        Granularity::Nodes(2),
        no_logging![],
    );
    assert_eq!(degree_sum, g.num_arcs());
    Ok(())
}

// ── ErdosRenyi random graph ──

#[test]
fn test_erdos_renyi() -> Result<()> {
    let g = ErdosRenyi::new(50, 0.0, 0);
    assert_eq!(g.num_nodes(), 50);
    // p=0 means no arcs
    let mut iter = g.iter();
    while let Some((_, succ)) = iter.next() {
        assert_eq!(succ.into_iter().count(), 0);
    }

    let g = ErdosRenyi::new(10, 1.0, 0);
    // p=1 means all possible arcs (no self-loops since ErdosRenyi excludes them)
    let mut total_arcs = 0;
    let mut iter = g.iter();
    while let Some((_, succ)) = iter.next() {
        total_arcs += succ.into_iter().count();
    }
    // With p=1.0, should have n*(n-1) = 90 arcs
    assert_eq!(total_arcs, 90);
    Ok(())
}

// ── PermutedGraph (via random access) ──

#[test]
fn test_permuted_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let perm = [1, 2, 0]; // 0->1, 1->2, 2->0
    let pg = PermutedGraph {
        graph: &g,
        perm: &perm,
    };
    // Node 0 (mapped to 1) should have successors of original node 0 mapped:
    // original (0,1) -> (1,2), original (1,2) -> (2,0), original (2,0) -> (0,1)
    // So PermutedGraph at node 1 should give successors [2] (original node 0's succs mapped)
    let mut iter = pg.iter();
    while let Some((node, succ)) = iter.next() {
        let s: Vec<_> = succ.collect();
        match node {
            0 => assert_eq!(s, vec![1]), // orig node 2: (2,0) -> (0,1)
            1 => assert_eq!(s, vec![2]), // orig node 0: (0,1) -> (1,2)
            2 => assert_eq!(s, vec![0]), // orig node 1: (1,2) -> (2,0)
            _ => panic!("unexpected node {}", node),
        }
    }
    Ok(())
}

// ── Left/Right projections ──

#[test]
fn test_left_projection() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    let left = Left(g);
    assert_eq!(left.num_nodes(), 3);

    let mut iter = left.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1, 2]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![2]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), Vec::<usize>::new());
    Ok(())
}

// ── DFS visits ──

#[test]
fn test_dfs_previsit() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::depth_first;
    // Simple chain: 0->1->2->3
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visited = vec![];
    depth_first::SeqNoPred::new(&g)
        .visit([0], |event| {
            if let depth_first::EventNoPred::Previsit { node, .. } = event {
                visited.push(node);
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    assert_eq!(visited, vec![0, 1, 2, 3]);
    Ok(())
}

#[test]
fn test_dfs_order_disconnected() -> Result<()> {
    use webgraph::visits::depth_first;
    // Two disconnected components: 0->1, 2->3
    let g = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let nodes: Vec<_> = depth_first::SeqPred::new(&g)
        .into_iter()
        .map(|e| e.node)
        .collect();
    // Should visit all 4 nodes
    assert_eq!(nodes.len(), 4);
    assert!(nodes.contains(&0));
    assert!(nodes.contains(&1));
    assert!(nodes.contains(&2));
    assert!(nodes.contains(&3));
    Ok(())
}

// ── BFS visits ──

#[test]
fn test_bfs_order_distances() -> Result<()> {
    use webgraph::visits::breadth_first;
    // Star graph: 0->1, 0->2, 0->3, 1->4, 2->4
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (0, 3), (1, 4), (2, 4)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&g)).into_iter().collect();

    // Root node should have distance 0
    assert_eq!(events[0].node, 0);
    assert_eq!(events[0].distance, 0);

    // Nodes 1,2,3 should have distance 1
    for e in &events[1..4] {
        assert_eq!(e.distance, 1);
    }
    Ok(())
}

// ── CSR Graph ──

#[test]
fn test_csr_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CsrGraph::from_lender(g.iter());
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(csr.num_arcs(), 4);
    assert_eq!(csr.outdegree(0), 2);
    assert_eq!(
        RandomAccessLabeling::labels(&csr, 0).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        RandomAccessLabeling::labels(&csr, 2).collect::<Vec<_>>(),
        vec![0]
    );
    Ok(())
}

// ── BTreeGraph ──

#[test]
fn test_btree_graph_add_arcs() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let mut g = BTreeGraph::new();
    g.add_arcs([(2, 0), (0, 2), (0, 1), (1, 2)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 4);
    // BTreeGraph keeps successors sorted
    assert_eq!(g.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    Ok(())
}

// ── Right projection ──

#[test]
fn test_right_projection() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    let right = Right(g);
    assert_eq!(right.num_nodes(), 3);

    let mut iter = right.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![10, 20]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![30]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), Vec::<u32>::new());
    Ok(())
}

// ── Left/Right random access ──

#[test]
fn test_left_random_access() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    let left = Left(g);
    assert_eq!(left.num_arcs(), 3);
    assert_eq!(left.outdegree(0), 2);
    assert_eq!(left.outdegree(1), 1);
    assert_eq!(
        RandomAccessLabeling::labels(&left, 0)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        RandomAccessLabeling::labels(&left, 1)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![0]
    );
    Ok(())
}

#[test]
fn test_right_random_access() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    let right = Right(g);
    assert_eq!(right.num_arcs(), 3);
    assert_eq!(right.outdegree(0), 2);
    assert_eq!(
        RandomAccessLabeling::labels(&right, 0)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![10, 20]
    );
    Ok(())
}

// ── BTreeGraph: more operations ──

#[test]
fn test_btree_graph_from_arcs() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 3);
    assert!(g.has_arc(0, 1));
    assert!(g.has_arc(1, 2));
    assert!(g.has_arc(2, 0));
    assert!(!g.has_arc(0, 2));
    Ok(())
}

#[test]
fn test_btree_graph_empty() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let g = BTreeGraph::empty(5);
    assert_eq!(g.num_nodes(), 5);
    assert_eq!(g.num_arcs(), 0);
    for node in 0..5 {
        assert_eq!(g.outdegree(node), 0);
    }
    Ok(())
}

#[test]
fn test_btree_graph_from_lender() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let v = VecGraph::from_arcs([(0, 2), (0, 1), (1, 0), (2, 1)]);
    let b = BTreeGraph::from_lender(v.iter());
    assert_eq!(b.num_nodes(), 3);
    assert_eq!(b.num_arcs(), 4);
    graph::eq(&v, &b)?;
    Ok(())
}

#[test]
fn test_btree_graph_add_node() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let mut g = BTreeGraph::new();
    assert!(g.add_node(0));
    assert!(!g.add_node(0));
    assert!(g.add_node(5));
    assert_eq!(g.num_nodes(), 6); // nodes 0..=5
    Ok(())
}

#[test]
fn test_btree_graph_duplicate_arc() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let mut g = BTreeGraph::new();
    g.add_node(1);
    assert!(g.add_arc(0, 1));
    assert!(!g.add_arc(0, 1)); // duplicate
    assert_eq!(g.num_arcs(), 1);
    Ok(())
}

#[test]
fn test_btree_graph_iter_from() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut iter = g.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![2]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![0]);
    assert!(iter.next().is_none());
    Ok(())
}

// ── Zip random access ──

#[test]
fn test_zip_random_access() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let z = Zip(g0, g1);
    assert_eq!(z.num_arcs(), 3);
    assert_eq!(z.outdegree(0), 2);
    let succs: Vec<_> = RandomAccessLabeling::labels(&z, 0).collect();
    assert_eq!(succs, vec![(1, 1), (2, 2)]);
    let succs: Vec<_> = RandomAccessLabeling::labels(&z, 1).collect();
    assert_eq!(succs, vec![(0, 0)]);
    Ok(())
}

// ── MemoryUsage ──

#[test]
fn test_memory_usage_batch_size() {
    let mu = MemoryUsage::BatchSize(1000);
    assert_eq!(mu.batch_size::<u64>(), 1000);
    assert_eq!(mu.batch_size::<u32>(), 1000);
}

#[test]
fn test_memory_usage_memory_size() {
    let mu = MemoryUsage::MemorySize(8000);
    assert_eq!(mu.batch_size::<u64>(), 1000); // 8000 / 8
    assert_eq!(mu.batch_size::<u32>(), 2000); // 8000 / 4
}

#[test]
fn test_memory_usage_mul_div() {
    let mu = MemoryUsage::MemorySize(1000);
    let scaled = mu * 3;
    assert_eq!(scaled.batch_size::<u8>(), 3000);

    let halved = mu / 2;
    assert_eq!(halved.batch_size::<u8>(), 500);

    let mu = MemoryUsage::BatchSize(1000);
    let scaled = mu * 2;
    assert_eq!(scaled.batch_size::<u64>(), 2000);

    let halved = mu / 4;
    assert_eq!(halved.batch_size::<u64>(), 250);
}

#[test]
fn test_memory_usage_display() {
    let mu = MemoryUsage::MemorySize(1024);
    assert_eq!(format!("{}", mu), "1024 bytes");

    let mu = MemoryUsage::BatchSize(500);
    assert_eq!(format!("{}", mu), "500 elements");
}

// ── humanize ──

#[test]
fn test_humanize() {
    use webgraph::utils::humanize;
    assert_eq!(humanize(0.0), "0");
    assert_eq!(humanize(999.0), "999");
    assert_eq!(humanize(1000.0), "1.000K");
    assert_eq!(humanize(1500.0), "1.500K");
    assert_eq!(humanize(1_000_000.0), "1.000M");
    assert_eq!(humanize(2_500_000_000.0), "2.500G");
}

// ── Graph equality error cases ──

#[test]
fn test_graph_eq_different_outdegree() {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Outdegree { .. }));
}

#[test]
fn test_eq_sorted_different_nodes() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::NumNodes { .. }));
}

#[test]
fn test_labeled_graph_eq_different() {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 99)]);
    let err = graph::eq_labeled(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

// ── CSR Graph: more operations ──

#[test]
fn test_csr_graph_default() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrGraph;
    let csr = CsrGraph::default();
    assert_eq!(csr.num_nodes(), 0);
    assert_eq!(csr.num_arcs(), 0);
    Ok(())
}

#[test]
fn test_csr_graph_from_seq_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let csr = CsrGraph::from_seq_graph(&g);
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(csr.num_arcs(), 3);
    graph::eq(&g, &csr)?;
    Ok(())
}

// ── DFS: SeqPred with cycle ──

#[test]
fn test_dfs_pred_cycle() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::depth_first;
    // Cycle: 0->1->2->0
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut previsited = vec![];
    let mut revisited = vec![];
    depth_first::SeqPred::new(&g)
        .visit(0..g.num_nodes(), |event| {
            match event {
                depth_first::EventPred::Previsit { node, .. } => previsited.push(node),
                depth_first::EventPred::Revisit { node, .. } => revisited.push(node),
                _ => {}
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    assert_eq!(previsited.len(), 3);
    // Node 0 gets revisited when we visit the back edge 2->0
    assert!(revisited.contains(&0));
    Ok(())
}

// ── DFS: SeqPath with on_stack detection ──

#[test]
fn test_dfs_path_on_stack() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::depth_first;
    // Cycle: 0->1->2->0, extra edge: 0->2
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let mut found_on_stack = false;
    depth_first::SeqPath::new(&g)
        .visit(0..g.num_nodes(), |event| {
            if let depth_first::EventPred::Revisit { on_stack, .. } = event {
                if on_stack {
                    found_on_stack = true;
                }
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    // There's a cycle, so at least one revisit should find a node on stack
    assert!(found_on_stack);
    Ok(())
}

// ── DFS: early termination ──

#[test]
fn test_dfs_early_termination() -> Result<()> {
    use webgraph::visits::depth_first;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visited = vec![];
    let result = depth_first::SeqNoPred::new(&g).visit([0], |event| {
        if let depth_first::EventNoPred::Previsit { node, .. } = event {
            visited.push(node);
            if node == 2 {
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    });
    assert!(result.is_break());
    assert_eq!(visited, vec![0, 1, 2]);
    Ok(())
}

// ── BFS: disconnected components ──

#[test]
fn test_bfs_disconnected() -> Result<()> {
    use webgraph::visits::breadth_first;
    // Two components: {0,1} and {2,3}
    let g = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&g)).into_iter().collect();
    assert_eq!(events.len(), 4);

    // First two events should be root 0 and distance 0/1
    assert_eq!(events[0].root, 0);
    assert_eq!(events[0].distance, 0);
    assert_eq!(events[1].root, 0);
    assert_eq!(events[1].distance, 1);

    // Next two should be from root 2
    assert_eq!(events[2].root, 2);
    assert_eq!(events[2].distance, 0);
    assert_eq!(events[3].root, 2);
    assert_eq!(events[3].distance, 1);
    Ok(())
}

// ── JavaPermutation round-trip ──

#[test]
fn test_java_permutation_round_trip() -> Result<()> {
    use mmap_rs::MmapFlags;
    use std::io::Write;
    use value_traits::slices::SliceByValue;
    use webgraph::utils::JavaPermutation;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("perm.bin");
    let perm_data: Vec<usize> = vec![3, 1, 4, 0, 2];

    // Write as big-endian u64
    {
        let mut file = std::fs::File::create(&path)?;
        for &v in &perm_data {
            file.write_all(&(v as u64).to_be_bytes())?;
        }
    }

    // Read back via mmap
    let jp = JavaPermutation::mmap(&path, MmapFlags::empty())?;
    assert_eq!(jp.perm.as_ref().len(), perm_data.len());
    for (i, &expected) in perm_data.iter().enumerate() {
        assert_eq!(jp.index_value(i), expected);
    }
    Ok(())
}

// ── VecGraph: from_seq_graph equivalence ──

#[test]
fn test_vec_graph_from_seq_graph_eq() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0), (2, 3)]);
    let copy = VecGraph::from_sorted_lender(g.iter());
    graph::eq(&g, &copy)?;
    Ok(())
}

// ── NoSelfLoopsGraph: iter_from ──

#[test]
fn test_no_selfloops_iter_from() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 0), (0, 1), (1, 1), (1, 2), (2, 2)]);
    let nsl = NoSelfLoopsGraph(g);

    let mut iter = nsl.iter_from(1);
    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 1);
    assert_eq!(s.collect::<Vec<_>>(), vec![2]);

    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 2);
    assert_eq!(s.collect::<Vec<_>>(), Vec::<usize>::new());

    assert!(iter.next().is_none());
    Ok(())
}

// ── UnionGraph: iter_from ──

#[test]
fn test_union_graph_iter_from() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let g1 = VecGraph::from_arcs([(0, 2), (1, 3), (2, 0)]);
    let u = UnionGraph(g0, g1);

    let mut iter = u.iter_from(2);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    let s: Vec<_> = succ.collect();
    assert_eq!(s, vec![0, 3]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 3);
    assert_eq!(succ.collect::<Vec<_>>(), Vec::<usize>::new());

    assert!(iter.next().is_none());
    Ok(())
}

// ── Transpose round-trip ──

#[test]
fn test_transpose_round_trip() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 3), (3, 0)]);
    let t = transform::transpose(&g, MemoryUsage::BatchSize(2))?;
    let t = VecGraph::from_lender(&t);
    let tt = transform::transpose(&t, MemoryUsage::BatchSize(2))?;
    let tt = VecGraph::from_lender(&tt);
    graph::eq(&g, &tt)?;
    Ok(())
}

// ── Permute identity ──

#[test]
fn test_permute_identity() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let id = [0, 1, 2]; // identity permutation
    let p = transform::permute(&g, &id, MemoryUsage::BatchSize(10))?;
    let p = VecGraph::from_lender(&p);
    graph::eq(&g, &p)?;
    Ok(())
}

// ── Zip verify ──

#[test]
fn test_zip_verify_compatible() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let z = Zip(g0, g1);
    assert!(z.verify());
    Ok(())
}

#[test]
fn test_zip_verify_incompatible_succs() -> Result<()> {
    // Same structure but different number of successors for node 0
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2)]);
    let g1 = VecGraph::from_arcs([(0, 1)]);
    let z = Zip(g0, g1);
    assert!(!z.verify());
    Ok(())
}

#[test]
fn test_zip_verify_incompatible_nodes() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let z = Zip(g0, g1);
    assert!(!z.verify());
    Ok(())
}

// ── LabeledBTreeGraph ──

#[test]
fn test_labeled_btree_graph() -> Result<()> {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 3);
    let succs: Vec<_> = RandomAccessLabeling::labels(&g, 0).collect();
    assert_eq!(succs, vec![(1, 10), (2, 20)]);
    Ok(())
}

#[test]
fn test_labeled_btree_graph_from_lender() -> Result<()> {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20), ((2, 0), 30)]);
    let b = LabeledBTreeGraph::from_lender(g.iter());
    assert_eq!(b.num_nodes(), 3);
    assert_eq!(b.num_arcs(), 3);
    graph::eq_labeled(&g, &b)?;
    Ok(())
}

#[test]
fn test_labeled_btree_graph_remove_arc() -> Result<()> {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let mut g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20)]);
    assert_eq!(g.num_arcs(), 2);
    assert!(g.remove_arc(0, 1));
    assert_eq!(g.num_arcs(), 1);
    assert!(!g.remove_arc(0, 1)); // already removed
    assert_eq!(g.num_arcs(), 1);
    Ok(())
}

#[test]
fn test_btree_graph_shrink_to_fit() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let mut g = BTreeGraph::from_arcs([(0, 1), (1, 2)]);
    g.shrink_to_fit();
    // Just verify it doesn't crash and the graph is intact
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 2);
    Ok(())
}

// ── CsrSortedGraph ──

#[test]
fn test_csr_sorted_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrSortedGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CsrSortedGraph::from_seq_graph(&g);
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(csr.num_arcs(), 4);
    graph::eq(&g, &csr)?;
    Ok(())
}

// ── CompressedCsrGraph ──

#[test]
fn test_compressed_csr_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CompressedCsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let ccsr = CompressedCsrGraph::try_from_graph(&g)?;
    assert_eq!(ccsr.num_nodes(), 3);
    assert_eq!(ccsr.num_arcs(), 4);
    graph::eq(&g, &ccsr)?;
    Ok(())
}

#[test]
fn test_compressed_csr_sorted_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CompressedCsrSortedGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let ccsr = CompressedCsrSortedGraph::try_from_graph(&g)?;
    assert_eq!(ccsr.num_nodes(), 3);
    assert_eq!(ccsr.num_arcs(), 4);
    graph::eq(&g, &ccsr)?;
    Ok(())
}

// ── Simplify with batch-size ──

#[test]
fn test_simplify_with_batch_size() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let s = transform::simplify(&g, MemoryUsage::BatchSize(2))?;
    let s = VecGraph::from_lender(&s);
    assert_eq!(s.num_nodes(), 4);
    // All edges should be bidirectional
    for node in 0..4 {
        assert!(s.outdegree(node) >= 1);
    }
    Ok(())
}

// ── Permute with batch-size ──

#[test]
fn test_permute_reverse() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let perm = [3, 2, 1, 0]; // reverse
    let p = transform::permute(&g, &perm, MemoryUsage::BatchSize(2))?;
    let p = VecGraph::from_lender(&p);
    assert_eq!(p.num_nodes(), 4);
    // Arc (0,1) -> (3,2), (1,2) -> (2,1), (2,3) -> (1,0)
    assert_eq!(p.successors(3).collect::<Vec<_>>(), vec![2]);
    assert_eq!(p.successors(2).collect::<Vec<_>>(), vec![1]);
    assert_eq!(p.successors(1).collect::<Vec<_>>(), vec![0]);
    Ok(())
}

// ── Graph has_arc ──

#[test]
fn test_has_arc() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    assert!(g.has_arc(0, 1));
    assert!(g.has_arc(0, 2));
    assert!(g.has_arc(1, 2));
    assert!(g.has_arc(2, 0));
    assert!(!g.has_arc(1, 0));
    assert!(!g.has_arc(2, 1));
    Ok(())
}

// ── VecGraph: labeled from_sorted_lender and from_exact_lender ──

#[test]
fn test_labeled_vec_graph_from_lender_variants() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    let a = LabeledVecGraph::from_lender(g.iter());
    let b = LabeledVecGraph::from_sorted_lender(g.iter());
    let c = LabeledVecGraph::from_exact_lender(g.iter());
    graph::eq_labeled(&g, &a)?;
    graph::eq_labeled(&a, &b)?;
    graph::eq_labeled(&b, &c)?;
    Ok(())
}

// ── BFS visit from specific roots ──

#[test]
fn test_bfs_visit_callback() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::breadth_first;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut distances = vec![usize::MAX; g.num_nodes()];
    breadth_first::Seq::new(&g)
        .visit([0], |event| {
            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                distances[node] = distance;
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    assert_eq!(distances, vec![0, 1, 2, 3]);
    Ok(())
}

// ── DFS: postvisit events ──

#[test]
fn test_dfs_postvisit() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::depth_first;
    // Chain: 0->1->2
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut postvisited = vec![];
    depth_first::SeqPred::new(&g)
        .visit(0..g.num_nodes(), |event| {
            if let depth_first::EventPred::Postvisit { node, .. } = event {
                postvisited.push(node);
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    // Postvisit order should be reverse: 2, 1, 0
    assert_eq!(postvisited, vec![2, 1, 0]);
    Ok(())
}

// ── VecGraph: num_arcs_hint ──

#[test]
fn test_vec_graph_num_arcs_hint() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert_eq!(g.num_arcs_hint(), Some(3));
    Ok(())
}

// ── Granularity: no arcs info ──

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_no_arcs_info() {
    let g = Granularity::Arcs(100);
    // Panics because arcs-to-nodes conversion requires arc count
    let _ = g.node_granularity(50, None);
}

#[test]
#[should_panic(expected = "You need the number of arcs")]
fn test_granularity_nodes_no_arcs_info() {
    let g = Granularity::Nodes(100);
    // Panics because nodes-to-arcs conversion requires arc count
    let _ = g.arc_granularity(1000, None);
}

// ── SortPairs: external sort and merge ──

#[test]
fn test_sort_pairs_basic() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    sp.push(2, 3)?;
    sp.push(0, 1)?;
    sp.push(1, 2)?;
    sp.push(0, 0)?;
    let result: Vec<_> = sp.iter()?.collect();
    assert_eq!(
        result,
        vec![((0, 0), ()), ((0, 1), ()), ((1, 2), ()), ((2, 3), ())]
    );
    Ok(())
}

#[test]
fn test_sort_pairs_sort_method() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs = vec![(3, 0), (1, 2), (0, 1), (2, 3)];
    let result: Vec<_> = sp.sort(pairs)?.collect();
    assert_eq!(
        result,
        vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ()), ((3, 0), ())]
    );
    Ok(())
}

#[test]
fn test_sort_pairs_multiple_batches() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    // Tiny batch size to force multiple batches
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(2), dir.path())?;
    sp.push(5, 0)?;
    sp.push(3, 1)?;
    sp.push(1, 2)?;
    sp.push(0, 3)?;
    sp.push(4, 4)?;
    sp.push(2, 5)?;
    let result: Vec<((usize, usize), ())> = sp.iter()?.collect();
    // Should be sorted by (src, dst) lexicographic order
    assert_eq!(result[0].0, (0, 3));
    assert_eq!(result[1].0, (1, 2));
    assert_eq!(result[2].0, (2, 5));
    assert_eq!(result[3].0, (3, 1));
    assert_eq!(result[4].0, (4, 4));
    assert_eq!(result[5].0, (5, 0));
    Ok(())
}

// ── SortPairs: non-empty dir error ──

#[test]
fn test_sort_pairs_non_empty_dir() {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir().unwrap();
    // Create a file in the dir to make it non-empty
    std::fs::write(dir.path().join("dummy"), b"x").unwrap();
    let result = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path());
    assert!(result.is_err());
}

// ── KMergeIters: sum and collect ──

#[test]
fn test_kmerge_iters_sum() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let a = KMergeIters::new(vec![vec![((0, 1), ()), ((2, 3), ())].into_iter()]);
    let b = KMergeIters::new(vec![vec![((1, 2), ()), ((3, 4), ())].into_iter()]);
    let merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        vec![a, b].into_iter().sum::<KMergeIters<_>>();
    let result: Vec<_> = merged.collect();
    assert_eq!(
        result,
        vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ()), ((3, 4), ())]
    );
}

#[test]
fn test_kmerge_iters_collect() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let iters: Vec<Vec<((usize, usize), ())>> = vec![
        vec![((0, 0), ()), ((1, 1), ())],
        vec![((0, 1), ()), ((2, 0), ())],
    ];
    let merged: KMergeIters<_, ()> = iters.into_iter().collect();
    let result: Vec<_> = merged.collect();
    assert_eq!(
        result,
        vec![((0, 0), ()), ((0, 1), ()), ((1, 1), ()), ((2, 0), ())]
    );
}

// ── Matrix: basic operations ──

#[test]
fn test_matrix_basic() {
    use webgraph::utils::Matrix;
    let mut m = Matrix::<i32>::new(3, 4);
    assert_eq!(m[(0, 0)], 0);
    m[(1, 2)] = 42;
    assert_eq!(m[(1, 2)], 42);
    m[(2, 3)] = -7;
    assert_eq!(m[(2, 3)], -7);
    // Other cells unchanged
    assert_eq!(m[(0, 0)], 0);
    assert_eq!(m[(2, 0)], 0);
}

// ── ArcListGraph: construction and iteration ──

#[test]
fn test_arc_list_graph_unlabeled() -> Result<()> {
    use webgraph::graphs::arc_list_graph::ArcListGraph;
    let arcs = vec![(0, 1), (0, 2), (1, 0), (2, 1)];
    let g = ArcListGraph::new(3, arcs);
    assert_eq!(g.num_nodes(), 3);
    let mut iter = g.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1, 2]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![0]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1]);
    Ok(())
}

#[test]
fn test_arc_list_graph_labeled() -> Result<()> {
    use webgraph::graphs::arc_list_graph::ArcListGraph;
    let arcs = vec![((0, 1), 10u32), ((0, 2), 20), ((1, 0), 30)];
    let g = ArcListGraph::new_labeled(3, arcs.into_iter());
    assert_eq!(g.num_nodes(), 3);
    let mut iter = g.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(1, 10), (2, 20)]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(0, 30)]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.collect::<Vec<_>>(), Vec::<(usize, u32)>::new());
    Ok(())
}

// ── eq_sorted: sorted graph equality ──

#[test]
fn test_eq_sorted_identical() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    labels::eq_sorted(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_eq_sorted_different_num_nodes() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::NumNodes { .. }));
}

#[test]
fn test_eq_sorted_different_successors() {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 2), (1, 2), (2, 0)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

#[test]
fn test_eq_sorted_different_outdegree() {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 0), (2, 0)]);
    let err = labels::eq_sorted(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Outdegree { .. }));
}

// ── UnitLabelGraph: labeling wrapper ──

#[test]
fn test_unit_label_graph_iter_from() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let lg = UnitLabelGraph(&g);
    // iter_from(1) starts from node 1
    let mut iter = lg.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(2, ())]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(0, ())]);
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn test_unit_label_graph_num_arcs() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let lg = UnitLabelGraph(&g);
    assert_eq!(RandomAccessLabeling::num_arcs(&lg), 3);
    assert_eq!(RandomAccessLabeling::outdegree(&lg, 0), 1);
    Ok(())
}

// ── MemoryUsage: from_perc and batch_size ──

#[test]
fn test_memory_usage_from_perc() {
    let mu = webgraph::utils::MemoryUsage::from_perc(10.0);
    // Just verify it creates a MemorySize variant with a reasonable value
    let bs = mu.batch_size::<u64>();
    assert!(bs > 0);
}

#[test]
fn test_memory_usage_batch_size_memory() {
    let mu = webgraph::utils::MemoryUsage::MemorySize(1024);
    // size_of::<u64>() == 8, so 1024 / 8 = 128
    assert_eq!(mu.batch_size::<u64>(), 128);
    // size_of::<u32>() == 4, so 1024 / 4 = 256
    assert_eq!(mu.batch_size::<u32>(), 256);
}

#[test]
fn test_memory_usage_batch_size_batch() {
    let mu = webgraph::utils::MemoryUsage::BatchSize(42);
    // BatchSize just returns the value regardless of T
    assert_eq!(mu.batch_size::<u64>(), 42);
    assert_eq!(mu.batch_size::<u8>(), 42);
}

// ── humanize: edge cases ──

#[test]
fn test_humanize_large() {
    use webgraph::utils::humanize;
    assert_eq!(humanize(0.0), "0");
    assert_eq!(humanize(999.0), "999");
    assert_eq!(humanize(1000.0), "1.000K");
    assert_eq!(humanize(1_500_000.0), "1.500M");
    assert_eq!(humanize(1_000_000_000.0), "1.000G");
    assert_eq!(humanize(2_500_000_000_000.0), "2.500T");
}

// ── RaggedArray: additional methods ──

#[test]
fn test_ragged_array_operations() {
    use webgraph::utils::RaggedArray;
    let mut ra = RaggedArray::<i32>::new();
    assert!(ra.is_empty());
    assert_eq!(ra.num_values(), 0);
    ra.push(vec![1, 2, 3]);
    ra.push(vec![4]);
    assert!(!ra.is_empty());
    assert_eq!(ra.len(), 2);
    assert_eq!(ra.num_values(), 4);
    assert!(ra.values_capacity() >= 4);
    ra.shrink_to_fit();
    ra.shrink_values_to(4);
    assert_eq!(&ra[0], &[1, 2, 3]);
    assert_eq!(&ra[1], &[4]);
}

// ── SplitIters: construction ──

#[test]
fn test_split_iters() {
    use webgraph::utils::SplitIters;
    let boundaries = vec![0, 3, 5].into_boxed_slice();
    let iters = vec![vec![1, 2, 3], vec![4, 5]].into_boxed_slice();
    let si = SplitIters::new(boundaries, iters);
    assert_eq!(&*si.boundaries, &[0, 3, 5]);
    assert_eq!(&*si.iters, &[vec![1, 2, 3], vec![4, 5]]);

    // Test From conversion
    let si2: SplitIters<Vec<i32>> = (
        vec![0, 10].into_boxed_slice(),
        vec![vec![42]].into_boxed_slice(),
    )
        .into();
    assert_eq!(&*si2.boundaries, &[0, 10]);
}

// ── PermutedGraph: iter_from ──

#[test]
fn test_permuted_graph_iter_from() -> Result<()> {
    use webgraph::graphs::permuted_graph::PermutedGraph;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let perm = vec![2, 0, 1];
    let pg = PermutedGraph {
        graph: &g,
        perm: &perm,
    };
    // iter_from(1) should start from node 1 in the original graph
    let mut iter = pg.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    // node 1 permuted to 0, successor 2 permuted to 1
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![1]);
    let (node, succ) = iter.next().unwrap();
    // node 2 permuted to 1, successor 0 permuted to 2
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![2]);
    assert!(iter.next().is_none());
    Ok(())
}

// ── NoSelfLoopsGraph: num_arcs_hint, iter_from with self-loops ──

#[test]
fn test_no_selfloops_num_arcs_hint() {
    let g = VecGraph::from_arcs([(0, 0), (0, 1), (1, 1), (1, 2)]);
    let nsl = NoSelfLoopsGraph(g);
    // num_arcs_hint should return the same as the underlying graph
    assert_eq!(nsl.num_arcs_hint(), Some(4));
}

// ── EqError: Display formatting ──

#[test]
fn test_eq_error_display() {
    let err = EqError::NumNodes {
        first: 3,
        second: 5,
    };
    assert_eq!(format!("{err}"), "Different number of nodes: 3 != 5");

    let err = EqError::NumArcs {
        first: 10,
        second: 20,
    };
    assert_eq!(format!("{err}"), "Different number of arcs: 10 != 20");

    let err = EqError::Successors {
        node: 1,
        index: 0,
        first: "2".to_string(),
        second: "3".to_string(),
    };
    assert!(format!("{err}").contains("Different successors for node 1"));

    let err = EqError::Outdegree {
        node: 2,
        first: 3,
        second: 1,
    };
    assert!(format!("{err}").contains("Different outdegree for node 2"));
}

// ── graph::eq for unsorted graphs ──

#[test]
fn test_graph_eq_function() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    graph::eq(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_graph_eq_function_different() {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let g1 = VecGraph::from_arcs([(0, 2), (1, 2), (2, 0)]);
    let err = graph::eq(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

// ── graph::eq_labeled for labeled graphs ──

#[test]
fn test_graph_eq_labeled_same() -> Result<()> {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    graph::eq_labeled(&g0, &g1)?;
    Ok(())
}

#[test]
fn test_graph_eq_labeled_different_labels() {
    let g0 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let g1 = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 99)]);
    let err = graph::eq_labeled(&g0, &g1).unwrap_err();
    assert!(matches!(err, EqError::Successors { .. }));
}

// ── LabeledRandomAccessGraph: has_arc and successors ──

#[test]
fn test_labeled_graph_has_arc() -> Result<()> {
    use webgraph::traits::graph::LabeledRandomAccessGraph;
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    assert!(LabeledRandomAccessGraph::has_arc(&g, 0, 1));
    assert!(LabeledRandomAccessGraph::has_arc(&g, 0, 2));
    assert!(!LabeledRandomAccessGraph::has_arc(&g, 1, 0));
    assert!(LabeledRandomAccessGraph::has_arc(&g, 1, 2));
    assert!(!LabeledRandomAccessGraph::has_arc(&g, 2, 0));
    Ok(())
}

// ── temp_dir utility ──

#[test]
fn test_temp_dir() -> Result<()> {
    use webgraph::utils::temp_dir;
    let base = tempfile::tempdir()?;
    let dir = temp_dir(base.path())?;
    assert!(dir.exists());
    assert!(dir.is_dir());
    // Should be inside base
    assert!(dir.starts_with(base.path()));
    Ok(())
}

// ── ErdosRenyi: basic properties ──

#[test]
fn test_erdos_renyi_properties() -> Result<()> {
    let g = ErdosRenyi::new(100, 0.1, 42);
    assert_eq!(g.num_nodes(), 100);
    // Check that arcs exist by iterating
    let mut total_arcs = 0usize;
    let mut iter = g.iter();
    while let Some((_node, succ)) = iter.next() {
        total_arcs += succ.into_iter().count();
    }
    assert!(total_arcs > 0);
    Ok(())
}

// ── Left/Right projection: iter_from ──

#[test]
fn test_left_projection_iter_from() -> Result<()> {
    let g =
        LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30), ((2, 1), 40)]);
    let left = Left(g);
    // iter_from(1) should start at node 1
    let mut iter = left.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![0]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1]);
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn test_right_projection_iter_from() -> Result<()> {
    let g =
        LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30), ((2, 1), 40)]);
    let right = Right(g);
    let mut iter = right.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![30]);
    Ok(())
}

// ── Left/Right projection: num_arcs, outdegree ──

#[test]
fn test_left_projection_num_arcs_outdegree() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    let left = Left(g);
    assert_eq!(RandomAccessLabeling::num_arcs(&left), 3);
    assert_eq!(RandomAccessLabeling::outdegree(&left, 0), 2);
    assert_eq!(RandomAccessLabeling::outdegree(&left, 1), 1);
    assert_eq!(RandomAccessLabeling::outdegree(&left, 2), 0);
    Ok(())
}

// ── Zip: num_arcs_hint ──

#[test]
fn test_zip_num_arcs_hint() {
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g2 = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let z = Zip(&g1, &g2);
    // Zip does not override num_arcs_hint, so it returns None
    assert_eq!(z.num_arcs_hint(), None);
}

// ── SortPairs: sort_labeled and try_sort_labeled ──

#[test]
fn test_sort_pairs_sort_labeled() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    // Use SortPairs unlabeled (which uses DefaultBatchCodec internally)
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs = vec![((2, 3), ()), ((0, 1), ()), ((1, 2), ())];
    let result: Vec<_> = sp.sort_labeled(pairs)?.collect();
    assert_eq!(result, vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ())]);
    Ok(())
}

#[test]
fn test_sort_pairs_try_sort_labeled() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs: Vec<Result<_, std::convert::Infallible>> =
        vec![Ok(((2, 0), ())), Ok(((0, 1), ())), Ok(((1, 0), ()))];
    let result: Vec<_> = sp.try_sort_labeled(pairs)?.collect();
    assert_eq!(result, vec![((0, 1), ()), ((1, 0), ()), ((2, 0), ())]);
    Ok(())
}

#[test]
fn test_sort_pairs_try_sort() -> Result<()> {
    use webgraph::utils::SortPairs;
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new(webgraph::utils::MemoryUsage::BatchSize(100), dir.path())?;
    let pairs: Vec<Result<_, std::convert::Infallible>> = vec![Ok((3, 1)), Ok((1, 2)), Ok((0, 0))];
    let result: Vec<_> = sp.try_sort(pairs)?.collect();
    assert_eq!(result, vec![((0, 0), ()), ((1, 2), ()), ((3, 1), ())]);
    Ok(())
}

// ── KMergeIters: default, extend, add_assign ──

#[test]
fn test_kmerge_iters_default_and_extend() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut merged: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> = KMergeIters::default();
    // Extend with new iterators
    merged.extend(vec![
        vec![((0, 1), ()), ((2, 3), ())].into_iter(),
        vec![((1, 0), ())].into_iter(),
    ]);
    let result: Vec<_> = merged.collect();
    assert_eq!(result, vec![((0, 1), ()), ((1, 0), ()), ((2, 3), ())]);
}

#[test]
fn test_kmerge_iters_add_assign() {
    use webgraph::utils::sort_pairs::KMergeIters;
    let mut a: KMergeIters<std::vec::IntoIter<((usize, usize), ())>> =
        KMergeIters::new(vec![vec![((0, 0), ()), ((2, 2), ())].into_iter()]);
    let b = KMergeIters::new(vec![vec![((1, 1), ())].into_iter()]);
    a += b;
    let result: Vec<_> = a.collect();
    assert_eq!(result, vec![((0, 0), ()), ((1, 1), ()), ((2, 2), ())]);
}

// ── JavaPermutation: mutable operations ──

#[test]
fn test_java_permutation_mmap_mut() -> Result<()> {
    use mmap_rs::MmapFlags;
    use value_traits::slices::{SliceByValue, SliceByValueMut};
    use webgraph::utils::JavaPermutation;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("perm_mm.bin");

    // Write a file with big-endian u64 values
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for v in [0u64, 0, 0] {
            f.write_all(&v.to_be_bytes())?;
        }
    }

    // mmap_mut it and write values
    let mut perm = JavaPermutation::mmap_mut(&path, MmapFlags::empty())?;
    assert_eq!(perm.len(), 3);
    unsafe {
        perm.set_value_unchecked(0, 10);
        perm.set_value_unchecked(1, 20);
        perm.set_value_unchecked(2, 30);
    }
    assert_eq!(unsafe { perm.get_value_unchecked(0) }, 10);
    assert_eq!(unsafe { perm.get_value_unchecked(2) }, 30);
    assert_eq!(perm.as_ref().len(), 3);

    Ok(())
}

#[test]
fn test_java_permutation_bit_width() -> Result<()> {
    use mmap_rs::MmapFlags;
    use sux::traits::BitWidth;
    use webgraph::utils::JavaPermutation;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("perm_bw.bin");

    // Write a file first so we can mmap it
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for v in [0u64, 0] {
            f.write_all(&v.to_be_bytes())?;
        }
    }

    // Read-only variant
    let perm_ro = JavaPermutation::mmap(&path, MmapFlags::empty())?;
    assert_eq!(BitWidth::bit_width(&perm_ro), 64);
    assert_eq!(perm_ro.as_ref().len(), 2);

    // Read-write variant
    let perm_rw = JavaPermutation::mmap_mut(&path, MmapFlags::empty())?;
    assert_eq!(BitWidth::bit_width(&perm_rw), 64);
    assert_eq!(perm_rw.as_ref().len(), 2);

    Ok(())
}

// ── LabeledVecGraph: Default, shrink_to_fit, into_lender ──

#[test]
fn test_labeled_vec_graph_default() {
    let g = LabeledVecGraph::<u32>::default();
    assert_eq!(g.num_nodes(), 0);
}

#[test]
fn test_labeled_vec_graph_shrink_to_fit() {
    let mut g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    g.shrink_to_fit();
    // Still works after shrinking
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(RandomAccessLabeling::num_arcs(&g), 3);
}

#[test]
fn test_labeled_vec_graph_into_lender() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    // Use for_! macro which calls into_lender
    let mut count = 0;
    for_!((node, succ) in &g {
        let _ = node;
        let _ = succ.count();
        count += 1;
    });
    assert_eq!(count, 2);
    Ok(())
}

// ── VecGraph: into_lender ──

#[test]
fn test_vec_graph_into_lender() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut count = 0;
    for_!((node, succ) in &g {
        let _ = node;
        let _ = succ.count();
        count += 1;
    });
    assert_eq!(count, 3);
    Ok(())
}

// ── UnionGraph: num_arcs_hint, into_lender ──

#[test]
fn test_union_graph_num_arcs_hint() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(1, 0)]);
    let u = UnionGraph(g0, g1);
    assert_eq!(u.num_arcs_hint(), None);
}

#[test]
fn test_union_graph_into_lender() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g1 = VecGraph::from_arcs([(0, 2), (2, 0)]);
    let u = UnionGraph(g0, g1);
    // Use iter() directly instead of for_! since UnionGraph requires
    // SortedLender + Clone bounds on lenders for IntoLender
    let mut iter = u.iter();
    let mut count = 0;
    while let Some((_node, succ)) = iter.next() {
        let _ = succ.count();
        count += 1;
    }
    assert_eq!(count, 3);
    Ok(())
}

// ── Left/Right: num_arcs_hint ──

#[test]
fn test_left_num_arcs_hint() {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let left = Left(g);
    assert_eq!(left.num_arcs_hint(), Some(2));
}

#[test]
fn test_right_num_arcs_hint() {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let right = Right(g);
    assert_eq!(right.num_arcs_hint(), Some(2));
}

// ── AssumeSortedLender ──

#[test]
fn test_assume_sorted_lender() -> Result<()> {
    use webgraph::traits::labels::AssumeSortedLender;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let lender = g.iter();
    // SAFETY: VecGraph lenders are already sorted
    let mut sorted = unsafe { AssumeSortedLender::new(lender) };
    let (node, succ) = sorted.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![1]);
    let (node, _succ) = sorted.next().unwrap();
    assert_eq!(node, 1);
    // Check size_hint
    let (min, max) = sorted.size_hint();
    assert_eq!(min, 1);
    assert_eq!(max, Some(1));
    Ok(())
}

// ── eq_succs: outdegree mismatch paths ──

#[test]
fn test_eq_succs_first_shorter() {
    // First list shorter than second → Outdegree error
    let err = labels::eq_succs(0, vec![1], vec![1, 2]).unwrap_err();
    match err {
        EqError::Outdegree {
            node,
            first,
            second,
        } => {
            assert_eq!(node, 0);
            assert_eq!(first, 1);
            assert_eq!(second, 2);
        }
        _ => panic!("Expected Outdegree error"),
    }
}

#[test]
fn test_eq_succs_second_shorter() {
    // Second list shorter than first → Outdegree error
    let err = labels::eq_succs(0, vec![1, 2], vec![1]).unwrap_err();
    match err {
        EqError::Outdegree {
            node,
            first,
            second,
        } => {
            assert_eq!(node, 0);
            assert_eq!(first, 2);
            assert_eq!(second, 1);
        }
        _ => panic!("Expected Outdegree error"),
    }
}

// ── Left/Right: DoubleEndedIterator, ExactSizeIterator ──

#[test]
fn test_left_random_access_double_ended() -> Result<()> {
    use webgraph::labels::proj::LeftIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30)];
    let mut iter = LeftIntoIter(pairs.into_iter());
    assert_eq!(iter.next_back(), Some(3));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next_back(), Some(2));
    assert_eq!(iter.next(), None);
    Ok(())
}

#[test]
fn test_right_random_access_double_ended() -> Result<()> {
    use webgraph::labels::proj::RightIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30)];
    let mut iter = RightIntoIter(pairs.into_iter());
    assert_eq!(iter.next_back(), Some(30));
    assert_eq!(iter.next(), Some(10));
    // nth_back(0) = next_back()
    assert_eq!(iter.nth_back(0), Some(20));
    assert_eq!(iter.next(), None);
    Ok(())
}

#[test]
fn test_left_exact_size_iterator() {
    use webgraph::labels::proj::LeftIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30)];
    let iter = LeftIntoIter(pairs.into_iter());
    assert_eq!(iter.len(), 3);
}

#[test]
fn test_right_exact_size_iterator() {
    use webgraph::labels::proj::RightIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20)];
    let iter = RightIntoIter(pairs.into_iter());
    assert_eq!(iter.len(), 2);
}

#[test]
fn test_left_nth_back() -> Result<()> {
    use webgraph::labels::proj::LeftIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30), (4, 40)];
    let mut iter = LeftIntoIter(pairs.into_iter());
    // nth_back(1) skips 4, returns 3
    assert_eq!(iter.nth_back(1), Some(3));
    assert_eq!(iter.next_back(), Some(2));
    Ok(())
}

// ── permute: error on mismatched sizes ──

#[test]
fn test_permute_size_mismatch() {
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let perm = vec![0, 1]; // 2 elements but graph has 3 nodes
    let result = transform::permute(&g, &perm, webgraph::utils::MemoryUsage::BatchSize(10));
    assert!(result.is_err());
}

// ── Left/Right: size_hint ──

#[test]
fn test_left_size_hint() {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let left = Left(g);
    let lender = left.iter();
    let (min, max) = lender.size_hint();
    assert_eq!(min, 2);
    assert_eq!(max, Some(2));
}

// ── LeftIterator: is_empty and len ──

#[test]
fn test_left_iterator_is_empty() {
    use lender::ExactSizeLender;
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10)]);
    let left = Left(g);
    let mut lender = left.iter();
    assert!(!lender.is_empty());
    // LabeledVecGraph has 2 nodes (0 and 1), so the lender has 2 entries
    assert_eq!(lender.len(), 2);
    let _ = lender.next();
    assert_eq!(lender.len(), 1);
    let _ = lender.next();
    assert!(lender.is_empty());
    assert_eq!(lender.len(), 0);
}

#[test]
fn test_right_iterator_is_empty() {
    use lender::ExactSizeLender;
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let right = Right(g);
    let mut lender = right.iter();
    assert!(!lender.is_empty());
    assert_eq!(lender.len(), 2);
    let _ = lender.next();
    assert_eq!(lender.len(), 1);
    let _ = lender.next();
    assert!(lender.is_empty());
}

// ── BTreeGraph: outdegree, num_arcs_hint, into_lender ──

#[test]
fn test_btree_graph_outdegree() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let g = BTreeGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    assert_eq!(g.outdegree(0), 2);
    assert_eq!(g.outdegree(1), 1);
    assert_eq!(g.outdegree(2), 1);
    Ok(())
}

#[test]
fn test_btree_graph_num_arcs_hint() {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2)]);
    assert_eq!(g.num_arcs_hint(), Some(2));
}

#[test]
fn test_btree_graph_into_lender() -> Result<()> {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut count = 0;
    for_!((_node, succ) in &g {
        let _ = succ.count();
        count += 1;
    });
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn test_btree_graph_successors_len() {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let g = BTreeGraph::from_arcs([(0, 1), (0, 2), (0, 3)]);
    let succ = g.successors(0);
    assert_eq!(succ.len(), 3);
}

// ── LabeledBTreeGraph: outdegree, num_arcs_hint, into_lender, default ──

#[test]
fn test_labeled_btree_graph_default() {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let g = LabeledBTreeGraph::<u32>::default();
    assert_eq!(g.num_nodes(), 0);
}

#[test]
fn test_labeled_btree_graph_outdegree() {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    assert_eq!(RandomAccessLabeling::outdegree(&g, 0), 2);
    assert_eq!(RandomAccessLabeling::outdegree(&g, 1), 1);
    assert_eq!(RandomAccessLabeling::outdegree(&g, 2), 0);
}

#[test]
fn test_labeled_btree_graph_num_arcs_hint() {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    assert_eq!(g.num_arcs_hint(), Some(2));
}

#[test]
fn test_labeled_btree_graph_into_lender() -> Result<()> {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let mut count = 0;
    for_!((_node, succ) in &g {
        let _ = succ.count();
        count += 1;
    });
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn test_labeled_btree_graph_successors_len() {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((0, 3), 30)]);
    let succ = RandomAccessLabeling::labels(&g, 0);
    assert_eq!(succ.len(), 3);
}

// ── CsrGraph: accessors and factory methods ──

#[test]
fn test_csr_graph_new() {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = CsrGraph::new();
    assert_eq!(g.num_nodes(), 0);
}

#[test]
fn test_csr_graph_accessors() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let csr = CsrGraph::from_seq_graph(&g);
    // Test dcf() accessor
    assert_eq!(csr.dcf().len(), 4); // num_nodes + 1
    // Test successors() accessor
    assert_eq!(csr.successors().len(), 3); // total arcs
    // Test into_inner()
    let (dcf, succ) = csr.into_inner();
    assert_eq!(dcf.len(), 4);
    assert_eq!(succ.len(), 3);
    Ok(())
}

#[test]
fn test_csr_graph_from_lender() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let csr = CsrGraph::from_lender(g.iter());
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(
        RandomAccessLabeling::labels(&csr, 0).collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(
        RandomAccessLabeling::labels(&csr, 1).collect::<Vec<_>>(),
        vec![2]
    );
    Ok(())
}

#[test]
fn test_csr_graph_from_sorted_lender() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let csr = CsrGraph::from_sorted_lender(g.iter());
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(RandomAccessLabeling::outdegree(&csr, 0), 2);
    Ok(())
}

#[test]
fn test_csr_sorted_graph_from_lender() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrSortedGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0), (2, 1)]);
    let csr = CsrSortedGraph::from_lender(g.iter());
    assert_eq!(csr.num_nodes(), 3);
    // Should support successors() via RandomAccessGraph
    assert_eq!(csr.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    Ok(())
}

#[test]
fn test_csr_sorted_graph_from_seq_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrSortedGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CsrSortedGraph::from_seq_graph(&g);
    graph::eq(&g, &csr)?;
    Ok(())
}

#[test]
fn test_compressed_csr_graph_try_from_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CompressedCsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CompressedCsrGraph::try_from_graph(&g)?;
    assert_eq!(csr.num_nodes(), 3);
    // Check successors via RandomAccessLabeling
    assert_eq!(
        RandomAccessLabeling::labels(&csr, 0).collect::<Vec<_>>(),
        vec![1, 2]
    );
    Ok(())
}

#[test]
fn test_compressed_csr_sorted_graph_try_from_graph() -> Result<()> {
    use webgraph::graphs::csr_graph::CompressedCsrSortedGraph;
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CompressedCsrSortedGraph::try_from_graph(&g)?;
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(csr.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    Ok(())
}

#[test]
fn test_csr_graph_num_arcs_hint() {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let csr = CsrGraph::from_seq_graph(&g);
    assert_eq!(csr.num_arcs_hint(), Some(2));
}

#[test]
fn test_csr_graph_into_lender() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrGraph;
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let csr = CsrGraph::from_seq_graph(&g);
    let mut count = 0;
    for_!((_node, succ) in &csr {
        let _ = succ.count();
        count += 1;
    });
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn test_csr_sorted_graph_into_lender() -> Result<()> {
    use webgraph::graphs::csr_graph::CsrSortedGraph;
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let csr = CsrSortedGraph::from_seq_graph(&g);
    let mut count = 0;
    for_!((_node, succ) in &csr {
        let _ = succ.count();
        count += 1;
    });
    assert_eq!(count, 3);
    Ok(())
}

// ── DFS: stack, reset, interrupted visits ──

#[test]
fn test_dfs_interrupted_visit_stack() -> Result<()> {
    use webgraph::visits::depth_first;
    // Linear chain: 0->1->2->3->4
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visit = depth_first::SeqPred::new(&g);
    // Visit and interrupt at node 3
    let interrupted_node = visit.visit([0], |event| {
        if let depth_first::EventPred::Previsit { node, .. } = event {
            if node == 3 {
                return std::ops::ControlFlow::Break(node);
            }
        }
        std::ops::ControlFlow::Continue(())
    });
    assert_eq!(interrupted_node, std::ops::ControlFlow::Break(3));
    // After interruption, stack should contain the path from root
    let stack_nodes: Vec<usize> = visit.stack().collect();
    // Stack returns nodes in reverse order excluding the interrupted node
    assert!(!stack_nodes.is_empty());
    Ok(())
}

#[test]
fn test_dfs_reset() -> Result<()> {
    use webgraph::visits::depth_first;
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = depth_first::SeqPred::new(&g);
    // Do a full visit
    let _: Vec<_> = (&mut visit).into_iter().collect();
    // Reset
    visit.reset();
    // Visit again - should work the same
    let nodes: Vec<_> = (&mut visit).into_iter().map(|e| e.node).collect();
    assert_eq!(nodes, vec![0, 1, 2]);
    Ok(())
}

#[test]
fn test_dfs_no_pred_reset() -> Result<()> {
    use webgraph::visits::depth_first;
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = depth_first::SeqNoPred::new(&g);
    // Use callback visit
    use no_break::NoBreak;
    visit
        .visit([0], |_event| std::ops::ControlFlow::Continue(()))
        .continue_value_no_break();
    // Reset and visit again
    visit.reset();
    visit
        .visit([0], |_event| std::ops::ControlFlow::Continue(()))
        .continue_value_no_break();
    Ok(())
}

// ── BFS: BfsOrder and reset ──

#[test]
fn test_bfs_into_iter_order() -> Result<()> {
    use webgraph::visits::breadth_first;
    // Tree: 0->{1,2}, 1->{3}, 2->{4}
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 4)]);
    let mut visit = breadth_first::Seq::new(&g);
    let nodes: Vec<usize> = (&mut visit).into_iter().map(|e| e.node).collect();
    // BFS order should visit level by level
    assert_eq!(nodes[0], 0);
    // Nodes 1 and 2 should be at distance 1
    assert!(nodes[1..3].contains(&1));
    assert!(nodes[1..3].contains(&2));
    Ok(())
}

// ── BTreeGraph: add_arc panic on missing node ──

#[test]
#[should_panic(expected = "does not exist")]
fn test_btree_graph_add_arc_missing_src() {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let mut g = BTreeGraph::new();
    g.add_node(0);
    g.add_node(1);
    // Node 2 doesn't exist
    g.add_arc(2, 0);
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_btree_graph_add_arc_missing_dst() {
    use webgraph::graphs::btree_graph::BTreeGraph;
    let mut g = BTreeGraph::new();
    g.add_node(0);
    g.add_node(1);
    // Node 5 doesn't exist as destination
    g.add_arc(0, 5);
}

// ── LabeledBTreeGraph: remove_arc panic on missing node ──

#[test]
#[should_panic(expected = "does not exist")]
fn test_labeled_btree_graph_remove_arc_missing_node() {
    use webgraph::graphs::btree_graph::LabeledBTreeGraph;
    let mut g = LabeledBTreeGraph::<u32>::new();
    g.add_node(0);
    // Node 5 doesn't exist
    g.remove_arc(5, 0);
}
