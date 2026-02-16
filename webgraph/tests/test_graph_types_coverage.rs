/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for VecGraph, LabeledVecGraph, BTreeGraph, LabeledBTreeGraph, and CsrGraph types.

use anyhow::Result;
use lender::*;
use webgraph::{
    graphs::{
        btree_graph::{BTreeGraph, LabeledBTreeGraph},
        csr_graph::{CompressedCsrGraph, CompressedCsrSortedGraph, CsrGraph, CsrSortedGraph},
        vec_graph::{LabeledVecGraph, VecGraph},
    },
    prelude::*,
    traits::{
        RandomAccessLabeling, SequentialLabeling,
        graph::{self, LabeledRandomAccessGraph},
    },
};

// ═══════════════════════════════════════════════════════════════════════
//  VecGraph
// ═══════════════════════════════════════════════════════════════════════

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
fn test_vec_graph_iter_from_v1() -> Result<()> {
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

#[test]
fn test_vec_graph_from_seq_graph_eq() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0), (2, 3)]);
    let copy = VecGraph::from_sorted_lender(g.iter());
    graph::eq(&g, &copy)?;
    Ok(())
}

#[test]
fn test_vec_graph_num_arcs_hint() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert_eq!(g.num_arcs_hint(), Some(3));
    Ok(())
}

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

#[test]
fn test_vec_graph_iter_from_v2() {
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let mut iter = g.iter_from(2);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    let succs: Vec<_> = succ.into_iter().collect();
    assert_eq!(succs, vec![3]);
}

// ═══════════════════════════════════════════════════════════════════════
//  LabeledVecGraph
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_labeled_vec_graph() -> Result<()> {
    let g = LabeledVecGraph::<f64>::from_arcs([((0, 1), 1.0), ((0, 2), 2.5), ((1, 2), 3.0)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 3);
    let succs: Vec<_> = RandomAccessLabeling::labels(&g, 0).collect();
    assert_eq!(succs, vec![(1, 1.0), (2, 2.5)]);
    Ok(())
}

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

#[test]
fn test_labeled_graph_has_arc() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    assert!(LabeledRandomAccessGraph::has_arc(&g, 0, 1));
    assert!(LabeledRandomAccessGraph::has_arc(&g, 0, 2));
    assert!(!LabeledRandomAccessGraph::has_arc(&g, 1, 0));
    assert!(LabeledRandomAccessGraph::has_arc(&g, 1, 2));
    assert!(!LabeledRandomAccessGraph::has_arc(&g, 2, 0));
    Ok(())
}

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

#[test]
fn test_labeled_vec_graph_iter_from() {
    let g = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((0, 2), 20),
        ((1, 3), 30),
        ((2, 3), 40),
    ]);
    // iter_from starts from a specific node
    let mut iter = g.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    let labels: Vec<_> = succ.into_iter().collect();
    assert_eq!(labels, vec![(3, 30)]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    let labels: Vec<_> = succ.into_iter().collect();
    assert_eq!(labels, vec![(3, 40)]);
}

// ═══════════════════════════════════════════════════════════════════════
//  BTreeGraph
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_btree_graph_add_arcs_v1() -> Result<()> {
    let mut g = BTreeGraph::new();
    g.add_arcs([(2, 0), (0, 2), (0, 1), (1, 2)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 4);
    // BTreeGraph keeps successors sorted
    assert_eq!(g.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    Ok(())
}

#[test]
fn test_btree_graph_from_arcs_v1() -> Result<()> {
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
    let v = VecGraph::from_arcs([(0, 2), (0, 1), (1, 0), (2, 1)]);
    let b = BTreeGraph::from_lender(v.iter());
    assert_eq!(b.num_nodes(), 3);
    assert_eq!(b.num_arcs(), 4);
    graph::eq(&v, &b)?;
    Ok(())
}

#[test]
fn test_btree_graph_add_node_v1() -> Result<()> {
    let mut g = BTreeGraph::new();
    assert!(g.add_node(0));
    assert!(!g.add_node(0));
    assert!(g.add_node(5));
    assert_eq!(g.num_nodes(), 6); // nodes 0..=5
    Ok(())
}

#[test]
fn test_btree_graph_duplicate_arc() -> Result<()> {
    let mut g = BTreeGraph::new();
    g.add_node(1);
    assert!(g.add_arc(0, 1));
    assert!(!g.add_arc(0, 1)); // duplicate
    assert_eq!(g.num_arcs(), 1);
    Ok(())
}

#[test]
fn test_btree_graph_iter_from_v1() -> Result<()> {
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

#[test]
fn test_btree_graph_shrink_to_fit() -> Result<()> {
    let mut g = BTreeGraph::from_arcs([(0, 1), (1, 2)]);
    g.shrink_to_fit();
    // Just verify it doesn't crash and the graph is intact
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 2);
    Ok(())
}

#[test]
fn test_btree_graph_outdegree() -> Result<()> {
    let g = BTreeGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    assert_eq!(g.outdegree(0), 2);
    assert_eq!(g.outdegree(1), 1);
    assert_eq!(g.outdegree(2), 1);
    Ok(())
}

#[test]
fn test_btree_graph_num_arcs_hint() {
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2)]);
    assert_eq!(g.num_arcs_hint(), Some(2));
}

#[test]
fn test_btree_graph_into_lender() -> Result<()> {
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
    let g = BTreeGraph::from_arcs([(0, 1), (0, 2), (0, 3)]);
    let succ = g.successors(0);
    assert_eq!(succ.len(), 3);
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_btree_graph_add_arc_missing_src() {
    let mut g = BTreeGraph::new();
    g.add_node(0);
    g.add_node(1);
    // Node 2 doesn't exist
    g.add_arc(2, 0);
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_btree_graph_add_arc_missing_dst() {
    let mut g = BTreeGraph::new();
    g.add_node(0);
    g.add_node(1);
    // Node 5 doesn't exist as destination
    g.add_arc(0, 5);
}

#[test]
fn test_btree_graph_basic_operations() {
    let mut g = BTreeGraph::empty(5);
    g.add_arc(0, 1);
    g.add_arc(0, 2);
    g.add_arc(1, 3);
    g.add_arc(3, 4);

    assert_eq!(g.num_nodes(), 5);
    assert_eq!(g.num_arcs(), 4);

    // Test successors
    let succs: Vec<_> = g.successors(0).collect();
    assert_eq!(succs, vec![1, 2]);

    // Test outdegree
    assert_eq!(g.outdegree(0), 2);
    assert_eq!(g.outdegree(1), 1);
    assert_eq!(g.outdegree(2), 0);
}

#[test]
fn test_btree_graph_add_arcs_v2() {
    let mut g = BTreeGraph::new();
    g.add_arcs([(0, 1), (1, 2), (2, 0), (0, 3)]);

    assert_eq!(g.num_nodes(), 4);
    assert_eq!(g.num_arcs(), 4);
    let succs: Vec<_> = g.successors(0).collect();
    assert_eq!(succs, vec![1, 3]);
}

#[test]
fn test_btree_graph_add_node_v2() {
    let mut g = BTreeGraph::new();
    assert!(g.add_node(0));
    assert!(!g.add_node(0)); // Already exists
    assert!(g.add_node(5)); // Adds nodes 1-5 as well
    assert_eq!(g.num_nodes(), 6);
}

#[test]
fn test_btree_graph_iter() {
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut iter = g.iter();

    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 0);
    assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![1]);

    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 1);
    assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![2]);

    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 2);
    assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![0]);

    assert!(iter.next().is_none());
}

#[test]
fn test_btree_graph_iter_from_v2() {
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut iter = g.iter_from(1);

    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 1);
    assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![2]);
}

#[test]
fn test_btree_graph_from_arcs_v2() {
    let g = BTreeGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 3);

    let mut iter = g.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1]);
}

// ═══════════════════════════════════════════════════════════════════════
//  LabeledBTreeGraph
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_labeled_btree_graph() -> Result<()> {
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    assert_eq!(g.num_nodes(), 3);
    assert_eq!(g.num_arcs(), 3);
    let succs: Vec<_> = RandomAccessLabeling::labels(&g, 0).collect();
    assert_eq!(succs, vec![(1, 10), (2, 20)]);
    Ok(())
}

#[test]
fn test_labeled_btree_graph_from_lender() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20), ((2, 0), 30)]);
    let b = LabeledBTreeGraph::from_lender(g.iter());
    assert_eq!(b.num_nodes(), 3);
    assert_eq!(b.num_arcs(), 3);
    graph::eq_labeled(&g, &b)?;
    Ok(())
}

#[test]
fn test_labeled_btree_graph_remove_arc_v1() -> Result<()> {
    let mut g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20)]);
    assert_eq!(g.num_arcs(), 2);
    assert!(g.remove_arc(0, 1));
    assert_eq!(g.num_arcs(), 1);
    assert!(!g.remove_arc(0, 1)); // already removed
    assert_eq!(g.num_arcs(), 1);
    Ok(())
}

#[test]
fn test_labeled_btree_graph_default() {
    let g = LabeledBTreeGraph::<u32>::default();
    assert_eq!(g.num_nodes(), 0);
}

#[test]
fn test_labeled_btree_graph_outdegree() {
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    assert_eq!(RandomAccessLabeling::outdegree(&g, 0), 2);
    assert_eq!(RandomAccessLabeling::outdegree(&g, 1), 1);
    assert_eq!(RandomAccessLabeling::outdegree(&g, 2), 0);
}

#[test]
fn test_labeled_btree_graph_num_arcs_hint() {
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    assert_eq!(g.num_arcs_hint(), Some(2));
}

#[test]
fn test_labeled_btree_graph_into_lender() -> Result<()> {
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
    let g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((0, 3), 30)]);
    let succ = RandomAccessLabeling::labels(&g, 0);
    assert_eq!(succ.len(), 3);
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_labeled_btree_graph_remove_arc_missing_node() {
    let mut g = LabeledBTreeGraph::<u32>::new();
    g.add_node(0);
    // Node 5 doesn't exist
    g.remove_arc(5, 0);
}

#[test]
fn test_labeled_btree_graph_remove_arc_v2() {
    let mut g = LabeledBTreeGraph::<u32>::empty(3);
    g.add_arc(0, 1, 10);
    g.add_arc(0, 2, 20);
    g.add_arc(1, 2, 30);

    assert_eq!(g.num_arcs(), 3);

    g.remove_arc(0, 1);
    assert_eq!(g.num_arcs(), 2);
    let succs: Vec<_> = g.successors(0).collect();
    assert_eq!(succs, vec![(2, 20)]);
}

#[test]
fn test_labeled_btree_graph_operations() {
    let mut g = LabeledBTreeGraph::<u32>::new();
    g.add_arcs([((0, 1), 10), ((0, 2), 20), ((1, 3), 30)]);

    assert_eq!(g.num_nodes(), 4);
    assert_eq!(g.num_arcs(), 3);

    // Test successors with labels
    let succs: Vec<_> = g.successors(0).collect();
    assert_eq!(succs, vec![(1, 10), (2, 20)]);

    // Test iter
    let mut iter = g.iter();
    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 0);
    assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![(1, 10), (2, 20)]);
}

// ═══════════════════════════════════════════════════════════════════════
//  CsrGraph
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_csr_graph() -> Result<()> {
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

#[test]
fn test_csr_graph_default() -> Result<()> {
    let csr = CsrGraph::default();
    assert_eq!(csr.num_nodes(), 0);
    assert_eq!(csr.num_arcs(), 0);
    Ok(())
}

#[test]
fn test_csr_graph_from_seq_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let csr = CsrGraph::from_seq_graph(&g);
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(csr.num_arcs(), 3);
    graph::eq(&g, &csr)?;
    Ok(())
}

#[test]
fn test_csr_sorted_graph_v1() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CsrSortedGraph::from_seq_graph(&g);
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(csr.num_arcs(), 4);
    graph::eq(&g, &csr)?;
    Ok(())
}

#[test]
fn test_compressed_csr_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let ccsr = CompressedCsrGraph::try_from_graph(&g)?;
    assert_eq!(ccsr.num_nodes(), 3);
    assert_eq!(ccsr.num_arcs(), 4);
    graph::eq(&g, &ccsr)?;
    Ok(())
}

#[test]
fn test_compressed_csr_sorted_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let ccsr = CompressedCsrSortedGraph::try_from_graph(&g)?;
    assert_eq!(ccsr.num_nodes(), 3);
    assert_eq!(ccsr.num_arcs(), 4);
    graph::eq(&g, &ccsr)?;
    Ok(())
}

#[test]
fn test_csr_graph_new() {
    let g = CsrGraph::new();
    assert_eq!(g.num_nodes(), 0);
}

#[test]
fn test_csr_graph_accessors() -> Result<()> {
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
fn test_csr_graph_from_lender_v1() -> Result<()> {
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
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let csr = CsrGraph::from_sorted_lender(g.iter());
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(RandomAccessLabeling::outdegree(&csr, 0), 2);
    Ok(())
}

#[test]
fn test_csr_sorted_graph_from_lender() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0), (2, 1)]);
    let csr = CsrSortedGraph::from_lender(g.iter());
    assert_eq!(csr.num_nodes(), 3);
    // Should support successors() via RandomAccessGraph
    assert_eq!(csr.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    Ok(())
}

#[test]
fn test_csr_sorted_graph_from_seq_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CsrSortedGraph::from_seq_graph(&g);
    graph::eq(&g, &csr)?;
    Ok(())
}

#[test]
fn test_compressed_csr_graph_try_from_graph() -> Result<()> {
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
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let csr = CompressedCsrSortedGraph::try_from_graph(&g)?;
    assert_eq!(csr.num_nodes(), 3);
    assert_eq!(csr.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    Ok(())
}

#[test]
fn test_csr_graph_num_arcs_hint() {
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let csr = CsrGraph::from_seq_graph(&g);
    assert_eq!(csr.num_arcs_hint(), Some(2));
}

#[test]
fn test_csr_graph_into_lender() -> Result<()> {
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

#[test]
fn test_csr_graph_basic() {
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let csr = CsrGraph::from_seq_graph(&g);
    assert_eq!(csr.num_nodes(), 4);
    assert_eq!(csr.num_arcs(), 4);
    assert_eq!(csr.outdegree(0), 2);
    // Iterate over the graph to verify content
    let mut iter = csr.iter();
    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 0);
    assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![1, 2]);
    let (n, s) = iter.next().unwrap();
    assert_eq!(n, 1);
    assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![3]);
}

#[test]
fn test_csr_graph_empty() {
    let csr = CsrGraph::new();
    assert_eq!(csr.num_nodes(), 0);
    assert_eq!(csr.num_arcs(), 0);
}

#[test]
fn test_csr_graph_from_lender_v2() {
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3)]);
    let csr = CsrGraph::from_lender(&g);

    assert_eq!(csr.num_nodes(), 4);
    assert_eq!(csr.num_arcs(), 3);
}

#[test]
fn test_csr_sorted_graph_v2() {
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (0, 2), (1, 3)]);
    let csr = CsrSortedGraph::from_seq_graph(&g);

    assert_eq!(csr.num_nodes(), 4);
    assert_eq!(csr.num_arcs(), 3);
}
