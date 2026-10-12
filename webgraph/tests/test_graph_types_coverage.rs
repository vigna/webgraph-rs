/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for VecGraph, LabeledVecGraph, BTreeGraph, LabeledBTreeGraph, and CsrGraph types.

mod common;

use anyhow::Result;
use common::{TEST_GRAPH_ARCS, test_graph, vec_graph};
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
        labels,
    },
};

/// Checks that a random-access graph is equal to `expected`, also through its
/// random-access methods, `iter_from`, and `get_num_arcs`.
fn check_graph<G: RandomAccessGraph>(g: &G, expected: &VecGraph) -> Result<()>
where
    for<'a> G::Lender<'a>: SortedLender,
{
    let n = expected.num_nodes();
    graph::eq(g, expected)?;
    labels::check_impl(g)?;
    assert_eq!(g.get_num_arcs(), Some(expected.num_arcs()));
    for src in 0..n {
        assert_eq!(g.outdegree(src), expected.outdegree(src));
        for dst in 0..n {
            assert_eq!(
                g.has_arc(src, dst),
                expected.successors(src).any(|succ| succ == dst)
            );
        }
    }
    let from = n / 2;
    let arcs: Vec<_> = expected
        .iter()
        .into_pairs()
        .filter(|&(s, _)| s >= from)
        .collect();
    graph::eq(
        &VecGraph::from_lender(g.iter_from(from)),
        &vec_graph(n, arcs),
    )?;
    Ok(())
}

/// Checks that a labeled graph is equal to `expected`, also through its
/// random-access methods, `iter_from`, `get_num_arcs`, and `IntoLender`.
macro_rules! check_labeled {
    ($g:expr, $expected:expr) => {{
        let (g, expected) = ($g, $expected);
        graph::eq_labeled(g, expected)?;
        labels::check_impl(g)?;
        graph::eq_labeled(&LabeledVecGraph::from_lender(g), expected)?;
        assert_eq!(g.get_num_arcs(), Some(expected.num_arcs()));
        for node in 0..expected.num_nodes() {
            assert_eq!(
                RandomAccessLabeling::outdegree(g, node),
                RandomAccessLabeling::outdegree(expected, node)
            );
            assert_eq!(
                RandomAccessLabeling::labels(g, node).len(),
                RandomAccessLabeling::outdegree(expected, node)
            );
        }
        let mut iter = g.iter_from(1);
        let (node, succ) = iter.next().unwrap();
        assert_eq!(node, 1);
        assert!(
            succ.into_iter()
                .eq(RandomAccessLabeling::labels(expected, 1))
        );
    }};
}

/// The test graph, labeled with 10 times the source plus the target.
fn labeled_arcs() -> [((usize, usize), u32); 11] {
    TEST_GRAPH_ARCS.map(|(s, d)| ((s, d), (10 * s + d) as u32))
}

// ── VecGraph ──

#[test]
fn test_vec_graph() -> Result<()> {
    let expected = test_graph();
    // The test graph is the oracle of most tests, so we pin it down
    assert_eq!(expected.num_nodes(), 8);
    assert_eq!(expected.num_arcs(), 11);
    let succs: Vec<Vec<usize>> = (0..expected.num_nodes())
        .map(|node| expected.successors(node).collect())
        .collect();
    assert_eq!(
        succs,
        [
            vec![1, 2],
            vec![3, 4, 5],
            vec![4],
            vec![6],
            vec![6],
            vec![6, 7],
            vec![2],
            vec![],
        ]
    );
    check_graph(&VecGraph::from_arcs(TEST_GRAPH_ARCS), &expected)?;
    graph::eq(&VecGraph::from_lender(&expected), &expected)?;

    let mut g = VecGraph::new();
    g.add_node(7);
    for (src, dst) in TEST_GRAPH_ARCS {
        g.add_arc(src, dst);
    }
    check_graph(&g, &expected)?;

    let empty = VecGraph::empty(5);
    assert_eq!(empty.num_nodes(), 5);
    assert_eq!(empty.num_arcs(), 0);
    check_graph(&empty, &vec_graph(5, []))?;
    Ok(())
}

#[test]
fn test_labeled_vec_graph() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs(labeled_arcs());
    // Pins down the labels, as the first check compares g with itself
    let labels: Vec<Vec<(usize, u32)>> = (0..g.num_nodes())
        .map(|node| RandomAccessLabeling::labels(&g, node).collect())
        .collect();
    assert_eq!(
        labels,
        [
            vec![(1, 1), (2, 2)],
            vec![(3, 13), (4, 14), (5, 15)],
            vec![(4, 24)],
            vec![(6, 36)],
            vec![(6, 46)],
            vec![(6, 56), (7, 57)],
            vec![(2, 62)],
            vec![],
        ]
    );
    check_labeled!(&g, &g);
    assert!(LabeledRandomAccessGraph::has_arc(&g, 0, 1));
    assert!(!LabeledRandomAccessGraph::has_arc(&g, 1, 0));

    let mut copy = LabeledVecGraph::from_lender(g.iter());
    copy.shrink_to_fit();
    check_labeled!(&copy, &g);

    // Floating-point labels
    let g = LabeledVecGraph::<f64>::from_arcs([((0, 1), 1.0), ((0, 2), 2.5), ((1, 2), 3.0)]);
    let succs: Vec<_> = RandomAccessLabeling::labels(&g, 0).collect();
    assert_eq!(succs, vec![(1, 1.0), (2, 2.5)]);

    assert_eq!(LabeledVecGraph::<u32>::default().num_nodes(), 0);
    Ok(())
}

// ── BTreeGraph ──

#[test]
fn test_btree_graph() -> Result<()> {
    let expected = test_graph();
    check_graph(&BTreeGraph::from_arcs(TEST_GRAPH_ARCS), &expected)?;
    check_graph(&BTreeGraph::from_lender(expected.iter()), &expected)?;
    graph::eq(
        &VecGraph::from_lender(&BTreeGraph::from_arcs(TEST_GRAPH_ARCS)),
        &expected,
    )?;

    // add_arcs keeps successors sorted, whatever the input order
    let mut g = BTreeGraph::new();
    g.add_arcs(TEST_GRAPH_ARCS.into_iter().rev());
    check_graph(&g, &expected)?;
    g.shrink_to_fit();
    check_graph(&g, &expected)?;

    check_graph(&BTreeGraph::empty(5), &vec_graph(5, []))?;

    // The successors of a BTreeGraph have an exact size
    let g = BTreeGraph::from_arcs(TEST_GRAPH_ARCS);
    for node in 0..g.num_nodes() {
        assert_eq!(g.successors(node).len(), expected.outdegree(node));
    }
    Ok(())
}

#[test]
fn test_btree_graph_add_node_and_arc() {
    let mut g = BTreeGraph::new();
    assert!(g.add_node(0));
    assert!(!g.add_node(0));
    assert!(g.add_node(5));
    assert_eq!(g.num_nodes(), 6); // nodes 0..=5
    assert!(g.add_arc(0, 1));
    assert!(!g.add_arc(0, 1)); // duplicate
    assert_eq!(g.num_arcs(), 1);
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_btree_graph_add_arc_missing_src() {
    let mut g = BTreeGraph::empty(2);
    g.add_arc(2, 0);
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_btree_graph_add_arc_missing_dst() {
    let mut g = BTreeGraph::empty(2);
    g.add_arc(0, 5);
}

// ── LabeledBTreeGraph ──

#[test]
fn test_labeled_btree_graph() -> Result<()> {
    let expected = LabeledVecGraph::<u32>::from_arcs(labeled_arcs());
    check_labeled!(
        &LabeledBTreeGraph::<u32>::from_arcs(labeled_arcs()),
        &expected
    );
    check_labeled!(&LabeledBTreeGraph::from_lender(expected.iter()), &expected);

    let mut g = LabeledBTreeGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20)]);
    assert!(g.remove_arc(0, 1));
    assert_eq!(g.num_arcs(), 1);
    assert!(!g.remove_arc(0, 1)); // already removed
    assert_eq!(g.num_arcs(), 1);

    assert_eq!(LabeledBTreeGraph::<u32>::default().num_nodes(), 0);
    Ok(())
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_labeled_btree_graph_remove_arc_missing_node() {
    let mut g = LabeledBTreeGraph::<u32>::new();
    g.add_node(0);
    g.remove_arc(5, 0);
}

// ── CsrGraph ──

#[test]
fn test_csr_graphs() -> Result<()> {
    let g = test_graph();
    check_graph(&CsrGraph::from_lender(g.iter()), &g)?;
    check_graph(&CsrGraph::from_sorted_lender(g.iter()), &g)?;
    check_graph(&CsrGraph::from_seq_graph(&g), &g)?;
    check_graph(&CsrSortedGraph::from_lender(g.iter()), &g)?;
    check_graph(&CsrSortedGraph::from_seq_graph(&g), &g)?;
    check_graph(&CompressedCsrGraph::try_from_graph(&g)?, &g)?;
    check_graph(&CompressedCsrSortedGraph::try_from_graph(&g)?, &g)?;
    graph::eq(&VecGraph::from_lender(&CsrGraph::from_seq_graph(&g)), &g)?;
    graph::eq(
        &VecGraph::from_lender(&CsrSortedGraph::from_seq_graph(&g)),
        &g,
    )?;

    let csr = CsrGraph::default();
    assert_eq!(csr.num_nodes(), 0);
    assert_eq!(csr.num_arcs(), 0);
    Ok(())
}

#[test]
fn test_csr_graph_accessors() {
    let g = test_graph();
    let csr = CsrGraph::from_seq_graph(&g);
    assert_eq!(csr.dcf().len(), g.num_nodes() + 1);
    assert_eq!(csr.successors().len(), 11);
    let (dcf, succ) = csr.into_inner();
    assert_eq!(dcf.len(), g.num_nodes() + 1);
    assert_eq!(succ.len(), 11);
}
