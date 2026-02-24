/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for graph wrappers: UnionGraph, NoSelfLoopsGraph, PermutedGraph, ErdosRenyi.

use anyhow::Result;
use lender::*;
use webgraph::{
    graphs::{
        no_selfloops_graph::NoSelfLoopsGraph, permuted_graph::PermutedGraph, random::ErdosRenyi,
        union_graph::UnionGraph, vec_graph::VecGraph,
    },
    prelude::*,
    traits::SequentialLabeling,
};

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

#[test]
fn test_union_graph_num_arcs_hint() {
    let g0 = VecGraph::from_arcs([(0, 1)]);
    let g1 = VecGraph::from_arcs([(1, 0)]);
    let u = UnionGraph(g0, g1);
    assert_eq!(u.num_arcs_hint(), None);
}

#[test]
fn test_union_graph_eq_original() -> Result<()> {
    use webgraph::traits::graph;

    let basename = std::path::Path::new("../data/cnr-2000");
    let g1 = BvGraph::with_basename(basename).load()?;
    let g2 = BvGraph::with_basename(basename).load()?;
    let g_ref = BvGraph::with_basename(basename).load()?;
    let union = UnionGraph(g1, g2);

    // The union of a graph with itself should be equal to the original graph
    graph::eq(&union, &g_ref)?;
    Ok(())
}

// ── NoSelfLoopsGraph ──

#[test]
fn test_no_selfloops_complete() -> Result<()> {
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

#[test]
fn test_no_selfloops_graph_into_lender() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 0), (0, 1), (1, 1), (1, 2), (2, 0), (2, 2)]);
    let nsl = NoSelfLoopsGraph(g);

    assert_eq!(nsl.num_nodes(), 3);

    let mut arcs = vec![];
    for_!((node, succs) in &nsl {
        for succ in succs {
            arcs.push((node, succ));
        }
    });
    arcs.sort();
    assert_eq!(arcs, vec![(0, 1), (1, 2), (2, 0)]);
    Ok(())
}

// ── PermutedGraph ──

#[test]
fn test_permuted_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let perm = [1, 2, 0]; // 0->1, 1->2, 2->0
    let pg = PermutedGraph {
        graph: &g,
        perm: &perm,
    };
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

#[test]
fn test_permuted_graph_iter_from() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let perm = vec![2, 0, 1];
    let pg = PermutedGraph {
        graph: &g,
        perm: &perm,
    };
    let mut iter = pg.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![1]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![2]);
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn test_permuted_graph_into_lender() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let perm = [2, 0, 1]; // node 0 -> 2, node 1 -> 0, node 2 -> 1
    let pg = PermutedGraph {
        graph: &g,
        perm: &perm,
    };

    assert_eq!(pg.num_nodes(), 3);

    let mut arcs = vec![];
    for_!((node, succs) in &pg {
        for succ in succs {
            arcs.push((node, succ));
        }
    });
    arcs.sort();
    assert_eq!(arcs, vec![(0, 1), (1, 2), (2, 0)]);
    Ok(())
}

// ── ErdosRenyi ──

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
