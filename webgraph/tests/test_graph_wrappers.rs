/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for graph wrappers: UnionGraph, NoSelfLoopsGraph, PermutedGraph, ErdosRenyi.

mod common;

use anyhow::Result;
use common::vec_graph;
use lender::*;
#[cfg(not(miri))]
use webgraph::prelude::BvGraph;
use webgraph::{
    graphs::{
        no_selfloops_graph::NoSelfLoopsGraph, permuted_graph::PermutedGraph, random::ErdosRenyi,
        union_graph::UnionGraph, vec_graph::VecGraph,
    },
    traits::{SequentialLabeling, SplitLabeling, graph},
};

// ── UnionGraph ──

#[test]
fn test_union_graph() -> Result<()> {
    let u = UnionGraph(
        VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]),
        VecGraph::from_arcs([(0, 2), (1, 3), (2, 0)]),
    );
    let expected = vec_graph(4, [(0, 1), (0, 2), (1, 2), (1, 3), (2, 0), (2, 3)]);
    graph::eq(&VecGraph::from_lender(u.iter()), &expected)?;
    graph::eq(
        &VecGraph::from_lender(u.iter_from(2)),
        &vec_graph(4, [(2, 0), (2, 3)]),
    )?;
    assert_eq!(u.get_num_arcs(), None);
    Ok(())
}

#[test]
fn test_union_graph_different_sizes() -> Result<()> {
    // Regression: split_iter forwarded union-sized cutpoints to both
    // children, panicking on the smaller one.
    let u = UnionGraph(vec_graph(2, [(0, 1)]), vec_graph(4, [(0, 1), (2, 3)]));
    let expected = vec_graph(4, [(0, 1), (2, 3)]);
    graph::eq(&VecGraph::from_lender(u.iter()), &expected)?;
    // Split iteration agrees with sequential iteration
    let mut arcs = vec![];
    for lender in u.split_iter(2) {
        for_!((node, succ) in lender {
            arcs.extend(succ.into_iter().map(|s| (node, s)));
        });
    }
    assert_eq!(arcs, [(0, 1), (2, 3)]);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_union_graph_eq_original() -> Result<()> {
    // The union of a graph with itself is equal to the original graph
    let basename = common::cnr_2000_basename();
    let g1 = BvGraph::with_basename(&basename).load()?;
    let g2 = BvGraph::with_basename(&basename).load()?;
    let g_ref = BvGraph::with_basename(&basename).load()?;
    graph::eq(&UnionGraph(g1, g2), &g_ref)?;
    Ok(())
}

// ── NoSelfLoopsGraph ──

#[test]
fn test_no_selfloops() -> Result<()> {
    let nsl = NoSelfLoopsGraph(VecGraph::from_arcs([
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (1, 2),
        (2, 0),
        (2, 2),
    ]));
    graph::eq(&nsl, &vec_graph(3, [(0, 1), (1, 0), (1, 2), (2, 0)]))?;
    graph::eq(
        &VecGraph::from_lender(nsl.iter_from(1)),
        &vec_graph(3, [(1, 0), (1, 2), (2, 0)]),
    )?;
    // Regression: the hint forwarded the underlying arc count, overstating
    // the filtered graph (the type docs say no exact count is available).
    assert_eq!(nsl.get_num_arcs(), None);
    Ok(())
}

// ── PermutedGraph ──

#[test]
fn test_permuted_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let perm = [2, 0, 1]; // node 0 -> 2, node 1 -> 0, node 2 -> 1
    let pg = PermutedGraph::new(&g, &perm);
    let expected = vec_graph(3, [(0, 1), (1, 2), (2, 0)]);
    graph::eq(&VecGraph::from_lender(&pg), &expected)?;
    // The permuted graph returns the nodes in the original order
    graph::eq(
        &VecGraph::from_lender(pg.iter_from(1)),
        &vec_graph(3, [(0, 1), (1, 2)]),
    )?;
    Ok(())
}

// ── ErdosRenyi ──

#[test]
fn test_erdos_renyi() -> Result<()> {
    // p = 0 means no arcs
    graph::eq(&ErdosRenyi::new(50, 0.0, 0), &VecGraph::empty(50))?;
    // p = 1 means all possible arcs except self-loops
    let complete = vec_graph(
        10,
        (0..10).flat_map(|i| (0..10).filter(move |&j| j != i).map(move |j| (i, j))),
    );
    graph::eq(&ErdosRenyi::new(10, 1.0, 0), &complete)?;
    Ok(())
}
