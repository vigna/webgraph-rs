/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for [`ParSortedGraph`], [`ParallelGraph`], and [`ParallelDcfGraph`].

mod common;

use anyhow::Result;
use common::build_ef;
use dsi_bitstream::prelude::BE;
use webgraph::graphs::bvgraph::{BvComp, BvGraphSeq};
use webgraph::graphs::par_graphs::ParGraph;
use webgraph::graphs::par_sorted_graph::ParSortedGraph;
use webgraph::graphs::permuted_graph::PermutedGraph;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;
use webgraph::traits::graph;
use webgraph::utils::par_sort_iters::ParSortIters;

/// Builds the canonical test graph (5 nodes, 7 arcs).
///
/// ```text
/// 0 -> 1, 2, 3   (outdegree 3, indegree 1 from node 2)
/// 1 -> 2          (outdegree 1, indegree 1 from node 0)
/// 2 -> 0          (outdegree 1, indegree 2 from nodes 0, 1)
/// 3 ->            (outdegree 0, indegree 1 from node 0)
/// 4 -> 0, 2       (outdegree 2, indegree 0)
/// ```
///
/// Contains: a cycle (0 -> 1 -> 2 -> 0), outdegree 0 (node 3), 1 (nodes 1, 2),
/// 2 (node 4), 3 (node 0); indegree 0 (node 4), 1 (nodes 1, 3),
/// 2 (node 2), 3 (node 0).
fn test_graph() -> VecGraph {
    VecGraph::from_arcs([(0, 1), (0, 2), (0, 3), (1, 2), (2, 0), (4, 0), (4, 2)])
}

// ── SortedGraph ──

#[test]
fn test_sorted_graph_preserves_graph() -> Result<()> {
    let g = test_graph();
    let sorted = ParSortedGraph::par_from(&g)?;
    graph::eq(&g, &sorted)?;
    Ok(())
}

#[test]
fn test_sorted_graph_from_permuted() -> Result<()> {
    // Verifies that sorting the arcs from a permuted graph yields the
    // same result as the reference (permuted) VecGraph. We collect
    // permuted pairs via iteration and sort them with ParSortIters.
    let g = test_graph();
    let perm = [0, 1, 2, 3, 4]; // identity permutation
    let pg = PermutedGraph::new(&g, &perm);

    let num_nodes = pg.num_nodes();
    let pairs: Vec<(usize, usize)> = pg.iter().into_pairs().collect();

    let par_sort = ParSortIters::new(num_nodes)?.num_partitions(2);
    let split = par_sort.sort(vec![pairs])?;
    let sorted = ParSortedGraph::from_parts(split.boundaries, split.iters);

    graph::eq(&g, &sorted)?;
    Ok(())
}

#[test]
fn test_sorted_graph_par_iters_boundaries() -> Result<()> {
    let g = test_graph();
    let sorted = ParSortedGraph::par_from(&g)?;
    let (_lenders, boundaries) = sorted.into_par_lenders();
    // Boundaries must start at 0 and end at num_nodes
    assert_eq!(*boundaries.first().unwrap(), 0);
    assert_eq!(*boundaries.last().unwrap(), g.num_nodes());
    // Boundaries must be non-decreasing
    for w in boundaries.windows(2) {
        assert!(w[0] <= w[1]);
    }
    Ok(())
}

#[test]
fn test_sorted_graph_with_part() -> Result<()> {
    let g = test_graph();
    let sorted = ParSortedGraph::config().num_partitions(2).par_sort(&g)?;
    graph::eq(&g, &sorted)?;
    // 2 partitions means 3 boundary points
    let (_lenders, boundaries) = sorted.into_par_lenders();
    assert_eq!(boundaries.len(), 3);
    assert_eq!(boundaries[0], 0);
    assert_eq!(*boundaries.last().unwrap(), g.num_nodes());
    Ok(())
}

#[test]
fn test_sorted_graph_from_parts() -> Result<()> {
    let g = test_graph();
    let num_nodes = g.num_nodes();

    let pairs: Vec<_> = g.split_iter(2).map(|lender| lender.into_pairs()).collect();

    let par_sort = ParSortIters::new(num_nodes)?.num_partitions(2);
    let split = par_sort.sort(pairs)?;

    let sorted = ParSortedGraph::from_parts(split.boundaries, split.iters);
    assert_eq!(sorted.num_nodes(), num_nodes);
    graph::eq(&g, &sorted)?;
    Ok(())
}

// ── ParallelGraph ──

#[test]
fn test_parallel_graph_custom_partitions() -> Result<()> {
    let g = test_graph();
    let pg = ParGraph::new(g, 3);
    let (lenders, boundaries) = pg.into_par_lenders();
    assert_eq!(lenders.len(), 3);
    assert_eq!(boundaries.len(), 4);
    assert_eq!(boundaries[0], 0);
    assert_eq!(*boundaries.last().unwrap(), 5);
    Ok(())
}

#[test]
fn test_parallel_graph_delegates_random_access() -> Result<()> {
    let g = test_graph();
    let pg = ParGraph::new(g, 2);
    assert_eq!(pg.num_arcs(), 7);
    assert_eq!(pg.outdegree(0), 3);
    assert_eq!(pg.outdegree(1), 1);
    assert_eq!(pg.outdegree(2), 1);
    assert_eq!(pg.outdegree(3), 0);
    assert_eq!(pg.outdegree(4), 2);
    Ok(())
}

#[test]
fn test_parallel_graph_graph_equality() -> Result<()> {
    let g = test_graph();
    let pg = ParGraph::new(g.clone(), 2);
    graph::eq(&g, &pg)?;
    Ok(())
}

// ── Integration: par_comp roundtrips ──

#[test]
fn test_par_comp_with_sorted_graph() -> Result<()> {
    let g = test_graph();
    let sorted = ParSortedGraph::par_from(&g)?;

    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("sorted");
    BvComp::with_basename(&basename).par_comp::<BE, _>(&sorted)?;

    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    assert_eq!(seq.num_nodes(), 5);
    assert_eq!(seq.num_arcs_hint(), Some(7));
    labels::eq_sorted(&g, &seq)?;
    Ok(())
}

#[test]
fn test_par_comp_with_parallel_graph() -> Result<()> {
    let g = test_graph();
    let pg = ParGraph::new(g.clone(), 2);

    let dir = tempfile::tempdir()?;
    let basename = dir.path().join("parallel");
    BvComp::with_basename(&basename).par_comp::<BE, _>(&pg)?;

    build_ef(&basename)?;
    let loaded = BvGraph::with_basename(&basename).load()?;
    assert_eq!(loaded.num_nodes(), 5);
    assert_eq!(loaded.num_arcs(), 7);
    labels::eq_sorted(&g, &loaded)?;
    Ok(())
}
