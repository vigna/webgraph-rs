/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_progress_logger::no_logging;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::SequentialLabeling;
use webgraph::transform;
use webgraph::utils::MemoryUsage;
use webgraph_algo::sccs::{self, Sccs};

#[test]
fn test_sccs_par_sort_by_size() -> Result<()> {
    let mut sccs = Sccs::new(3, vec![0, 1, 1, 1, 0, 2].into_boxed_slice());
    let sizes = sccs.par_sort_by_size();
    // Should give the same result as sort_by_size
    assert_eq!(sizes, vec![3, 2, 1].into_boxed_slice());
    assert_eq!(sccs.components().to_owned(), vec![1, 0, 0, 0, 1, 2]);
    Ok(())
}

#[test]
fn test_sccs_num_components() {
    let sccs = Sccs::new(5, vec![0, 1, 2, 3, 4].into_boxed_slice());
    assert_eq!(sccs.num_components(), 5);
}

#[test]
fn test_sccs_single_component() {
    let sccs = Sccs::new(1, vec![0, 0, 0, 0].into_boxed_slice());
    assert_eq!(sccs.compute_sizes(), vec![4].into_boxed_slice());
}

#[test]
fn test_sccs_par_sort_single_component() {
    let mut sccs = Sccs::new(1, vec![0, 0, 0].into_boxed_slice());
    let sizes = sccs.par_sort_by_size();
    assert_eq!(sizes, vec![3].into_boxed_slice());
}

#[test]
fn test_tarjan_empty_graph() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let sccs = sccs::tarjan(g, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_tarjan_self_loop() {
    let graph = VecGraph::from_arcs([(0, 0)]);
    let sccs = sccs::tarjan(graph, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_tarjan_two_cycles() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]);
    let sccs = sccs::tarjan(graph, no_logging![]);
    assert_eq!(sccs.num_components(), 2);
}

#[test]
fn test_tarjan_chain() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let sccs = sccs::tarjan(graph, no_logging![]);
    assert_eq!(sccs.num_components(), 4);
}

#[test]
fn test_kosaraju_chain() -> Result<()> {
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let transpose =
        VecGraph::from_lender(transform::transpose(&graph, MemoryUsage::BatchSize(10000))?.iter());
    let sccs = sccs::kosaraju(&graph, &transpose, no_logging![]);
    assert_eq!(sccs.num_components(), 4);
    Ok(())
}

#[test]
fn test_symm_seq_disconnected() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]);
    let sccs = sccs::symm_seq(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 2);
}

#[test]
fn test_symm_par_disconnected() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]);
    let sccs = sccs::symm_par(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 2);
}

#[test]
fn test_symm_seq_single_node() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let sccs = sccs::symm_seq(g, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_symm_par_single_node() {
    let mut g = VecGraph::new();
    g.add_node(0);
    let sccs = sccs::symm_par(g, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_symm_seq_triangle() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (0, 2), (2, 0)]);
    let sccs = sccs::symm_seq(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_symm_par_triangle() {
    let graph = VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (0, 2), (2, 0)]);
    let sccs = sccs::symm_par(&graph, no_logging![]);
    assert_eq!(sccs.num_components(), 1);
}

#[test]
fn test_sccs_compute_sizes_empty() {
    let sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
    let sizes = sccs.compute_sizes();
    assert_eq!(sizes, vec![1, 1, 1].into_boxed_slice());
}

#[test]
fn test_sccs_sort_by_size_all_equal() {
    let mut sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
    let sizes = sccs.sort_by_size();
    // All components have size 1, so sorting is stable
    assert_eq!(sizes, vec![1, 1, 1].into_boxed_slice());
}

#[test]
fn test_sccs_par_sort_by_size_all_equal() {
    let mut sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
    let sizes = sccs.par_sort_by_size();
    assert_eq!(sizes, vec![1, 1, 1].into_boxed_slice());
}
