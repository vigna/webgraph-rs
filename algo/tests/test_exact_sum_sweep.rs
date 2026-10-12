/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

mod common;

use anyhow::Result;
use common::{ess, graph_and_transpose, symm_graph};
use dsi_progress_logger::no_logging;
use std::sync::atomic::Ordering;
use sux::bits::AtomicBitVec;
use sux::traits::AtomicBitVecOps;
use webgraph::graphs::random::ErdosRenyi;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::traits::{NodeLabelsLender, RandomAccessGraph, SequentialLabeling};
use webgraph_algo::distances::exact_sum_sweep::*;

/// Returns an [`AtomicBitVec`] of length `n` with the given bits set.
fn radial(n: usize, nodes: &[usize]) -> AtomicBitVec {
    let rv = AtomicBitVec::new(n);
    for &node in nodes {
        rv.set(node, true, Ordering::Relaxed);
    }
    rv
}

/// Returns the (forward) eccentricities computed by breadth-first visits;
/// unreachable nodes are ignored.
fn bfs_eccentricities(graph: &impl RandomAccessGraph) -> Vec<usize> {
    let n = graph.num_nodes();
    (0..n)
        .map(|start| {
            let mut dist = vec![usize::MAX; n];
            let mut queue = std::collections::VecDeque::from([start]);
            dist[start] = 0;
            let mut ecc = 0;
            while let Some(node) = queue.pop_front() {
                ecc = dist[node];
                for succ in graph.successors(node) {
                    if dist[succ] == usize::MAX {
                        dist[succ] = dist[node] + 1;
                        queue.push_back(succ);
                    }
                }
            }
            ecc
        })
        .collect()
}

#[test]
fn test_path() {
    let (graph, transpose) = graph_and_transpose(3, [(0, 1), (1, 2), (2, 1), (1, 0)]);
    for result in ess::run::<All>(&graph, &transpose, || None) {
        assert_eq!(result.forward_eccentricities.as_ref(), &[2, 1, 2]);
        assert_eq!(result.backward_eccentricities[0], 2);
        assert_eq!((result.radius, result.diameter), (1, 2));
        assert_eq!(result.radial_vertex, 1);
        assert!(result.diametral_vertex == 2 || result.diametral_vertex == 0);
    }
}

#[test]
fn test_many_scc() {
    let (graph, transpose) = graph_and_transpose(
        7,
        [
            (0, 1),
            (1, 0),
            (1, 2),
            (2, 1),
            (6, 2),
            (2, 6),
            (3, 4),
            (4, 3),
            (4, 5),
            (5, 4),
            (0, 3),
            (0, 4),
            (1, 5),
            (1, 4),
            (2, 5),
        ],
    );
    for result in ess::run::<All>(&graph, &transpose, || None) {
        assert_eq!(result.radius, 2);
        assert_eq!(result.radial_vertex, 1);
    }
}

#[test]
fn test_lozenge() {
    let (graph, transpose) = graph_and_transpose(4, [(0, 1), (1, 0), (0, 2), (1, 3), (2, 3)]);
    for result in ess::run::<Radius>(&graph, &transpose, || None) {
        assert_eq!(result.radius, 2);
        assert!(result.radial_vertex == 0 || result.radial_vertex == 1);
    }
}

#[test]
fn test_many_dir_path() {
    let (graph, transpose) = graph_and_transpose(
        19,
        [
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 4),
            (5, 6),
            (6, 7),
            (7, 8),
            (8, 9),
            (9, 10),
            (10, 18),
            (11, 12),
            (13, 14),
            (14, 15),
            (15, 16),
            (16, 17),
        ],
    );
    for result in ess::run::<All>(&graph, &transpose, || Some(radial(19, &[8, 16]))) {
        assert_eq!((result.radius, result.diameter), (1, 6));
        assert_eq!(result.radial_vertex, 16);
        assert!(result.diametral_vertex == 5 || result.diametral_vertex == 18);
    }
}

#[test]
fn test_cycle() {
    for size in [3, 5, 7] {
        let (graph, transpose) = graph_and_transpose(size, (0..size).map(|i| (i, (i + 1) % size)));
        for result in ess::run::<RadiusDiameter>(&graph, &transpose, || None) {
            assert_eq!((result.radius, result.diameter), (size - 1, size - 1));
        }
    }
}

#[test]
fn test_clique() {
    for size in [10, 50, 100] {
        let (graph, transpose) = graph_and_transpose(
            size,
            (0..size).flat_map(|i| (0..size).filter(move |&j| j != i).map(move |j| (i, j))),
        );
        let candidates = [0, size / 2, size - 1];
        for result in ess::run::<All>(&graph, &transpose, || Some(radial(size, &candidates))) {
            assert_eq!(result.forward_eccentricities.as_ref(), vec![1; size]);
            assert!(candidates.contains(&result.radial_vertex));
        }
    }
}

#[test]
fn test_no_arcs() {
    for size in [2, 100] {
        let graph = VecGraph::empty(size);
        for result in ess::run::<All>(&graph, &graph, || None) {
            assert_eq!((result.radius, result.diameter), (0, 0));
        }
    }
}

#[test]
fn test_sparse() {
    let (graph, transpose) = graph_and_transpose(66, [(10, 32), (10, 65), (65, 10), (21, 44)]);
    for result in ess::run::<All>(&graph, &transpose, || None) {
        assert_eq!(result.radius, 1);
        assert_eq!(result.radial_vertex, 10);
    }
}

#[test]
fn test_no_radial_vertices() {
    let (graph, transpose) = graph_and_transpose(2, [(0, 1)]);
    for result in ess::run::<All>(&graph, &transpose, || Some(AtomicBitVec::new(2))) {
        assert_eq!(result.radius, usize::MAX);
    }
}

#[test]
#[should_panic]
fn test_empty_graph() {
    let graph = VecGraph::new();
    All::run(&graph, &graph, None, true, no_logging![]);
}

#[test]
fn test_er() -> Result<()> {
    for d in 2..=4 {
        let graph = VecGraph::from_lender(ErdosRenyi::new(100, (d as f64) / 100.0, 0).iter());
        let (graph, transpose) = graph_and_transpose(100, graph.iter().into_pairs());
        let eccentricities = bfs_eccentricities(&graph);
        for result in ess::run::<All>(&graph, &transpose, || None) {
            assert_eq!(
                result.forward_eccentricities.as_ref(),
                eccentricities.as_slice()
            );
        }
    }
    Ok(())
}

#[test]
fn test_symm_radius_backward_pivot() {
    // Regression: symmetric backward sweeps finalized the eccentricity of
    // the start vertex without lowering the radius upper bound, so
    // `Radius::run_symm` could return a non-minimal eccentricity. On this
    // graph the true radius is 2, but the buggy version returned 3.
    let graph = symm_graph(
        9,
        [
            (0, 1),
            (0, 5),
            (1, 2),
            (1, 7),
            (2, 3),
            (3, 4),
            (3, 8),
            (4, 5),
            (4, 6),
            (5, 6),
            (6, 7),
            (7, 8),
        ],
    );
    for result in ess::run_symm::<Radius>(&graph) {
        assert_eq!(result.radius, 2);
    }
}

/// Deterministic 64-bit LCG for the brute-force test below.
fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

#[test]
fn test_symm_brute_force_small() {
    let mut state = 0x243F_6A88_85A3_08D3u64;
    for trial in 0..400u32 {
        // n in 3..=10; the modulus keeps values tiny, so the cast is sound
        let n = 3 + usize::try_from(lcg(&mut state) % 8).unwrap();
        let mut arcs = std::collections::BTreeSet::new();
        // Spanning path for connectivity, so that all nodes are radial.
        for i in 0..n - 1 {
            arcs.insert((i, i + 1));
        }
        for _ in 0..n {
            let u = usize::try_from(lcg(&mut state) % u64::try_from(n).unwrap()).unwrap();
            let v = usize::try_from(lcg(&mut state) % u64::try_from(n).unwrap()).unwrap();
            if u != v {
                arcs.insert((u.min(v), u.max(v)));
            }
        }
        let graph = symm_graph(n, arcs.iter().copied());
        let eccs = bfs_eccentricities(&graph);
        let radius_true = *eccs.iter().min().unwrap();
        let diameter_true = *eccs.iter().max().unwrap();

        for result in ess::run_symm::<Radius>(&graph) {
            assert_eq!(result.radius, radius_true, "trial {trial} arcs {arcs:?}");
        }
        for result in ess::run_symm::<All>(&graph) {
            assert_eq!(
                (result.radius, result.diameter),
                (radius_true, diameter_true),
                "trial {trial} arcs {arcs:?}"
            );
            assert_eq!(
                result.eccentricities.as_ref(),
                eccs.as_slice(),
                "trial {trial} arcs {arcs:?}"
            );
        }
    }
}
