/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

use std::ops::ControlFlow::Continue;

use anyhow::Result;
use dsi_progress_logger::no_logging;
use no_break::NoBreak;
use sux::bits::AtomicBitVec;
use sux::traits::AtomicBitVecOps;
use webgraph::graphs::random::ErdosRenyi;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::BTreeGraph;
use webgraph::traits::SequentialLabeling;
use webgraph::transform::transpose_seq;
use webgraph::utils::MemoryUsage;
use webgraph::visits::Sequential;
use webgraph::visits::breadth_first::{EventPred, Seq};
use webgraph_algo::distances::exact_sum_sweep::*;

#[test]
fn test_path() -> Result<()> {
    let arcs = [(0_usize, 1_usize), (1, 2), (2, 1), (1, 0)];

    let graph = BTreeGraph::from_arcs(arcs.iter().copied());
    let transposed = BTreeGraph::from_arcs(arcs.iter().map(|(a, b)| (*b, *a)));

    for sum_sweep in [
        All::run(&graph, &transposed, None, true, no_logging![]),
        All::run(&graph, &transposed, None, false, no_logging![]),
    ] {
        assert_eq!(sum_sweep.forward_eccentricities[0], 2);
        assert_eq!(sum_sweep.forward_eccentricities[1], 1);
        assert_eq!(sum_sweep.forward_eccentricities[2], 2);
        assert_eq!(sum_sweep.backward_eccentricities[0], 2);
        assert_eq!(sum_sweep.diameter, 2);
        assert_eq!(sum_sweep.radius, 1);
        assert_eq!(sum_sweep.radial_vertex, 1);
        assert!(sum_sweep.diametral_vertex == 2 || sum_sweep.diametral_vertex == 0);
    }

    Ok(())
}

#[test]
fn test_many_scc() -> Result<()> {
    let arcs = vec![
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
    ];
    let transposed_arcs = arcs.iter().map(|(a, b)| (*b, *a)).collect::<Vec<_>>();

    let graph = VecGraph::from_arcs(arcs);
    let transposed = VecGraph::from_arcs(transposed_arcs);

    for sum_sweep in [
        All::run(&graph, &transposed, None, true, no_logging![]),
        All::run(&graph, &transposed, None, false, no_logging![]),
    ] {
        assert_eq!(sum_sweep.radius, 2);
        assert_eq!(sum_sweep.radial_vertex, 1);
    }

    Ok(())
}

#[test]
fn test_lozenge() -> Result<()> {
    let arcs = [(0, 1), (1, 0), (0, 2), (1, 3), (2, 3)];

    let graph = VecGraph::from_arcs(arcs);
    let transposed = VecGraph::from_arcs(arcs.iter().map(|(a, b)| (*b, *a)));

    for sum_sweep in [
        Radius::run(&graph, &transposed, None, true, no_logging![]),
        Radius::run(&graph, &transposed, None, false, no_logging![]),
    ] {
        assert_eq!(sum_sweep.radius, 2);
        assert!(sum_sweep.radial_vertex == 0 || sum_sweep.radial_vertex == 1);
    }

    Ok(())
}

#[test]
fn test_many_dir_path() -> Result<()> {
    let arcs = vec![
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
    ];

    let graph = BTreeGraph::from_arcs(arcs.iter().copied());
    let transpose = BTreeGraph::from_arcs(arcs.iter().map(|(x, y)| (*y, *x)));

    let make_radial = || {
        let rv = AtomicBitVec::new(19);
        rv.set(16, true, std::sync::atomic::Ordering::Relaxed);
        rv.set(8, true, std::sync::atomic::Ordering::Relaxed);
        rv
    };

    for sum_sweep in [
        All::run(&graph, &transpose, Some(make_radial()), true, no_logging![]),
        All::run(
            &graph,
            &transpose,
            Some(make_radial()),
            false,
            no_logging![],
        ),
    ] {
        assert_eq!(sum_sweep.diameter, 6);
        assert_eq!(sum_sweep.radius, 1);
        assert_eq!(sum_sweep.radial_vertex, 16);
        assert!(sum_sweep.diametral_vertex == 5 || sum_sweep.diametral_vertex == 18);
    }

    Ok(())
}

#[test]
fn test_cycle() -> Result<()> {
    for size in [3, 5, 7] {
        let mut vec_graph = VecGraph::new();
        for i in 0..size {
            vec_graph.add_node(i);
        }
        let mut transposed_vec_graph = vec_graph.clone();
        for i in 0..size {
            if i == size - 1 {
                vec_graph.add_arc(i, 0);
                transposed_vec_graph.add_arc(0, i);
            } else {
                vec_graph.add_arc(i, i + 1);
                transposed_vec_graph.add_arc(i + 1, i);
            }
        }

        let graph = vec_graph;
        let transposed = transposed_vec_graph;

        for sum_sweep in [
            RadiusDiameter::run(&graph, &transposed, None, true, no_logging![]),
            RadiusDiameter::run(&graph, &transposed, None, false, no_logging![]),
        ] {
            assert_eq!(sum_sweep.diameter, size - 1);
            assert_eq!(sum_sweep.radius, size - 1);
        }
    }

    Ok(())
}

#[test]
fn test_clique() -> Result<()> {
    for size in [10, 50, 100] {
        let mut vec_graph = VecGraph::new();
        for i in 0..size {
            vec_graph.add_node(i);
        }
        for i in 0..size {
            for j in 0..size {
                if i != j {
                    vec_graph.add_arc(i, j);
                }
            }
        }

        let graph = vec_graph.clone();
        let transposed = vec_graph;
        let candidates = [0, size / 2, size - 1];

        let make_radial = || {
            let rv = AtomicBitVec::new(size);
            rv.set(candidates[0], true, std::sync::atomic::Ordering::Relaxed);
            rv.set(candidates[1], true, std::sync::atomic::Ordering::Relaxed);
            rv.set(candidates[2], true, std::sync::atomic::Ordering::Relaxed);
            rv
        };

        for sum_sweep in [
            All::run(
                &graph,
                &transposed,
                Some(make_radial()),
                true,
                no_logging![],
            ),
            All::run(
                &graph,
                &transposed,
                Some(make_radial()),
                false,
                no_logging![],
            ),
        ] {
            for i in 0..size {
                assert_eq!(sum_sweep.forward_eccentricities[i], 1);
            }
            assert!(candidates.contains(&sum_sweep.radial_vertex));
        }
    }

    Ok(())
}

#[test]
fn test_empty() -> Result<()> {
    let mut vec_graph = VecGraph::new();
    for i in 0..100 {
        vec_graph.add_node(i);
    }

    let graph = vec_graph.clone();
    let transposed = vec_graph;

    for sum_sweep in [
        All::run(&graph, &transposed, None, true, no_logging![]),
        All::run(&graph, &transposed, None, false, no_logging![]),
    ] {
        assert_eq!(sum_sweep.radius, 0);
        assert_eq!(sum_sweep.diameter, 0);
    }

    Ok(())
}

#[test]
fn test_sparse() -> Result<()> {
    let arcs = [(10, 32), (10, 65), (65, 10), (21, 44)];
    let graph = BTreeGraph::from_arcs(arcs.iter().copied());
    let transpose = BTreeGraph::from_arcs(arcs.iter().map(|(x, y)| (*y, *x)));

    for sum_sweep in [
        All::run(&graph, &transpose, None, true, no_logging![]),
        All::run(&graph, &transpose, None, false, no_logging![]),
    ] {
        assert_eq!(sum_sweep.radius, 1);
        assert_eq!(sum_sweep.radial_vertex, 10);
    }

    Ok(())
}

#[test]
fn test_no_radial_vertices() -> Result<()> {
    let arcs = vec![(0, 1)];

    let mut vec_graph = VecGraph::new();
    for i in 0..2 {
        vec_graph.add_node(i);
    }
    let mut transposed_vec_graph = vec_graph.clone();
    for arc in arcs {
        vec_graph.add_arc(arc.0, arc.1);
        transposed_vec_graph.add_arc(arc.1, arc.0);
    }

    let graph = vec_graph;
    let transposed = transposed_vec_graph;

    for sum_sweep in [
        All::run(
            &graph,
            &transposed,
            Some(AtomicBitVec::new(2)),
            true,
            no_logging![],
        ),
        All::run(
            &graph,
            &transposed,
            Some(AtomicBitVec::new(2)),
            false,
            no_logging![],
        ),
    ] {
        assert_eq!(sum_sweep.radius, usize::MAX);
    }

    Ok(())
}

#[test]
#[should_panic]
fn test_empty_graph() {
    let vec_graph = VecGraph::new();

    let graph = vec_graph.clone();
    let transposed = vec_graph;

    All::run(&graph, &transposed, None, true, no_logging![]);
}

#[test]
fn test_graph_no_edges() -> Result<()> {
    let mut vec_graph = VecGraph::new();
    for i in 0..2 {
        vec_graph.add_node(i);
    }

    let graph = vec_graph.clone();
    let transposed = vec_graph;

    for sum_sweep in [
        All::run(&graph, &transposed, None, true, no_logging![]),
        All::run(&graph, &transposed, None, false, no_logging![]),
    ] {
        assert_eq!(sum_sweep.radius, 0);
        assert_eq!(sum_sweep.diameter, 0);
    }

    Ok(())
}

#[allow(clippy::needless_range_loop)]
#[test]
fn test_er() -> Result<()> {
    for d in 2..=4 {
        let graph = VecGraph::from_lender(ErdosRenyi::new(100, (d as f64) / 100.0, 0).iter());

        let trans = VecGraph::from_lender(
            transpose_seq(&graph, MemoryUsage::BatchSize(1000), no_logging![])?.iter(),
        );

        for ess in [
            All::run(&graph, &trans, None, true, no_logging![]),
            All::run(&graph, &trans, None, false, no_logging![]),
        ] {
            let mut pll = Seq::new(&graph);
            let mut ecc = [0; 100];
            for root in 0..100 {
                pll.visit([root], |event| {
                    if let EventPred::Visit { distance, .. } = event {
                        ecc[root] = ecc[root].max(distance);
                    }
                    Continue(())
                })
                .continue_value_no_break();
                pll.reset();
            }

            for node in 0..100 {
                assert_eq!(
                    ess.forward_eccentricities[node], ecc[node],
                    "node = {}, actual = {}, expected = {}",
                    node, ess.forward_eccentricities[node], ecc[node]
                );
            }
        }
    }

    Ok(())
}

#[test]
fn test_symm_radius_backward_pivot() -> Result<()> {
    // Regression: symmetric backward sweeps finalized the eccentricity of
    // the start vertex without lowering the radius upper bound, so
    // `Radius::run_symm` could return a non-minimal eccentricity. On this
    // graph the true radius is 2, but the buggy version returned 3.
    let arcs = [
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
    ];
    let mut graph = VecGraph::empty(9);
    for &(u, v) in &arcs {
        graph.add_arc(u, v);
        graph.add_arc(v, u);
    }
    for use_tot in [true, false] {
        let res = Radius::run_symm(&graph, use_tot, no_logging![]);
        assert_eq!(res.radius, 2);
    }
    Ok(())
}

/// Deterministic 64-bit LCG for the brute-force test below.
fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

/// BFS eccentricities of a connected symmetric graph.
fn bfs_eccentricities(graph: &VecGraph) -> Vec<usize> {
    use webgraph::traits::RandomAccessGraph;
    let n = graph.num_nodes();
    let mut eccs = vec![0; n];
    for start in 0..n {
        let mut dist = vec![usize::MAX; n];
        let mut queue = std::collections::VecDeque::new();
        dist[start] = 0;
        queue.push_back(start);
        while let Some(node) = queue.pop_front() {
            for succ in graph.successors(node) {
                if dist[succ] == usize::MAX {
                    dist[succ] = dist[node] + 1;
                    queue.push_back(succ);
                }
            }
        }
        eccs[start] = dist.into_iter().max().unwrap();
    }
    eccs
}

#[test]
fn test_symm_brute_force_small() -> Result<()> {
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
        let mut graph = VecGraph::empty(n);
        for &(u, v) in &arcs {
            graph.add_arc(u, v);
            graph.add_arc(v, u);
        }
        let eccs = bfs_eccentricities(&graph);
        let radius_true = *eccs.iter().min().unwrap();
        let diameter_true = *eccs.iter().max().unwrap();

        for use_tot in [true, false] {
            let res = Radius::run_symm(&graph, use_tot, no_logging![]);
            assert_eq!(
                res.radius, radius_true,
                "radius mismatch on trial {trial} (use_tot {use_tot}) arcs {arcs:?}"
            );
            let res = All::run_symm(&graph, use_tot, no_logging![]);
            assert_eq!(
                res.radius, radius_true,
                "all-radius mismatch on trial {trial} (use_tot {use_tot}) arcs {arcs:?}"
            );
            assert_eq!(
                res.diameter, diameter_true,
                "diameter mismatch on trial {trial} (use_tot {use_tot}) arcs {arcs:?}"
            );
            assert_eq!(
                res.eccentricities.as_ref(),
                eccs.as_slice(),
                "ecc mismatch on trial {trial} (use_tot {use_tot}) arcs {arcs:?}"
            );
        }
    }
    Ok(())
}
