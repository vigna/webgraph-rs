/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

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
use webgraph::transform::transpose;
use webgraph::utils::MemoryUsage;
use webgraph::visits::Sequential;
use webgraph::visits::breadth_first::{EventPred, Seq};
use webgraph_algo::distances::exact_sum_sweep::*;

#[test]
fn test_path() -> Result<()> {
    let arcs = vec![(0_usize, 1_usize), (1, 2), (2, 1), (1, 0)];

    let mut vec_graph = BTreeGraph::from_arcs(arcs.iter().copied());
    let mut transposed_vec_graph = vec_graph.clone();
    for arc in arcs {
        vec_graph.add_arc(arc.0, arc.1);
        transposed_vec_graph.add_arc(arc.1, arc.0);
    }

    let graph = vec_graph;
    let transposed = transposed_vec_graph;

    let sum_sweep = All::run(&graph, &transposed, None, no_logging![]);

    assert_eq!(sum_sweep.forward_eccentricities[0], 2);
    assert_eq!(sum_sweep.forward_eccentricities[1], 1);
    assert_eq!(sum_sweep.forward_eccentricities[2], 2);
    assert_eq!(sum_sweep.backward_eccentricities[0], 2);
    assert_eq!(sum_sweep.diameter, 2);
    assert_eq!(sum_sweep.radius, 1);
    assert_eq!(sum_sweep.radial_vertex, 1);
    assert!(sum_sweep.diametral_vertex == 2 || sum_sweep.diametral_vertex == 0);

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

    let sum_sweep = All::run(&graph, &transposed, None, no_logging![]);

    assert_eq!(sum_sweep.radius, 2);
    assert_eq!(sum_sweep.radial_vertex, 1);

    Ok(())
}

#[test]
fn test_lozenge() -> Result<()> {
    let arcs = vec![(0, 1), (1, 0), (0, 2), (1, 3), (2, 3)];

    let mut graph = VecGraph::new();
    for i in 0..4 {
        graph.add_node(i);
    }
    let mut transpose = graph.clone();
    for arc in arcs {
        graph.add_arc(arc.0, arc.1);
        transpose.add_arc(arc.1, arc.0);
    }

    let sum_sweep = Radius::run(graph, transpose, None, no_logging![]);

    assert_eq!(sum_sweep.radius, 2);
    assert!(sum_sweep.radial_vertex == 0 || sum_sweep.radial_vertex == 1);

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
    let radial_vertices = AtomicBitVec::new(19);
    radial_vertices.set(16, true, std::sync::atomic::Ordering::Relaxed);
    radial_vertices.set(8, true, std::sync::atomic::Ordering::Relaxed);

    let sum_sweep = All::run(graph, transpose, Some(radial_vertices), no_logging![]);

    assert_eq!(sum_sweep.diameter, 6);
    assert_eq!(sum_sweep.radius, 1);
    assert_eq!(sum_sweep.radial_vertex, 16);
    assert!(sum_sweep.diametral_vertex == 5 || sum_sweep.diametral_vertex == 18);

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

        let sum_sweep = RadiusDiameter::run(&graph, &transposed, None, no_logging![]);

        assert_eq!(sum_sweep.diameter, size - 1);
        assert_eq!(sum_sweep.radius, size - 1);
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
        let radial_vertices = AtomicBitVec::new(size);
        let rngs = [
            rand::random::<u64>() as usize % size,
            rand::random::<u64>() as usize % size,
            rand::random::<u64>() as usize % size,
        ];
        radial_vertices.set(rngs[0], true, std::sync::atomic::Ordering::Relaxed);
        radial_vertices.set(rngs[1], true, std::sync::atomic::Ordering::Relaxed);
        radial_vertices.set(rngs[2], true, std::sync::atomic::Ordering::Relaxed);

        let sum_sweep = All::run(&graph, &transposed, Some(radial_vertices), no_logging![]);

        for i in 0..size {
            assert_eq!(sum_sweep.forward_eccentricities[i], 1);
        }
        assert!(rngs.contains(&sum_sweep.radial_vertex));
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

    let sum_sweep = All::run(&graph, &transposed, None, no_logging![]);

    assert_eq!(sum_sweep.radius, 0);
    assert_eq!(sum_sweep.diameter, 0);

    Ok(())
}

#[test]
fn test_sparse() -> Result<()> {
    let arcs = [(10, 32), (10, 65), (65, 10), (21, 44)];
    let graph = BTreeGraph::from_arcs(arcs.iter().copied());
    let transpose = BTreeGraph::from_arcs(arcs.iter().map(|(x, y)| (*y, *x)));
    let sum_sweep = All::run(graph, transpose, None, no_logging![]);
    assert_eq!(sum_sweep.radius, 1);
    assert_eq!(sum_sweep.radial_vertex, 10);
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
    let radial_vertices = AtomicBitVec::new(2);

    let sum_sweep = All::run(&graph, &transposed, Some(radial_vertices), no_logging![]);

    assert_eq!(sum_sweep.radius, usize::MAX);

    Ok(())
}

#[test]
#[should_panic]
fn test_empty_graph() {
    let vec_graph = VecGraph::new();

    let graph = vec_graph.clone();
    let transposed = vec_graph;

    All::run(&graph, &transposed, None, no_logging![]);
}

#[test]
fn test_graph_no_edges() -> Result<()> {
    let mut vec_graph = VecGraph::new();
    for i in 0..2 {
        vec_graph.add_node(i);
    }

    let graph = vec_graph.clone();
    let transposed = vec_graph;

    let sum_sweep = All::run(&graph, &transposed, None, no_logging![]);

    assert_eq!(sum_sweep.radius, 0);
    assert_eq!(sum_sweep.diameter, 0);

    Ok(())
}

#[allow(clippy::needless_range_loop)]
#[test]
fn test_er() -> Result<()> {
    for d in 2..=4 {
        let graph = VecGraph::from_lender(ErdosRenyi::new(100, (d as f64) / 100.0, 0).iter());

        let transpose = VecGraph::from_lender(transpose(&graph, MemoryUsage::default())?.iter());

        let ess = All::run(&graph, transpose, None, no_logging![]);

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
                node, ess.backward_eccentricities[node], ecc[node]
            );
        }
    }

    Ok(())
}
