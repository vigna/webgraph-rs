/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_progress_logger::prelude::*;
use epserde::{deser::Deserialize, ser::Serialize};
use lender::for_;
use sux::bit_vec;
use sux::traits::BitVecOpsMut;
use webgraph::graphs::random::ErdosRenyi;
use webgraph::prelude::{BTreeGraph, BvGraph};
use webgraph::thread_pool;
use webgraph::transform;
use webgraph::utils::MemoryUsage;
use webgraph::{graphs::vec_graph::VecGraph, traits::SequentialLabeling};
use webgraph_algo::sccs::{self, Sccs};

#[test]
fn test_compute_sizes() -> Result<()> {
    let sccs = Sccs::new(3, vec![0, 0, 0, 1, 2, 2, 1, 2, 0, 0].into_boxed_slice());

    assert_eq!(sccs.compute_sizes(), vec![5, 2, 3].into_boxed_slice());

    Ok(())
}

#[test]
fn test_sort_by_size() -> Result<()> {
    let mut sccs = Sccs::new(3, vec![0, 1, 1, 1, 0, 2].into_boxed_slice());

    sccs.sort_by_size();

    assert_eq!(sccs.components().to_owned(), vec![1, 0, 0, 0, 1, 2]);

    Ok(())
}

#[test]
fn test_epserde_roundtrip() -> Result<()> {
    let original = Sccs::new(3, vec![0, 0, 0, 1, 2, 2, 1, 2, 0, 0].into_boxed_slice());

    let mut file = std::io::Cursor::new(vec![]);
    unsafe { original.serialize(&mut file) }?;
    let data = file.into_inner();
    let deserialized = unsafe { <Sccs>::deserialize_eps(&data) }?;

    assert_eq!(original.num_components(), deserialized.num_components());
    assert_eq!(original.components(), deserialized.components());

    Ok(())
}

macro_rules! test_scc_algo {
    ($scc:expr, $name:ident) => {
        mod $name {
            use super::*;

            #[test]
            fn test_buckets() -> Result<()> {
                let arcs = [
                    (0, 0),
                    (1, 0),
                    (1, 2),
                    (2, 1),
                    (2, 3),
                    (2, 4),
                    (2, 5),
                    (3, 4),
                    (4, 3),
                    (5, 5),
                    (5, 6),
                    (5, 7),
                    (5, 8),
                    (6, 7),
                    (8, 7),
                ];
                let transposed_arcs = arcs.iter().map(|(a, b)| (*b, *a)).collect::<Vec<_>>();

                let graph = VecGraph::from_arcs(arcs);
                let transposed_graph = VecGraph::from_arcs(transposed_arcs);

                let mut components = $scc(&graph, &transposed_graph, &thread_pool![], no_logging![]);

                assert_eq!(components.components()[3], components.components()[4]);

                let mut buckets = bit_vec![false; graph.num_nodes()];
                buckets.set(0, true);
                buckets.set(3, true);
                buckets.set(4, true);

                let sizes = components.sort_by_size();
                assert_eq!(sizes, vec![2, 2, 1, 1, 1, 1, 1].into_boxed_slice());

                Ok(())
            }

            #[test]
            fn test_buckets_2() -> Result<()> {
                let arcs = [(0, 1), (1, 2), (2, 0), (1, 3)];
                let transposed_arcs = arcs.iter().map(|(a, b)| (*b, *a)).collect::<Vec<_>>();

                let graph = VecGraph::from_arcs(arcs);
                let transposed_graph = VecGraph::from_arcs(transposed_arcs);

                let mut components = $scc(&graph, &transposed_graph, &thread_pool![], no_logging![]);
                let sizes = components.sort_by_size();

                assert_eq!(sizes, vec![3, 1].into_boxed_slice());

                Ok(())
            }

            #[test]
            fn test_cycle() -> Result<()> {
                let arcs = [(0, 1), (1, 2), (2, 3), (3, 0)];
                let transposed_arcs = arcs.iter().map(|(a, b)| (*b, *a)).collect::<Vec<_>>();

                let graph = VecGraph::from_arcs(arcs);
                let transposed_graph = VecGraph::from_arcs(transposed_arcs);

                let components = $scc(&graph, &transposed_graph, &thread_pool![], no_logging![]);
                let sizes = components.compute_sizes();

                assert_eq!(sizes, vec![4].into_boxed_slice());

                Ok(())
            }

            #[test]
            fn test_complete_graph() -> Result<()> {
                let mut g = VecGraph::new();
                for i in 0..5 {
                    g.add_node(i);
                }
                let mut t = g.clone();
                for i in 0..5 {
                    for j in 0..5 {
                        if i != j {
                            g.add_arc(i, j);
                            t.add_arc(j, i);
                        }
                    }
                }

                let graph = g;
                let transposed_graph = t;

                let mut components = $scc(&graph, &transposed_graph, &thread_pool![], no_logging![]);

                let sizes = components.sort_by_size();

                for i in 0..5 {
                    assert_eq!(components.components()[i], 0);
                }
                assert_eq!(sizes, vec![5].into_boxed_slice());

                Ok(())
            }

            #[test]
            fn test_tree() -> Result<()> {
                let arcs = [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6)];
                let transposed_arcs = arcs.iter().map(|(a, b)| (*b, *a)).collect::<Vec<_>>();

                let graph = VecGraph::from_arcs(arcs);
                let transposed_graph = VecGraph::from_arcs(transposed_arcs);

                let components = $scc(&graph, &transposed_graph, &thread_pool![], no_logging![]);

                assert_eq!(components.num_components(), 7);

                Ok(())
            }
        }
    };
}

test_scc_algo!(|g, _, _, pl| sccs::tarjan(g, pl), tarjan);
test_scc_algo!(|g, t, _, pl| sccs::kosaraju(g, t, pl), kosaraju);

#[test]
fn test_large() -> Result<()> {
    let basename = "../data/cnr-2000";

    let graph = BvGraph::with_basename(basename).load()?;
    let transpose = BvGraph::with_basename(basename.to_string() + "-t").load()?;

    let kosaraju = sccs::kosaraju(&graph, &transpose, no_logging![]);
    let tarjan = sccs::tarjan(&graph, no_logging![]);

    assert_eq!(kosaraju.num_components(), 100977);
    assert_eq!(tarjan.num_components(), 100977);

    let num_nodes = graph.num_nodes();
    for x in (0..num_nodes).step_by(1000) {
        for y in (x + 1..num_nodes).step_by(1000) {
            assert_eq!(
                tarjan.components()[x] == tarjan.components()[y],
                kosaraju.components()[x] == kosaraju.components()[y]
            );
        }
    }

    Ok(())
}

#[test]
fn test_er() -> Result<()> {
    for n in (10..=100).step_by(10) {
        for d in 1..10 {
            let graph = VecGraph::from_lender(ErdosRenyi::new(n, (d as f64) / 10.0, 0).iter());

            let transpose = VecGraph::from_lender(
                transform::transpose(&graph, MemoryUsage::BatchSize(10000))?.iter(),
            );
            let kosaraju = sccs::kosaraju(&graph, &transpose, no_logging![]);
            let tarjan = sccs::tarjan(&graph, no_logging![]);

            assert_eq!(kosaraju.num_components(), tarjan.num_components());
        }
    }
    Ok(())
}

#[test]
fn test_lozenge() -> Result<()> {
    let arcs = [(0, 1), (1, 0), (0, 2), (1, 3), (2, 3)];
    let graph = VecGraph::from_arcs(arcs);

    let components = sccs::tarjan(&graph, no_logging![]);

    assert_eq!(components.components(), &[2, 2, 1, 0]);

    Ok(())
}

#[test]
fn test_er_symm() -> Result<()> {
    for n in (10..=100).step_by(10) {
        for d in 1..10 {
            let er = ErdosRenyi::new(n, (d as f64) / 10.0, 0).iter();
            let mut sym_graph = BTreeGraph::new();
            sym_graph.add_node(n - 1);
            for_!((src, succ) in er {
                for dst in succ {
                    sym_graph.add_arc(src, dst);
                    sym_graph.add_arc(dst, src);
                }
            });
            let symm_par = sccs::symm_par(&sym_graph, &thread_pool![], no_logging![]);
            let symm_seq = sccs::symm_seq(&sym_graph, no_logging![]);
            let tarjan = sccs::tarjan(sym_graph, no_logging![]);
            assert_eq!(symm_seq.num_components(), tarjan.num_components());
            assert_eq!(symm_par.num_components(), tarjan.num_components());
        }
    }
    Ok(())
}
