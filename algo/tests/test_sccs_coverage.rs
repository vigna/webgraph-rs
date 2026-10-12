/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use dsi_progress_logger::no_logging;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph_algo::sccs::{self, Sccs};

#[test]
fn test_sccs_sizes() {
    let sccs = Sccs::new(5, vec![0, 1, 2, 3, 4].into_boxed_slice());
    assert_eq!(sccs.num_components(), 5);
    let sccs = Sccs::new(1, vec![0, 0, 0, 0].into_boxed_slice());
    assert_eq!(sccs.compute_sizes(), vec![4].into_boxed_slice());
    let sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
    assert_eq!(sccs.compute_sizes(), vec![1, 1, 1].into_boxed_slice());
}

#[test]
fn test_sccs_sort_by_size() {
    for par in [false, true] {
        let sort = |sccs: &mut Sccs| {
            if par {
                sccs.par_sort_by_size()
            } else {
                sccs.sort_by_size()
            }
        };
        let mut sccs = Sccs::new(3, vec![0, 1, 1, 1, 0, 2].into_boxed_slice());
        assert_eq!(sort(&mut sccs), vec![3, 2, 1].into_boxed_slice());
        assert_eq!(sccs.components().to_owned(), vec![1, 0, 0, 0, 1, 2]);

        let mut sccs = Sccs::new(1, vec![0, 0, 0].into_boxed_slice());
        assert_eq!(sort(&mut sccs), vec![3].into_boxed_slice());

        // All components have size 1, so the sizes are trivially [1, 1, 1]
        let mut sccs = Sccs::new(3, vec![0, 1, 2].into_boxed_slice());
        assert_eq!(sort(&mut sccs), vec![1, 1, 1].into_boxed_slice());
    }
}

#[test]
fn test_tarjan() {
    for (graph, num_components) in [
        (VecGraph::empty(1), 1),
        (VecGraph::from_arcs([(0, 0)]), 1),
        (VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]), 2),
    ] {
        assert_eq!(
            sccs::tarjan(graph, no_logging![]).num_components(),
            num_components
        );
    }
}

#[test]
fn test_symm() {
    for (graph, num_components) in [
        (VecGraph::empty(1), 1),
        (VecGraph::from_arcs([(0, 1), (1, 0), (2, 3), (3, 2)]), 2),
        (
            VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 1), (0, 2), (2, 0)]),
            1,
        ),
    ] {
        assert_eq!(
            sccs::symm_seq(&graph, no_logging![]).num_components(),
            num_components
        );
        #[cfg(not(miri))]
        assert_eq!(
            sccs::symm_par(&graph, no_logging![]).num_components(),
            num_components
        );
    }
}
