/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_bitstream::traits::BE;
use lender::prelude::*;

use webgraph::{
    graphs::{
        arc_list_graph::{ArcListGraph, Iter},
        btree_graph::LabeledBTreeGraph,
        vec_graph::LabeledVecGraph,
    },
    prelude::BvGraph,
    traits::{graph, NodeLabelsLender, RandomAccessLabeling, SequentialLabeling, SplitLabeling},
};

#[test]
fn test_arc_list_graph_iter() {
    let iter =
        Iter::<Box<u64>, std::vec::IntoIter<(usize, usize, Box<u64>)>>::new(10, vec![].into_iter());
    for_!((_succ, labels) in iter {
        for_!(item in labels {
          println!("{:?}", item);
        });
    });
}

fn test_graph_iters<I1, I2>(mut iter: I1, mut truth_iter: I2)
where
    I1: for<'next> NodeLabelsLender<'next, Label = usize> + ExactSizeLender,
    I2: for<'next> NodeLabelsLender<'next, Label = usize> + ExactSizeLender,
{
    loop {
        assert_eq!(iter.len(), truth_iter.len());

        let pair = iter.next();
        let tpair = truth_iter.next();
        assert_eq!(
            pair.is_some(),
            tpair.is_some(),
            "Mismatch in iterator lengths"
        );
        let Some((src, succ)) = pair else {
            break;
        };
        let (tsrc, tsucc) = tpair.unwrap();
        assert_eq!(src, tsrc);

        let succ = succ.into_iter().collect::<Vec<_>>();
        let tsucc = tsucc.into_iter().collect::<Vec<_>>();
        assert_eq!(succ, tsucc, "error at node {}", src);
    }

    // fused iterators
    for _ in 0..10 {
        assert!(iter.next().is_none(), "Iterator should be exhausted");
        assert!(
            truth_iter.next().is_none(),
            "Truth iterator should be exhausted"
        );
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.len(), truth_iter.len());
    }
}

#[test]
fn test_arc_list_graph_cnr2000() {
    let graph = BvGraph::with_basename("../data/cnr-2000")
        .endianness::<BE>()
        .load()
        .unwrap();

    let mut arcs = vec![];
    for_!((src, succs) in graph.iter() {
      for_!(succ in succs {
        arcs.push((src, succ));
      });
    });
    assert_eq!(arcs.len(), graph.num_arcs() as _);

    let arc_graph =
        webgraph::graphs::arc_list_graph::ArcListGraph::new(graph.num_nodes(), arcs.into_iter());

    assert_eq!(arc_graph.num_nodes(), graph.num_nodes());
    test_graph_iters(arc_graph.iter(), graph.iter());

    for n in 1..=11 {
        let iters: Vec<_> = arc_graph.split_iter(n).collect();
        let truth_iters: Vec<_> = graph.split_iter(n).collect();

        assert_eq!(truth_iters.len(), n, "Expected {} iterators", n);
        assert_eq!(
            iters.len(),
            truth_iters.len(),
            "Mismatch in split iterators length"
        );

        for ((start1, iter), (start2, titer)) in iters.into_iter().zip(truth_iters) {
            assert_eq!(start1, start2, "Mismatch in split start nodes");
            assert_eq!(iter.len(), titer.len(), "Mismatch in iterator lengths");
            test_graph_iters(iter, titer);
        }
    }
}

#[test]
fn test_arc_list_graph() -> anyhow::Result<()> {
    let arcs = [
        (0, 1, Some(1.0)),
        (0, 2, None),
        (1, 2, Some(2.0)),
        (2, 4, Some(f64::INFINITY)),
        (3, 4, Some(f64::NEG_INFINITY)),
    ];
    let g = LabeledBTreeGraph::<_>::from_arcs(arcs);
    let coo = ArcListGraph::new_labeled(g.num_nodes(), arcs.iter().copied());
    let g2 = LabeledBTreeGraph::<_>::from_lender(coo.iter());

    graph::eq_labeled(&g, &g2)?;

    let g = LabeledVecGraph::<_>::from_arcs(arcs);
    let coo = ArcListGraph::new_labeled(g.num_nodes(), arcs.iter().copied());
    let g2 = LabeledBTreeGraph::<_>::from_lender(coo.iter());

    graph::eq_labeled(&g, &g2)?;

    Ok(())
}

#[test]
fn test_split_iters_from_with_empty_end_nodes() -> anyhow::Result<()> {
    use webgraph::graphs::arc_list_graph::{self, SplitIters};

    // Create a graph with 10 nodes where the last 2 nodes have no outgoing arcs
    // Nodes 0-7 have arcs, nodes 8-9 have no arcs
    let num_nodes = 10;
    let arcs = vec![
        (0, 1, ()),
        (0, 2, ()),
        (1, 3, ()),
        (2, 4, ()),
        (2, 5, ()),
        (3, 6, ()),
        (5, 7, ()),
        (6, 7, ()),
        (7, 1, ()),
        // nodes 8 and 9 have no outgoing arcs
    ];

    // Split into 3 partitions: [0-3], [4-6], [7-9]
    // The last partition [7-9] should include nodes 8 and 9 even though they have no arcs
    let partition_boundaries: Box<[usize]> = vec![0, 4, 7, 10].into_boxed_slice();
    let num_partitions = partition_boundaries.len() - 1;

    // Create partitioned pairs (simulating what ParSortPairs would return)
    let mut partitioned_iters: Vec<Vec<(usize, usize, ())>> = Vec::new();

    for i in 0..num_partitions {
        let start = partition_boundaries[i];
        let end = partition_boundaries[i + 1];
        let partition_arcs: Vec<_> = arcs
            .iter()
            .filter(|(src, _, _)| *src >= start && *src < end)
            .copied()
            .collect();

        partitioned_iters.push(partition_arcs);
    }

    // Convert to lenders using the From trait via SplitIters
    let split_iters = SplitIters::new(partition_boundaries, partitioned_iters.into_boxed_slice());
    let (boundaries, lenders): (Box<[usize]>, Box<[arc_list_graph::Iter<(), _>]>) =
        split_iters.into();

    // Verify we got the right number of lenders
    assert_eq!(
        lenders.len(),
        num_partitions,
        "Should have {} lenders",
        num_partitions
    );
    assert_eq!(
        boundaries.len(),
        num_partitions + 1,
        "Should have {} boundaries",
        num_partitions + 1
    );

    // Collect all nodes from all lenders
    let mut all_nodes = Vec::new();
    for mut lender in lenders.into_vec() {
        while let Some((node_id, successors)) = lender.next() {
            all_nodes.push(node_id);
            let _succs: Vec<_> = successors.into_iter().collect();
        }
    }

    // Verify we enumerated ALL nodes 0..9, including the last two without arcs
    assert_eq!(
        all_nodes.len(),
        num_nodes,
        "Should enumerate all {} nodes",
        num_nodes
    );
    assert_eq!(
        all_nodes,
        (0..num_nodes).collect::<Vec<_>>(),
        "Should enumerate nodes 0..{} in order",
        num_nodes - 1
    );

    // Specifically verify nodes 8 and 9 are included
    assert!(all_nodes.contains(&8), "Node 8 should be enumerated");
    assert!(all_nodes.contains(&9), "Node 9 should be enumerated");

    Ok(())
}
