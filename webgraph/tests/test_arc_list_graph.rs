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
        let iters = arc_graph.split_iter(n);
        let truth_iters = graph.split_iter(n);

        assert_eq!(truth_iters.len(), n, "Expected {} iterators", n);
        assert_eq!(
            iters.len(),
            truth_iters.len(),
            "Mismatch in split iterators length"
        );

        for (iter, titer) in iters.zip(truth_iters) {
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
