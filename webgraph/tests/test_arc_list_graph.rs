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
    let iter = Iter::<Box<u64>, Vec<_>>::new(10, vec![].into_iter());
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

    let arcgraph =
        webgraph::graphs::arc_list_graph::ArcListGraph::new(graph.num_nodes(), arcs.into_iter());

    assert_eq!(arcgraph.num_nodes(), graph.num_nodes());
    test_graph_iters(arcgraph.iter(), graph.iter());

    for n in 1..=11 {
        let iters = arcgraph.split_iter(n);
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

#[test]
fn test_arc_list_graph_skip() -> anyhow::Result<()> {
    let offset = 1_000;
    let arcs = vec![
        (offset + 0, 1),
        (offset + 0, 2),
        (offset + 1, 2),
        (offset + 2, 4),
        (offset + 3, 4),
    ];

    for iter_from in [offset - 10, offset, offset + 1, offset + 10] {
        let arcs1 = ArcListGraph::new(offset + 4, arcs.iter().copied())
            .iter()
            .skip(iter_from);
        let mut arcs2 = ArcListGraph::new(offset + 4, arcs.iter().copied()).iter_from(iter_from);
        for_!((node1, succ1) in arcs1 {
            let (node2, succ2) = arcs2.next().unwrap();
            assert_eq!(node1, node2);
            assert_eq!(succ1.into_iter().collect::<Vec<_>>(), succ2.into_iter().collect::<Vec<_>>());
        });
        assert!(arcs2.next().is_none());
    }

    Ok(())
}
