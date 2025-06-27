use dsi_bitstream::traits::BE;
use lender::prelude::*;

use webgraph::{
    graphs::arc_list_graph::Iter,
    labels::Left,
    prelude::BvGraph,
    traits::{RandomAccessLabeling, SequentialLabeling},
};

#[test]
fn test_arclist_graph_iter() {
    let iter = Iter::<Box<u64>, Vec<_>>::new(10, vec![].into_iter());
    for_!((_succ, labels) in iter {
        for_!(item in labels {
          println!("{:?}", item);
        });
    });
}

#[test]
fn test_arclist_graph_cnr2000() {
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
    let arcgraph = Left(arcgraph);

    assert_eq!(arcgraph.num_nodes(), graph.num_nodes());

    let mut iter = arcgraph.iter();
    let mut truth_iter = graph.iter();

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
        assert_eq!(iter.len(), truth_iter.len());
    }
}
