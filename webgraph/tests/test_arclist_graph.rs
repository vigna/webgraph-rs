use lender::prelude::*;

use webgraph::graphs::arc_list_graph::Iter;

#[test]
fn test_arclist_graph_iter() {
    let iter = Iter::<Box<u64>, Vec<_>>::new(10, vec![].into_iter());
    for_!((_succ, labels) in iter {
        for_!(item in labels {
          println!("{:?}", item);
        });
    });
}
