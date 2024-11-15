use std::collections::HashSet;

use anyhow::Result;

use dsi_bitstream::prelude::BE;
use webgraph::{algo::BfsOrder, graphs::bvgraph::BvGraph, labels::proj::Left};

#[test]
fn test_start() -> Result<()> {
    // 4 -> 0 -> 2
    //       `-> 3
    // 1 -> 5
    let mut graph = webgraph::graphs::vec_graph::VecGraph::new();

    for i in 0..=5 {
        graph.add_node(i);
    }
    graph.add_arc(4, 0);
    graph.add_arc(0, 2);
    graph.add_arc(0, 3);
    graph.add_arc(1, 5);

    let order: Vec<_> = BfsOrder::new(&Left(graph)).collect();

    assert_eq!(order, vec![0, 2, 3, 1, 5, 4]);

    Ok(())
}

#[test]
fn test_start_orphan() -> Result<()> {
    // 0 -> 4 -> 2
    //       `-> 3
    // 1 -> 5
    let mut graph = webgraph::graphs::vec_graph::VecGraph::new();

    for i in 0..=5 {
        graph.add_node(i);
    }
    graph.add_arc(0, 4);
    graph.add_arc(4, 2);
    graph.add_arc(4, 3);
    graph.add_arc(1, 5);

    let order: Vec<_> = BfsOrder::new(&Left(graph)).collect();

    assert_eq!(order, vec![0, 4, 2, 3, 1, 5]);

    Ok(())
}

#[test]
fn test_cnr2000() -> Result<()> {
    let graph = BvGraph::with_basename("tests/data/cnr-2000")
        .endianness::<BE>()
        .load()?;
    let seen: HashSet<usize> = HashSet::new();
    for node in BfsOrder::new(&graph) {
        assert!(!seen.contains(&node), "{} was seen twice", node);
    }
    Ok(())
}
