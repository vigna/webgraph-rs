use super::{MergedGraph, SortPairs};
use crate::traits::SequentialGraph;
use anyhow::Result;
use core::marker::PhantomData;

/// A struct that takes the edges of a graph and sorts them by source node.
/// This is just a convenience wrapper over [`SortPairs`]
pub struct Sorted {
    num_nodes: usize,
    sort_pairs: SortPairs<()>,
}

impl Sorted {
    /// Create a new `Sorted` struct.
    pub fn new(num_nodes: usize, batch_size: usize) -> anyhow::Result<Self> {
        Ok(Sorted {
            num_nodes,
            sort_pairs: SortPairs::new(batch_size)?,
        })
    }

    /// Add an edge to the graph.
    pub fn push(&mut self, x: usize, y: usize) -> Result<()> {
        self.sort_pairs.push(x, y, ())
    }

    /// Add a batch of edges to the graph.
    pub fn extend<I: Iterator<Item = (usize, J)>, J: Iterator<Item = usize>>(
        &mut self,
        iter_nodes: I,
    ) -> Result<()> {
        for (x, succ) in iter_nodes {
            for s in succ {
                self.push(x, s)?;
            }
        }
        Ok(())
    }

    /// Build the graph into a readable sequential graph.
    pub fn build(mut self) -> Result<MergedGraph<()>> {
        self.sort_pairs.finish()?;
        Ok(MergedGraph {
            num_nodes: self.num_nodes,
            sorted_pairs: self.sort_pairs,
            marker: PhantomData,
        })
    }
}

#[test]
fn test_sorted_permuted_graph() -> Result<()> {
    use crate::bvgraph::VecGraph;
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let mut s = Sorted::new(g.num_nodes(), 1)?;
    s.extend(g.iter_nodes())?;
    let m = s.build()?;
    let h = VecGraph::from_node_iter(m.iter_nodes());
    assert_eq!(g, h);

    for batch_size in vec![1, 10, 100] {
        let mut s = Sorted::new(4, batch_size)?;
        for _ in 0..100 {
            s.push(1, 2)?;
            s.push(2, 2)?;
            s.push(2, 1)?;
            s.push(1, 1)?;
        }

        let m = s.build()?;
        let mut g = VecGraph::empty(4);
        g.add_arc_list(&[(1, 1), (1, 2), (2, 2), (2, 1)]);
        let h = VecGraph::from_node_iter(m.iter_nodes());
        assert_eq!(g, h);
    }

    Ok(())
}
