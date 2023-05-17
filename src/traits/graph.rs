use anyhow::Result;

/// Traits of the operations we can do on a graph
pub trait Graph {
    type NodesIter<'a>: Iterator<Item = (u64, Self::SequentialSuccessorIter<'a>)> + 'a
    where
        Self: 'a;
    type SequentialSuccessorIter<'a>: Iterator<Item = u64> + 'a
    where
        Self: 'a;
    type RandomSuccessorIter<'a>: Iterator<Item = u64> + 'a
    where
        Self: 'a;

    fn num_nodes(&self) -> usize;

    fn num_arcs(&self) -> usize;

    fn iter_nodes(&self) -> Self::NodesIter<'_>;

    /// Get a sorted iterator over the neighbours node_id
    fn successors(&self, node_id: u64) -> Result<Self::RandomSuccessorIter<'_>>;

    /// Get the number of outgoing edges of a node
    fn outdegree(&self, node_id: u64) -> Result<usize> {
        Ok(self.successors(node_id)?.count())
    }

    /// Return if the given edge `src_node_id -> dst_node_id` exists or not
    fn arc(&self, src_node_id: u64, dst_node_id: u64) -> Result<bool> {
        for neighbour_id in self.successors(src_node_id)? {
            // found
            if neighbour_id == dst_node_id {
                return Ok(true);
            }
            // early stop
            if neighbour_id > dst_node_id {
                return Ok(false);
            }
        }
        Ok(false)
    }
}
