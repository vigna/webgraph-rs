use anyhow::Result;

/// Traits of the operations we can do on a graph
pub trait Graph {
    type NeighboursIter<'a>: Iterator<Item = u64> + 'a
    where
        Self: 'a;

    /// Get a sorted iterator over the neighbours node_id
    fn get_neighbours(&self, node_id: u64) -> Result<Self::NeighboursIter<'_>>;

    /// Get the number of outgoing edges of a node
    fn get_degree(&self, node_id: u64) -> Result<usize> {
        Ok(self.get_neighbours(node_id)?.count())
    }

    /// Return if the given edge `src_node_id -> dst_node_id` exists or not
    fn has_edge(&self, src_node_id: u64, dst_node_id: u64) -> Result<bool> {
        for neighbour_id in self.get_neighbours(src_node_id)? {
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
