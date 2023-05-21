use anyhow::Result;

// A graph that can be accessed sequentially
pub trait SequentialGraph {
    type NodesIter<'a>: Iterator<Item = (usize, Self::SequentialSuccessorIter<'a>)> + 'a
    where
        Self: 'a;
    type SequentialSuccessorIter<'a>: Iterator<Item = usize> + 'a
    where
        Self: 'a;
    fn num_nodes(&self) -> usize;

    fn iter_nodes(&self) -> Self::NodesIter<'_>;
}

// A graph that can be accessed randomly
pub trait RandomAccessGraph {
    type RandomSuccessorIter<'a>: Iterator<Item = usize> + 'a
    where
        Self: 'a;

    fn num_nodes(&self) -> usize;

    fn num_arcs(&self) -> usize;

    /// Get a sorted iterator over the neighbours node_id
    fn successors(&self, node_id: usize) -> Result<Self::RandomSuccessorIter<'_>>;

    /// Get the number of outgoing edges of a node
    fn outdegree(&self, node_id: usize) -> Result<usize> {
        Ok(self.successors(node_id)?.count())
    }

    /// Return if the given edge `src_node_id -> dst_node_id` exists or not
    fn arc(&self, src_node_id: usize, dst_node_id: usize) -> Result<bool> {
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

// Marker trait for sequential graphs that enumerate nodes in increasing order
pub trait SortedNodes {}

// Marker trait for graphs that enumerate nodes in increasing order
pub trait SortedSuccessors {}

// A graph that can be accessed both sequentially and randomly,
// and which enumerates nodes and successors in increasing order.
pub trait Graph: SequentialGraph + RandomAccessGraph + SortedNodes + SortedSuccessors {}
