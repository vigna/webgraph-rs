use crate::traits::*;
use anyhow::Result;

mod circular_buffer;

mod readers;
pub use readers::*;

mod iter;
pub use iter::*;

pub trait Graph: {
    type NeighboursIter<'a>: Iterator<Item=u64> + 'a 
    where
        Self: 'a;

    fn get_neighbours(&self, node_id: u64) -> Result<Self::NeighboursIter<'_>>;

    fn get_degree(&self, node_id: u64) -> Result<usize> {
        Ok(self.get_neighbours(node_id)?.count())
    }

    fn has_edge(&self, src_node_id: u64, dst_node_id: u64) -> Result<bool> {
        for neighbour_id in self.get_neighbours(src_node_id)? {
            // found
            if neighbour_id == dst_node_id {
                return Ok(true);
            }
            // early stop
            if neighbour_id > dst_node_id {
                return Ok(false)
            }
        }
        Ok(false)
    }
}