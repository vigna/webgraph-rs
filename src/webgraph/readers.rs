use super::*;

pub trait WebgraphBackref {
    type NeighboursIter<'a>: Iterator<Item=u64> + ExactSizeIterator + 'a 
    where
        Self: 'a;
    
    fn get_degree(&self, node_id: u64) -> Result<u64>;

    fn get_backref(&self, node_id: u64) -> Result<Self::NeighboursIter<'_>>;
}

/* 
pub struct WebgraphReaderSequential<'a, CR: WebGraphCodesReader> {
    codes_reader: &'a mut CR,
    backrefs: CricularBuffer,
    current_node_id: u64,
    min_interval_length: usize,
}

impl<'a, CR: WebGraphCodesReader> WebgraphBackref<CR> for 
    WebgraphReaderSequential<'a, CR> {
    fn get_degree(&self, node_id: u64, _: &mut CR) -> Result<u64> {
        Ok(self.backrefs.get(node_id).len() as u64)
    }
    fn get_backref(&self, node_id: u64, _: &mut CR) -> Result<Self::NeighboursIter<'_>> {
        Ok(self.backrefs.get(node_id).iter())
    }
}

pub struct WebgraphReaderRandomAccess<'a, CR: WebGraphCodesReader + BitSeek> {
    codes_reader: &'a mut CR,
    min_interval_length: usize,
}
*/