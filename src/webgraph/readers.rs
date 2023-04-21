use super::*;

pub trait WebgraphBackref {
    type NeighboursIter<'a>: Iterator<Item=u64> + ExactSizeIterator + 'a 
    where
        Self: 'a;
    
    fn get_degree(&self, node_id: u64) -> Result<u64>;

    fn get_backref(&self, node_id: u64) -> Result<Self::NeighboursIter<'_>>;
}

pub struct WebgraphReaderSequential<'a, CR: WebGraphCodesReader> {
    codes_reader: &'a mut CR,
    backrefs: CricularBuffer,
    current_node_id: u64,
    min_interval_length: usize,
}

impl<'a, CR: WebGraphCodesReader> WebgraphReaderSequential<'a, CR> {
    pub fn new(codes_reader: &'a mut CR, min_interval_length: usize, compression_windows: usize) -> Self {
        Self {
            codes_reader,
            min_interval_length,
            backrefs: CricularBuffer::new(compression_windows),
            current_node_id: 0,
        }
    }

    pub fn get_neighbours(&mut self, node_id: u64) -> Result<&[u64]> {
        let mut res = self.backrefs.take();
        let iter = decode_node(node_id, self.codes_reader, &mut self.backrefs, self.min_interval_length)?;
        for node in iter {
            res.push(node);
        }
        Ok(self.backrefs.push(res))
    }
}

impl WebgraphBackref for CricularBuffer {
    type NeighboursIter<'b> = core::iter::Copied<core::slice::Iter<'b, u64>>
    where
        Self: 'b;

    fn get_degree(&self, node_id: u64) -> Result<u64> {
        Ok(self.get(node_id).len() as u64)
    }
    fn get_backref(&self, node_id: u64) -> Result<Self::NeighboursIter<'_>> {
        Ok(self.get(node_id).iter().copied())
    }
}

/*
pub struct WebgraphReaderRandomAccess<'a, CR: WebGraphCodesReader + BitSeek> {
    codes_reader: &'a mut CR,
    min_interval_length: usize,
}

impl<'a, CR: WebGraphCodesReader + BitSeek> WebgraphBackref for 
    WebgraphReaderRandomAccess<'a, CR> {
    type NeighboursIter<'b> = WebGraphLazyIter<MaskedIterator<Self::NeighboursIter<'b>>>
    where
        Self: 'b;

    fn get_degree(&self, node_id: u64) -> Result<u64> {
        Ok(self.backrefs.get(node_id).len() as u64)
    }
    fn get_backref(&self, node_id: u64) -> Result<Self::NeighboursIter<'_>> {
        Ok(self.backrefs.get(node_id).iter().copied())
    }
} */