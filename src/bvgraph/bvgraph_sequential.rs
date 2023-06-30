use super::*;
use crate::utils::nat2int;
use anyhow::Result;
use dsi_bitstream::prelude::*;

pub struct BVGraphSequential<CRB: WebGraphCodesReaderBuilder> {
    codes_reader_builder: CRB,
    number_of_nodes: usize,
    number_of_arcs: Option<usize>,
    compression_window: usize,
    min_interval_length: usize,
}

impl<CRB: WebGraphCodesReaderBuilder> SequentialGraph for BVGraphSequential<CRB> {
    type NodesIter<'a> = WebgraphSequentialIter<CRB::Reader<'a>>
    where
        Self: 'a;

    type SequentialSuccessorIter<'a> = std::vec::IntoIter<usize>
    where
        Self: 'a;

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        self.number_of_arcs
    }

    #[inline(always)]
    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        WebgraphSequentialIter::new(
            self.codes_reader_builder.get_reader(0).unwrap(),
            self.compression_window,
            self.min_interval_length,
            self.number_of_nodes,
        )
    }
}

impl<CRB: WebGraphCodesReaderBuilder> NumNodes for BVGraphSequential<CRB> {
    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }
}

impl<CRB: WebGraphCodesReaderBuilder> BVGraphSequential<CRB> {
    pub fn new(
        codes_reader_builder: CRB,
        compression_window: usize,
        min_interval_length: usize,
        number_of_nodes: usize,
        number_of_arcs: Option<usize>,
    ) -> Self {
        Self {
            codes_reader_builder,
            compression_window,
            min_interval_length,
            number_of_nodes,
            number_of_arcs,
        }
    }

    #[inline(always)]
    /// Change the codes reader builder
    pub fn map_codes_reader_builder<CRB2, F>(self, map_func: F) -> BVGraphSequential<CRB2>
    where
        F: FnOnce(CRB) -> CRB2,
        CRB2: WebGraphCodesReaderBuilder,
    {
        BVGraphSequential {
            codes_reader_builder: map_func(self.codes_reader_builder),
            number_of_nodes: self.number_of_nodes,
            number_of_arcs: self.number_of_arcs,
            compression_window: self.compression_window,
            min_interval_length: self.min_interval_length,
        }
    }

    #[inline(always)]
    /// Consume self and return the codes reader builder
    pub fn unwrap_codes_reader_builder(self) -> CRB {
        self.codes_reader_builder
    }
}

impl<CRB: WebGraphCodesReaderBuilder> BVGraphSequential<CRB>
where
    for<'a> CRB::Reader<'a>: WebGraphCodesSkipper,
{
    #[inline(always)]
    pub fn iter_degrees(&self) -> WebgraphDegreesIter<CRB::Reader<'_>> {
        WebgraphDegreesIter::new(
            self.codes_reader_builder.get_reader(0).unwrap(),
            self.min_interval_length,
            self.compression_window,
            self.number_of_nodes,
        )
    }
}

/// A fast sequential iterator over the nodes of the graph and their successors.
/// This iterator does not require to know the offsets of each node in the graph.
#[derive(Clone)]
pub struct WebgraphSequentialIter<CR: WebGraphCodesReader> {
    codes_reader: CR,
    backrefs: CircularBufferVec,
    compression_window: usize,
    min_interval_length: usize,
    number_of_nodes: usize,
}

impl<CR: WebGraphCodesReader + BitSeek> WebgraphSequentialIter<CR> {
    #[inline(always)]
    pub fn get_pos(&self) -> usize {
        self.codes_reader.get_pos()
    }
}

impl<CR: WebGraphCodesReader> WebgraphSequentialIter<CR> {
    pub fn new(
        codes_reader: CR,
        compression_window: usize,
        min_interval_length: usize,
        number_of_nodes: usize,
    ) -> Self {
        Self {
            codes_reader,
            backrefs: CircularBufferVec::new(compression_window + 1),
            compression_window,
            min_interval_length,
            number_of_nodes,
        }
    }

    /// Get the successors of the next node in the stream
    pub fn next_successors(&mut self) -> Result<&[usize]> {
        let node_id = self.backrefs.get_end_node_id();
        let mut res = self.backrefs.take();
        self.get_successors_iter_priv(node_id, &mut res)?;
        Ok(self.backrefs.push(res))
    }

    #[inline(always)]
    fn get_successors_iter_priv(&mut self, node_id: usize, results: &mut Vec<usize>) -> Result<()> {
        let degree = self.codes_reader.read_outdegree() as usize;
        // no edges, we are done!
        if degree == 0 {
            return Ok(());
        }

        // ensure that we have enough capacity in the vector for not reallocating
        results.reserve(degree.saturating_sub(results.capacity()));

        // read the reference offset
        let ref_delta = if self.compression_window != 0 {
            self.codes_reader.read_reference_offset() as usize
        } else {
            0
        };
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = node_id - ref_delta;
            // retrieve the data
            let neighbours = &self.backrefs[reference_node_id];
            //debug_assert!(!neighbours.is_empty());
            // get the info on which destinations to copy
            let number_of_blocks = self.codes_reader.read_block_count() as usize;
            // no blocks, we copy everything
            if number_of_blocks == 0 {
                results.extend_from_slice(neighbours);
            } else {
                // otherwise we copy only the blocks of even index
                // the first block could be zero
                let mut idx = self.codes_reader.read_blocks() as usize;
                results.extend_from_slice(&neighbours[..idx]);

                // while the other can't
                for block_id in 1..number_of_blocks {
                    let block = self.codes_reader.read_blocks() as usize;
                    let end = idx + block + 1;
                    if block_id % 2 == 0 {
                        results.extend_from_slice(&neighbours[idx..end]);
                    }
                    idx = end;
                }
                if number_of_blocks & 1 == 0 {
                    results.extend_from_slice(&neighbours[idx..]);
                }
            }
        };

        // if we still have to read nodes
        let nodes_left_to_decode = degree - results.len();
        if nodes_left_to_decode != 0 && self.min_interval_length != 0 {
            // read the number of intervals
            let number_of_intervals = self.codes_reader.read_interval_count() as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                let node_id_offset = nat2int(self.codes_reader.read_interval_start());
                let mut start = (node_id as i64 + node_id_offset) as usize;
                let mut delta = self.codes_reader.read_interval_len() as usize;
                delta += self.min_interval_length;
                // save the first interval
                results.extend(start..(start + delta));
                start += delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + self.codes_reader.read_interval_start() as usize;
                    delta = self.codes_reader.read_interval_len() as usize;
                    delta += self.min_interval_length;

                    results.extend(start..(start + delta));

                    start += delta;
                }
            }
        }

        // decode the extra nodes if needed
        let nodes_left_to_decode = degree - results.len();
        if nodes_left_to_decode != 0 {
            // pre-allocate with capacity for efficency
            let node_id_offset = nat2int(self.codes_reader.read_first_residual());
            let mut extra = (node_id as i64 + node_id_offset) as usize;
            results.push(extra);
            // decode the successive extra nodes
            for _ in 1..nodes_left_to_decode {
                extra += 1 + self.codes_reader.read_residual() as usize;
                results.push(extra);
            }
        }

        results.sort();
        Ok(())
    }
}

impl<CR: WebGraphCodesReader> Iterator for WebgraphSequentialIter<CR> {
    type Item = (usize, std::vec::IntoIter<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let node_id = self.backrefs.get_end_node_id();
        if node_id >= self.number_of_nodes as _ {
            return None;
        }
        let mut res = self.backrefs.take();
        self.get_successors_iter_priv(node_id, &mut res).unwrap();

        // this clippy suggestion is wrong, we cannot return a reference to a
        // local variable
        #[allow(clippy::unnecessary_to_owned)]
        Some((node_id, self.backrefs.push(res).to_vec().into_iter()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.number_of_nodes - self.backrefs.get_end_node_id();
        (len, Some(len))
    }
}

impl<CR: WebGraphCodesReader> ExactSizeIterator for WebgraphSequentialIter<CR> {}

impl<'a, CRB> IntoIterator for &'a BVGraphSequential<CRB>
where
    CRB: WebGraphCodesReaderBuilder,
{
    type IntoIter = WebgraphSequentialIter<CRB::Reader<'a>>;
    type Item = <WebgraphSequentialIter<CRB::Reader<'a>> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_nodes()
    }
}
