use super::*;
use crate::utils::nat2int;
use crate::utils::CircularBufferVec;
use anyhow::Result;
use dsi_bitstream::prelude::*;

/// A sequential BVGraph that can be read from a `codes_reader_builder`.
/// The builder is needed because we should be able to create multiple iterators
/// and this allows us to have a single place where to store the mmaped file.
pub struct BVGraphSequential<CRB: BVGraphCodesReaderBuilder> {
    codes_reader_builder: CRB,
    number_of_nodes: usize,
    number_of_arcs: Option<usize>,
    compression_window: usize,
    min_interval_length: usize,
}

impl<CRB: BVGraphCodesReaderBuilder> SequentialGraph for BVGraphSequential<CRB> {
    type NodesIter<'a> = WebgraphSequentialIter<CRB::Reader<'a>>
    where
        Self: 'a;

    type SequentialSuccessorIter<'a> = std::vec::IntoIter<usize>
    where
        Self: 'a;

    #[inline(always)]
    /// Return the number of nodes in the graph
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }

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

impl<CRB: BVGraphCodesReaderBuilder> BVGraphSequential<CRB> {
    /// Create a new sequential graph from a codes reader builder
    /// and the number of nodes.
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
        CRB2: BVGraphCodesReaderBuilder,
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

impl<CRB: BVGraphCodesReaderBuilder> BVGraphSequential<CRB>
where
    for<'a> CRB::Reader<'a>: BVGraphCodesSkipper,
{
    #[inline(always)]
    /// Create an iterator specialized in the degrees of the nodes.
    /// This is slightly faster because it can avoid decoding some of the nodes
    /// and completely skip the merging step.
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
pub struct WebgraphSequentialIter<CR: BVGraphCodesReader> {
    pub(crate) codes_reader: CR,
    pub(crate) backrefs: CircularBufferVec,
    pub(crate) compression_window: usize,
    pub(crate) min_interval_length: usize,
    pub(crate) number_of_nodes: usize,
    pub(crate) current_node: usize,
}

impl<CR: BVGraphCodesReader + BitSeek> WebgraphSequentialIter<CR> {
    #[inline(always)]
    /// Forward the call of `get_pos` to the inner `codes_reader`.
    /// This returns the current bits offset in the bitstream.
    pub fn get_pos(&self) -> usize {
        self.codes_reader.get_pos()
    }
}

impl<CR: BVGraphCodesReader> WebgraphSequentialIter<CR> {
    /// Create a new iterator from a codes reader
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
            current_node: 0,
        }
    }

    /// Get the successors of the next node in the stream
    pub fn next_successors(&mut self) -> Result<&[usize]> {
        let mut res = self.backrefs.take(self.current_node);
        self.get_successors_iter_priv(self.current_node, &mut res)?;
        let res = self.backrefs.push(self.current_node, res);
        self.current_node += 1;
        Ok(res)
    }

    #[inline(always)]
    /// Inner method called by `next_successors` and the iterator `next` method
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

impl<CR: BVGraphCodesReader> Iterator for WebgraphSequentialIter<CR> {
    type Item = (usize, std::vec::IntoIter<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_node >= self.number_of_nodes as _ {
            return None;
        }
        let mut res = self.backrefs.take(self.current_node);
        self.get_successors_iter_priv(self.current_node, &mut res)
            .unwrap();

        // this clippy suggestion is wrong, we cannot return a reference to a
        // local variable
        #[allow(clippy::unnecessary_to_owned)]
        let res = self
            .backrefs
            .push(self.current_node, res)
            .to_vec()
            .into_iter();
        let node_id = self.current_node;
        self.current_node += 1;
        Some((node_id, res))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.number_of_nodes - self.current_node;
        (len, Some(len))
    }
}

unsafe impl<CR: BVGraphCodesReader> SortedIterator for WebgraphSequentialIter<CR> {}
unsafe impl SortedIterator for std::vec::IntoIter<usize> {}

impl<CR: BVGraphCodesReader> ExactSizeIterator for WebgraphSequentialIter<CR> {}

impl<'a, CRB> IntoIterator for &'a BVGraphSequential<CRB>
where
    CRB: BVGraphCodesReaderBuilder,
{
    type IntoIter = WebgraphSequentialIter<CRB::Reader<'a>>;
    type Item = <WebgraphSequentialIter<CRB::Reader<'a>> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_nodes()
    }
}
