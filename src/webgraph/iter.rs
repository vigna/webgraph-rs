use super::*;
use core::iter::Peekable;
use crate::utils::nat2int;

/// An iterator that filters out blocks of values
pub struct MaskedIterator<I> {
    /// The resolved reference node, if present
    parent: Box<I>,
    /// The copy blocks from the ref node
    blocks: Vec<usize>,
    /// The id of block to parse
    block_idx: usize,
    /// Caching of the number of values returned, if needed
    size: usize,
}

impl<I: Iterator<Item=u64> + ExactSizeIterator> MaskedIterator<I> {
    pub fn new(parent: I, mut blocks: Vec<usize>) 
        -> Self {
        // the number of copied nodes
        let mut size: usize = 0;
        // the cumulative sum of the blocks
        let mut cumsum_blocks: usize = 0;
        // compute them
        for (i, x) in blocks.iter().enumerate(){
            // branchless add
            size += if i % 2 == 0{
                *x
            } else {
                0
            };
            cumsum_blocks += x;
        }

        // an empty blocks means that we should take all the neighbours
        let remainder = parent.len() - cumsum_blocks;
    
        // check if the last block is a copy or skip block
        if remainder != 0 && blocks.len() % 2 == 0 {
            size += remainder;
            blocks.push(remainder);
        }

        Self {
            parent: Box::new(parent),
            blocks,
            block_idx: 0,
            size,
        }
    }
}

impl<I: Iterator<Item=u64>> Iterator for MaskedIterator<I> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<I: Iterator<Item=u64>> ExactSizeIterator for MaskedIterator<I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }
}

pub struct WebGraphLazyIter<I: Iterator<Item=u64> + ExactSizeIterator> {
    /// The number of values left
    size: usize,
    /// Iterator over the destinations that we are going to copy
    /// from another node
    copied_nodes_iter: Option<Peekable<MaskedIterator<I>>>,

    /// Intervals of extra nodes
    intervals: Vec<(u64, usize)>,
    /// The index of interval to return
    intervals_idx: usize,
    /// Extra nodes
    extra_nodes: Vec<u64>,
    /// The index of extra nodes to return
    extra_nodes_idx: usize,
}

impl<I: Iterator<Item=u64> + ExactSizeIterator> core::default::Default for WebGraphLazyIter<I> {
    /// Create an empty iterator
    fn default() -> Self {
        Self {
            size: 0,
            copied_nodes_iter: None,
            intervals: vec![],
            intervals_idx: 0,
            extra_nodes: vec![],
            extra_nodes_idx: 0,
        }
    }
}

impl<I: Iterator<Item=u64> + ExactSizeIterator> WebGraphLazyIter<I> {
    pub fn new<CR: WebGraphCodesReader, BR: WebgraphBackref>(
        node_id: u64, codes_reader: &mut CR, backrefs: &mut BR, min_interval_length: usize,
    ) -> Result<Self> {
        let mut result = Self::default();
        let degree = codes_reader.read_outdegree()? as usize;
        // no edges, we are done!
        if degree == 0 {
            return Ok(result);
        }
        result.size = degree;
        let mut nodes_left_to_decode = degree; 

        // read the reference offset
        let ref_delta = codes_reader.read_reference_offset()?;
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = node_id - ref_delta;
            // retrieve the data
            let neighbours = backrefs.get_backref(reference_node_id)?;
            debug_assert!(neighbours.len() != 0);
            // get the info on which destinations to copy
            let number_of_blocks = codes_reader.read_block_count()? as usize;
            let mut blocks;
            if number_of_blocks == 0 {
                // if the numebr of blocks is zero, then we need to copy all
                // the destinations!
                blocks = vec![neighbours.len()];
            } else {
                blocks = Vec::with_capacity(number_of_blocks as usize);
                // the first block could be zero
                blocks.push(codes_reader.read_blocks()? as usize);
                // while the other can't
                for _ in 1..number_of_blocks {
                    let block = codes_reader.read_blocks()? as usize;
                    blocks.push(block + 1);
                }
            }
            // create the masked iterator
            let res = MaskedIterator::new(
                neighbours, 
                blocks,
            );
            nodes_left_to_decode -= res.len();
            
            result.copied_nodes_iter = Some(res.peekable());
        };

        // if we still have to read nodes
        if nodes_left_to_decode != 0 {
            // read the number of intervals
            let number_of_intervals = codes_reader.read_interval_count()? as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                result.intervals = Vec::with_capacity(number_of_intervals);
                let node_id_offset = nat2int(codes_reader.read_interval_start()?);
                debug_assert!((node_id as i64 + node_id_offset) >= 0);
                let mut start = (node_id as i64 + node_id_offset) as u64;
                let mut delta = min_interval_length + codes_reader.read_interval_len()? as usize;
                // save the first interval
                result.intervals.push((start, delta));
                start += delta as u64;
                nodes_left_to_decode -= delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + codes_reader.read_interval_start()?;
                    delta += 1 + codes_reader.read_interval_len()? as usize
                        + min_interval_length;
                    
                    result.intervals.push((start, delta));
                    start += delta as u64;
                    nodes_left_to_decode -= delta;
                }
            }
        }

        // decode the extra nodes if needed
        if nodes_left_to_decode != 0 {
            // pre-allocate with capacity for efficency
            result.extra_nodes = Vec::with_capacity(nodes_left_to_decode);
            let node_id_offset = nat2int(codes_reader.read_first_residual()?);
            let mut extra = (node_id as i64 + node_id_offset) as u64;
            result.extra_nodes.push(extra);
            // decode the successive extra nodes
            for _ in 1..nodes_left_to_decode {
                extra += 1 + codes_reader.read_residual()?;
                result.extra_nodes.push(extra);
            }
        }

        Ok(result)
    }
}

impl<I: Iterator<Item=u64> + ExactSizeIterator> Iterator for WebGraphLazyIter<I> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<I: Iterator<Item=u64> + ExactSizeIterator> ExactSizeIterator for WebGraphLazyIter<I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }
}
