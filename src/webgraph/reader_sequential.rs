use super::*;
use crate::utils::nat2int;
use core::iter::Peekable;

pub struct WebgraphReaderSequential<'a, CR: WebGraphCodesReader> {
    codes_reader: &'a mut CR,
    backrefs: CircularBuffer,
    min_interval_length: usize,
}

impl<'a, CR: WebGraphCodesReader> WebgraphReaderSequential<'a, CR> {
    pub fn new(
        codes_reader: &'a mut CR,
        min_interval_length: usize,
        compression_window: usize,
    ) -> Self {
        Self {
            codes_reader,
            backrefs: CircularBuffer::new(compression_window + 1),
            min_interval_length,
        }
    }

    pub fn get_successors_iter(&mut self, node_id: u64) -> Result<&[u64]> {
        let mut res = self.backrefs.take();
        for node in self.get_successors_iter_priv(node_id)? {
            res.push(node);
        }
        Ok(self.backrefs.push(res))
    }

    #[inline(always)]
    fn get_successors_iter_priv(&mut self, node_id: u64) -> Result<SuccessorsIterSequential> {
        let mut result = SuccessorsIterSequential::default();
        let degree = self.codes_reader.read_outdegree()? as usize;
        // no edges, we are done!
        if degree == 0 {
            return Ok(result);
        }
        result.size = degree;
        let mut nodes_left_to_decode = degree;
        // read the reference offset
        let ref_delta = self.codes_reader.read_reference_offset()?;
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = node_id - ref_delta;
            // retrieve the data
            let neighbours = self.backrefs.get(reference_node_id);
            debug_assert!(neighbours.len() != 0);
            // get the info on which destinations to copy
            let number_of_blocks = self.codes_reader.read_block_count()? as usize;
            let mut blocks = Vec::with_capacity(number_of_blocks as usize);
            if number_of_blocks != 0 {
                // the first block could be zero
                blocks.push(self.codes_reader.read_blocks()? as usize);
                // while the other can't
                for _ in 1..number_of_blocks {
                    let block = self.codes_reader.read_blocks()? as usize;
                    blocks.push(block + 1);
                }
            }
            // create the masked iterator
            let res = MaskedIterator::new(neighbours.iter().copied(), blocks);
            nodes_left_to_decode -= res.len();

            result.copied_nodes_iter = Some(res.peekable());
        };

        // if we still have to read nodes
        if nodes_left_to_decode != 0 {
            // read the number of intervals
            let number_of_intervals = self.codes_reader.read_interval_count()? as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                result.intervals = Vec::with_capacity(number_of_intervals);
                let node_id_offset = nat2int(self.codes_reader.read_interval_start()?);
                debug_assert!((node_id as i64 + node_id_offset) >= 0);
                let mut start = (node_id as i64 + node_id_offset) as u64;
                let mut delta = self.codes_reader.read_interval_len()? as usize;
                delta += self.min_interval_length;
                // save the first interval
                result.intervals.push((start, delta));
                start += delta as u64;
                nodes_left_to_decode -= delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + self.codes_reader.read_interval_start()?;
                    delta = self.codes_reader.read_interval_len()? as usize;
                    delta += self.min_interval_length;

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
            let node_id_offset = nat2int(self.codes_reader.read_first_residual()?);
            let mut extra = (node_id as i64 + node_id_offset) as u64;
            result.extra_nodes.push(extra);
            // decode the successive extra nodes
            for _ in 1..nodes_left_to_decode {
                extra += 1 + self.codes_reader.read_residual()?;
                result.extra_nodes.push(extra);
            }
        }

        Ok(result)
    }
}

///
pub struct SuccessorsIterSequential<'a> {
    /// The number of values left
    size: usize,
    /// Iterator over the destinations that we are going to copy
    /// from another node
    copied_nodes_iter:
        Option<Peekable<MaskedIterator<core::iter::Copied<core::slice::Iter<'a, u64>>>>>,

    /// Intervals of extra nodes
    intervals: Vec<(u64, usize)>,
    /// The index of interval to return
    intervals_idx: usize,
    /// Extra nodes
    extra_nodes: Vec<u64>,
    /// The index of extra nodes to return
    extra_nodes_idx: usize,
}

impl<'a> ExactSizeIterator for SuccessorsIterSequential<'a> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }
}

impl<'a> core::default::Default for SuccessorsIterSequential<'a> {
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

impl<'a> Iterator for SuccessorsIterSequential<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        // check if we should stop iterating
        if self.size == 0 {
            return None;
        }

        self.size -= 1;

        // Get the different nodes or usize::MAX if not present
        let copied_value = *self
            .copied_nodes_iter
            .as_mut()
            .map(|x| x.peek().unwrap_or(&u64::MAX))
            .unwrap_or(&u64::MAX);

        let extra_node = *self
            .extra_nodes
            .get(self.extra_nodes_idx)
            .unwrap_or(&u64::MAX);

        let interval_node = *{
            let (start, len) = self
                .intervals
                .get(self.intervals_idx)
                .unwrap_or(&(u64::MAX, usize::MAX));
            debug_assert_ne!(
                *len, 0,
                "there should never be an interval with length zero here"
            );
            start
        };

        debug_assert!(
            copied_value != u64::MAX || extra_node != u64::MAX || interval_node != u64::MAX,
            "At least one of the nodes must present, this should be a problem with the degree.",
        );

        // find the smallest of the values
        let min = copied_value.min(extra_node).min(interval_node);

        // depending on from where the node was, forward it
        if min == copied_value {
            self.copied_nodes_iter.as_mut().unwrap().next().unwrap();
        } else if min == extra_node {
            self.extra_nodes_idx += 1;
        } else {
            let (start, len) = &mut self.intervals[self.intervals_idx];
            debug_assert_ne!(
                *len, 0,
                "there should never be an interval with length zero here"
            );
            // if the interval has other values, just reduce the interval
            if *len > 1 {
                *len -= 1;
                *start += 1;
            } else {
                // otherwise just increase the idx to use the next interval
                self.intervals_idx += 1;
            }
        }

        Some(min)
    }
}
