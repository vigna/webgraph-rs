use anyhow::bail;
use dsi_bitstream::prelude::*;
use std::collections::HashMap;
use sux::traits::VSlice;

use super::*;
use crate::utils::nat2int;

pub struct CompFlags {
    pub outdegrees: Code,
    pub references: Code,
    pub blocks: Code,
    pub intervals: Code,
    pub residuals: Code,
    pub min_interval_length: usize,
    pub compression_window: usize,
}

impl CompFlags {
    pub fn code_from_str(s: &str) -> Option<Code> {
        match s.to_uppercase().as_str() {
            "UNARY" => Some(Code::Unary),
            "GAMMA" => Some(Code::Gamma),
            "DELTA" => Some(Code::Delta),
            "ZETA" => Some(Code::Zeta { k: 3 }),
            "NIBBLE" => Some(Code::Nibble),
            _ => None,
        }
    }

    pub fn from_properties(map: &HashMap<String, String>) -> anyhow::Result<Self> {
        // Default values, same as the Java class
        let mut cf = CompFlags {
            outdegrees: Code::Gamma,
            references: Code::Unary,
            blocks: Code::Gamma,
            intervals: Code::Gamma,
            residuals: Code::Zeta { k: 3 },
            min_interval_length: 4,
            compression_window: 7,
        };
        if let Some(comp_flags) = map.get("compressionflags") {
            if !comp_flags.is_empty() {
                for flag in comp_flags.split('|') {
                    dbg!(&flag);
                    let s: Vec<_> = flag.split('_').collect();
                    dbg!(&s);
                    // FIXME: this is a hack to avoid having to implement
                    // FromStr for Code
                    let code = CompFlags::code_from_str(s[1]).unwrap();
                    match s[0] {
                        "OUTDEGREES" => cf.outdegrees = code,
                        "REFERENCES" => cf.references = code,
                        "BLOCKS" => cf.blocks = code,
                        "INTERVALS" => cf.intervals = code,
                        "RESIDUALS" => cf.residuals = code,
                        _ => bail!("Unknown compression flag {}", flag),
                    }
                }
            }
        }
        if let Some(k) = map.get("zeta_k") {
            if k.parse::<usize>()? != 3 {
                bail!("Only ζ₃ is supported");
            }
        }
        if let Some(compression_window) = map.get("compressionwindow") {
            cf.compression_window = compression_window.parse()?;
        }
        if let Some(min_interval_length) = map.get("minintervallength") {
            cf.min_interval_length = min_interval_length.parse()?;
        }
        Ok(cf)
    }
}

/// BVGraph is an highly compressed graph format that can be traversed
/// sequentially or randomly without having to decode the whole graph.
pub struct BVGraph<CR, OFF> {
    /// Backend that allows us to read the bitstream of the graph to decode
    /// the edges.
    codes_reader: CR,
    /// The minimum size of the intervals we are going to decode.
    min_interval_length: usize,
    /// The bit offset at which we will have to start for decoding the edges of
    /// each node.
    offsets: OFF,
    /// The maximum distance between two nodes that reference each other.
    compression_window: usize,
    /// The number of nodes in the graph.
    number_of_nodes: usize,
    /// The number of arcs in the graph.
    number_of_arcs: usize,
}

impl<CR, OFF> BVGraph<CR, OFF>
where
    CR: WebGraphCodesReader + BitSeek + Clone,
    OFF: VSlice,
{
    pub fn new(
        codes_reader: CR,
        offsets: OFF,
        min_interval_length: usize,
        compression_window: usize,
        number_of_nodes: usize,
        number_of_arcs: usize,
    ) -> Self {
        Self {
            codes_reader,
            min_interval_length,
            offsets,
            compression_window,
            number_of_nodes,
            number_of_arcs,
        }
    }
}

impl<CR, OFF> NumNodes for BVGraph<CR, OFF>
where
    CR: WebGraphCodesReader + BitSeek + Clone,
    OFF: VSlice,
{
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }
}

impl<CR, OFF> SequentialGraph for BVGraph<CR, OFF>
where
    CR: WebGraphCodesReader + BitSeek + Clone,
    OFF: VSlice,
{
    type NodesIter<'a> = WebgraphSequentialIter<CR>
        where CR: 'a,
        OFF: 'a;
    type SequentialSuccessorIter<'a> = std::vec::IntoIter<usize>
        where CR: 'a,
        OFF: 'a;

    /// Return a fast sequential iterator over the nodes of the graph and their successors.
    fn iter_nodes(&self) -> WebgraphSequentialIter<CR> {
        WebgraphSequentialIter::new(
            self.codes_reader.clone(),
            self.min_interval_length,
            self.compression_window,
            self.number_of_nodes,
        )
    }
}

impl<CR, OFF> SortedNodes for BVGraph<CR, OFF>
where
    CR: WebGraphCodesReader + BitSeek + Clone,
    OFF: VSlice,
{
}

impl<CR, OFF> RandomAccessGraph for BVGraph<CR, OFF>
where
    CR: WebGraphCodesReader + BitSeek + Clone,
    OFF: VSlice,
{
    type RandomSuccessorIter<'a> = RandomSuccessorIter<CR>
        where CR: 'a,
        OFF: 'a;

    fn num_arcs(&self) -> usize {
        self.number_of_arcs
    }

    /// Return the outdegree of a node.
    fn outdegree(&self, node_id: usize) -> Result<usize> {
        let mut codes_reader = self.codes_reader.clone();
        codes_reader.set_pos(self.offsets.get(node_id).unwrap() as _)?;
        Ok(codes_reader.read_outdegree()? as usize)
    }

    #[inline(always)]
    /// Return a random access iterator over the successors of a node.
    fn successors(&self, node_id: usize) -> Result<RandomSuccessorIter<CR>> {
        let mut codes_reader = self.codes_reader.clone();
        codes_reader.set_pos(self.offsets.get(node_id).unwrap() as _)?;

        let mut result = RandomSuccessorIter::new(codes_reader);
        let degree = result.reader.read_outdegree()? as usize;
        // no edges, we are done!
        if degree == 0 {
            return Ok(result);
        }
        result.size = degree;
        let mut nodes_left_to_decode = degree;
        // read the reference offset
        let ref_delta = if self.compression_window != 0 {
            result.reader.read_reference_offset()? as usize
        } else {
            0
        };
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = node_id - ref_delta;
            // retrieve the data
            let neighbours = self.successors(reference_node_id)?;
            debug_assert!(neighbours.len() != 0);
            // get the info on which destinations to copy
            let number_of_blocks = result.reader.read_block_count()? as usize;
            // add +1 if the number of blocks is even, so we have capacity for
            // the block that will be added in the masked iterator
            let alloc_len = 1 + number_of_blocks - (number_of_blocks & 1);
            let mut blocks = Vec::with_capacity(alloc_len);
            if number_of_blocks != 0 {
                // the first block could be zero
                blocks.push(result.reader.read_blocks()? as usize);
                // while the other can't
                for _ in 1..number_of_blocks {
                    blocks.push(result.reader.read_blocks()? as usize + 1);
                }
            }
            // create the masked iterator
            let res = MaskedIterator::new(neighbours, blocks);
            nodes_left_to_decode -= res.len();

            result.copied_nodes_iter = Some(res);
        };

        // if we still have to read nodes
        if nodes_left_to_decode != 0 && self.min_interval_length != 0 {
            // read the number of intervals
            let number_of_intervals = result.reader.read_interval_count()? as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                result.intervals = Vec::with_capacity(number_of_intervals + 1);
                let node_id_offset = nat2int(result.reader.read_interval_start()?);
                debug_assert!((node_id as i64 + node_id_offset) >= 0);
                let mut start = (node_id as i64 + node_id_offset) as usize;
                let mut delta = result.reader.read_interval_len()? as usize;
                delta += self.min_interval_length;
                // save the first interval
                result.intervals.push((start, delta));
                start += delta;
                nodes_left_to_decode -= delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + result.reader.read_interval_start()? as usize;
                    delta = result.reader.read_interval_len()? as usize;
                    delta += self.min_interval_length;

                    result.intervals.push((start, delta));
                    start += delta;
                    nodes_left_to_decode -= delta;
                }
                // fake final interval to avoid checks in the implementation of
                // `next`
                result.intervals.push((usize::MAX - 1, 1));
            }
        }

        // decode just the first extra, if present (the others will be decoded on demand)
        if nodes_left_to_decode != 0 {
            let node_id_offset = nat2int(result.reader.read_first_residual()?);
            result.next_residual_node = (node_id as i64 + node_id_offset) as usize;
            result.residuals_to_go = nodes_left_to_decode - 1;
        }

        // setup the first interval node so we can decode without branches
        if !result.intervals.is_empty() {
            let (start, len) = &mut result.intervals[0];
            *len -= 1;
            result.next_interval_node = *start;
            *start += 1;
            result.intervals_idx += (*len == 0) as usize;
        };

        // cache the first copied node so we don't have to check if the iter
        // ended at every call of `next`
        result.next_copied_node = result
            .copied_nodes_iter
            .as_mut()
            .and_then(|iter| iter.next())
            .unwrap_or(usize::MAX);

        Ok(result)
    }
}

impl<CR, OFF> SortedSuccessors for BVGraph<CR, OFF>
where
    CR: WebGraphCodesReader + BitSeek + Clone,
    OFF: VSlice,
{
}

///
pub struct RandomSuccessorIter<CR: WebGraphCodesReader + BitSeek + Clone> {
    reader: CR,
    /// The number of values left
    size: usize,
    /// Iterator over the destinations that we are going to copy
    /// from another node
    copied_nodes_iter: Option<MaskedIterator<RandomSuccessorIter<CR>>>,

    /// Intervals of extra nodes
    intervals: Vec<(usize, usize)>,
    /// The index of interval to return
    intervals_idx: usize,
    /// Remaining residual nodes
    residuals_to_go: usize,
    /// The next residual node
    next_residual_node: usize,
    /// The next residual node
    next_copied_node: usize,
    /// The next interval node
    next_interval_node: usize,
}

impl<CR: WebGraphCodesReader + BitSeek + Clone> ExactSizeIterator for RandomSuccessorIter<CR> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }
}

impl<CR: WebGraphCodesReader + BitSeek + Clone> RandomSuccessorIter<CR> {
    /// Create an empty iterator
    fn new(reader: CR) -> Self {
        Self {
            reader,
            size: 0,
            copied_nodes_iter: None,
            intervals: vec![],
            intervals_idx: 0,
            residuals_to_go: 0,
            next_residual_node: usize::MAX,
            next_copied_node: usize::MAX,
            next_interval_node: usize::MAX,
        }
    }
}

impl<CR: WebGraphCodesReader + BitSeek + Clone> Iterator for RandomSuccessorIter<CR> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        // check if we should stop iterating
        if self.size == 0 {
            return None;
        }

        self.size -= 1;
        debug_assert!(
            self.next_copied_node != usize::MAX
                || self.next_residual_node != usize::MAX
                || self.next_interval_node != usize::MAX,
            "At least one of the nodes must present, this should be a problem with the degree.",
        );

        // find the smallest of the values
        let min = self.next_residual_node.min(self.next_interval_node);

        // depending on from where the node was, forward it
        if min >= self.next_copied_node {
            let res = self.next_copied_node;
            self.next_copied_node = self
                .copied_nodes_iter
                .as_mut()
                .and_then(|iter| iter.next())
                .unwrap_or(usize::MAX);
            return Some(res);
        } else if min == self.next_residual_node {
            if self.residuals_to_go == 0 {
                self.next_residual_node = usize::MAX;
            } else {
                self.residuals_to_go -= 1;
                // NOTE: here we cannot propagate the error
                self.next_residual_node += 1 + self
                    .reader
                    .read_residual()
                    .expect("Error while reading a residual")
                    as usize;
            }
        } else {
            let (start, len) = &mut self.intervals[self.intervals_idx];
            debug_assert_ne!(
                *len, 0,
                "there should never be an interval with length zero here"
            );
            // if the interval has other values, just reduce the interval
            *len -= 1;
            self.next_interval_node = *start;
            *start += 1;
            self.intervals_idx += (*len == 0) as usize;
        }

        Some(min)
    }
}
