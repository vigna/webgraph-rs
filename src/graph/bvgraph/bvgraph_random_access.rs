use sux::traits::{IndexedDict, MemCase};

use super::*;
use crate::utils::nat2int;

/// BVGraph is an highly compressed graph format that can be traversed
/// sequentially or randomly without having to decode the whole graph.
pub struct BVGraph<CRB: BVGraphCodesReaderBuilder, OFF: IndexedDict<Value = u64>> {
    /// Backend that can create objects that allows us to read the bitstream of
    /// the graph to decode the edges.
    codes_reader_builder: CRB,
    /// The bit offset at which we will have to start for decoding the edges of
    /// each node.
    offsets: MemCase<OFF>,
    /// The minimum size of the intervals we are going to decode.
    min_interval_length: usize,
    /// The maximum distance between two nodes that reference each other.
    compression_window: usize,
    /// The number of nodes in the graph.
    number_of_nodes: usize,
    /// The number of arcs in the graph.
    number_of_arcs: usize,
}

impl<CRB, OFF> BVGraph<CRB, OFF>
where
    CRB: BVGraphCodesReaderBuilder,
    OFF: IndexedDict<Value = u64>,
{
    /// Create a new BVGraph from the given parameters.
    ///
    /// # Arguments
    /// - `codes_reader_builder`: backend that can create objects that allows
    /// us to read the bitstream of the graph to decode the edges.
    /// - `offsets`: the bit offset at which we will have to start for decoding
    /// the edges of each node. (This is needed for the random accesses,
    /// [`BVGraphSequential`] does not need them)
    /// - `min_interval_length`: the minimum size of the intervals we are going
    /// to decode.
    /// - `compression_window`: the maximum distance between two nodes that
    /// reference each other.
    /// - `number_of_nodes`: the number of nodes in the graph.
    /// - `number_of_arcs`: the number of arcs in the graph.
    ///
    pub fn new(
        codes_reader_builder: CRB,
        offsets: MemCase<OFF>,
        min_interval_length: usize,
        compression_window: usize,
        number_of_nodes: usize,
        number_of_arcs: usize,
    ) -> Self {
        Self {
            codes_reader_builder,
            offsets,
            min_interval_length,
            compression_window,
            number_of_nodes,
            number_of_arcs,
        }
    }

    #[inline(always)]
    /// Change the codes reader builder (monad style)
    pub fn map_codes_reader_builder<CRB2, F>(self, map_func: F) -> BVGraph<CRB2, OFF>
    where
        F: FnOnce(CRB) -> CRB2,
        CRB2: BVGraphCodesReaderBuilder,
    {
        BVGraph {
            codes_reader_builder: map_func(self.codes_reader_builder),
            offsets: self.offsets,
            number_of_nodes: self.number_of_nodes,
            number_of_arcs: self.number_of_arcs,
            compression_window: self.compression_window,
            min_interval_length: self.min_interval_length,
        }
    }

    #[inline(always)]
    /// Change the offsets (monad style)
    pub fn map_offsets<OFF2, F>(self, map_func: F) -> BVGraph<CRB, OFF2>
    where
        F: FnOnce(MemCase<OFF>) -> MemCase<OFF2>,
        OFF2: IndexedDict<Value = u64>,
    {
        BVGraph {
            codes_reader_builder: self.codes_reader_builder,
            offsets: map_func(self.offsets),
            number_of_nodes: self.number_of_nodes,
            number_of_arcs: self.number_of_arcs,
            compression_window: self.compression_window,
            min_interval_length: self.min_interval_length,
        }
    }

    #[inline(always)]
    /// Consume self and return the codes reader builder and the offsets
    pub fn unwrap(self) -> (CRB, MemCase<OFF>) {
        (self.codes_reader_builder, self.offsets)
    }
}

impl<CRB, OFF> SequentialGraph for BVGraph<CRB, OFF>
where
    CRB: BVGraphCodesReaderBuilder,
    OFF: IndexedDict<Value = u64>,
{
    type NodesIter<'b> = WebgraphSequentialIter<CRB::Reader<'b>>
        where Self: 'b, CRB: 'b,
        OFF: 'b;
    type SequentialSuccessorIter<'b> = std::vec::IntoIter<usize>
        where Self: 'b, CRB: 'b,
        OFF: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        Some(self.number_of_arcs)
    }

    /// Return a fast sequential iterator over the nodes of the graph and their successors.
    fn iter_nodes(&self) -> WebgraphSequentialIter<CRB::Reader<'_>> {
        WebgraphSequentialIter::new(
            // a reader at offset 0 should always be buildable
            self.codes_reader_builder.get_reader(0).unwrap(),
            self.compression_window,
            self.min_interval_length,
            self.number_of_nodes,
        )
    }
}

impl<CRB, OFF> RandomAccessGraph for BVGraph<CRB, OFF>
where
    CRB: BVGraphCodesReaderBuilder,
    OFF: IndexedDict<Value = u64>,
{
    type RandomSuccessorIter<'b> = RandomSuccessorIter<CRB::Reader<'b>>
        where Self: 'b,
        CRB: 'b,
        OFF: 'b;

    fn num_arcs(&self) -> usize {
        self.number_of_arcs
    }

    /// Return the outdegree of a node.
    fn outdegree(&self, node_id: usize) -> usize {
        let mut codes_reader = self
            .codes_reader_builder
            .get_reader(self.offsets.get(node_id) as _)
            .expect("Cannot create reader");
        codes_reader.read_outdegree() as usize
    }

    #[inline(always)]
    /// Return a random access iterator over the successors of a node.
    fn successors(&self, node_id: usize) -> RandomSuccessorIter<CRB::Reader<'_>> {
        let codes_reader = self
            .codes_reader_builder
            .get_reader(self.offsets.get(node_id) as _)
            .expect("Cannot create reader");

        let mut result = RandomSuccessorIter::new(codes_reader);
        let degree = result.reader.read_outdegree() as usize;
        // no edges, we are done!
        if degree == 0 {
            return result;
        }
        result.size = degree;
        let mut nodes_left_to_decode = degree;
        // read the reference offset
        let ref_delta = if self.compression_window != 0 {
            result.reader.read_reference_offset() as usize
        } else {
            0
        };
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = node_id - ref_delta;
            // retrieve the data
            let neighbours = self.successors(reference_node_id);
            debug_assert!(neighbours.len() != 0);
            // get the info on which destinations to copy
            let number_of_blocks = result.reader.read_block_count() as usize;
            // add +1 if the number of blocks is even, so we have capacity for
            // the block that will be added in the masked iterator
            let alloc_len = 1 + number_of_blocks - (number_of_blocks & 1);
            let mut blocks = Vec::with_capacity(alloc_len);
            if number_of_blocks != 0 {
                // the first block could be zero
                blocks.push(result.reader.read_blocks() as usize);
                // while the other can't
                for _ in 1..number_of_blocks {
                    blocks.push(result.reader.read_blocks() as usize + 1);
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
            let number_of_intervals = result.reader.read_interval_count() as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                result.intervals = Vec::with_capacity(number_of_intervals + 1);
                let node_id_offset = nat2int(result.reader.read_interval_start());

                debug_assert!((node_id as i64 + node_id_offset) >= 0);
                let mut start = (node_id as i64 + node_id_offset) as usize;
                let mut delta = result.reader.read_interval_len() as usize;
                delta += self.min_interval_length;
                // save the first interval
                result.intervals.push((start, delta));
                start += delta;
                nodes_left_to_decode -= delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + result.reader.read_interval_start() as usize;
                    delta = result.reader.read_interval_len() as usize;
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
            let node_id_offset = nat2int(result.reader.read_first_residual());
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

        result
    }
}

/// The iterator returend from [`BVGraph`] that returns the successors of a
/// node in sorted order.
pub struct RandomSuccessorIter<CR: BVGraphCodesReader> {
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

impl<CR: BVGraphCodesReader> ExactSizeIterator for RandomSuccessorIter<CR> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }
}

unsafe impl<CR: BVGraphCodesReader> SortedIterator for RandomSuccessorIter<CR> {}

impl<CR: BVGraphCodesReader> RandomSuccessorIter<CR> {
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

impl<CR: BVGraphCodesReader> Iterator for RandomSuccessorIter<CR> {
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
                self.next_residual_node += 1 + self.reader.read_residual() as usize;
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

/// Allow to do `for (node, succ_iter) in &graph`
impl<'a, CRB, OFF> IntoIterator for &'a BVGraph<CRB, OFF>
where
    CRB: BVGraphCodesReaderBuilder,
    OFF: IndexedDict<Value = u64>,
{
    type IntoIter = WebgraphSequentialIter<CRB::Reader<'a>>;
    type Item = <WebgraphSequentialIter<CRB::Reader<'a>> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_nodes()
    }
}
