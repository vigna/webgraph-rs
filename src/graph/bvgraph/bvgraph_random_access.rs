/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::path::PathBuf;

use dsi_bitstream::traits::NE;
use lender::IntoLender;
use sux::prelude::*;

use crate::prelude::*;
use crate::utils::nat2int;

use super::code_reader_builder;

pub fn with_basename(
    basename: impl AsRef<std::path::Path>,
) -> Load<NE, Random, Dynamic, Mmap, Mmap> {
    Load {
        basename: PathBuf::from(basename.as_ref()),
        graph_load_flags: code_reader_builder::Flags::empty(),
        offsets_load_flags: code_reader_builder::Flags::empty(),
        _marker: std::marker::PhantomData,
    }
}

/// BVGraph is an highly compressed graph format that can be traversed
/// sequentially or randomly without having to decode the whole graph.
pub struct BVGraph<CRB: BVGraphCodesReaderBuilder> {
    /// Backend that can create objects that allows us to read the bitstream of
    /// the graph to decode the edges.
    codes_reader_builder: CRB,
    /// The minimum size of the intervals we are going to decode.
    min_interval_length: usize,
    /// The maximum distance between two nodes that reference each other.
    compression_window: usize,
    /// The number of nodes in the graph.
    number_of_nodes: usize,
    /// The number of arcs in the graph.
    number_of_arcs: u64,
}

impl<CRB> BVGraph<CRB>
where
    CRB: BVGraphCodesReaderBuilder,
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
        min_interval_length: usize,
        compression_window: usize,
        number_of_nodes: usize,
        number_of_arcs: u64,
    ) -> Self {
        Self {
            codes_reader_builder,
            min_interval_length,
            compression_window,
            number_of_nodes,
            number_of_arcs,
        }
    }

    #[inline(always)]
    /// Change the codes reader builder (monad style)
    pub fn map_codes_reader_builder<CRB2: BVGraphCodesReaderBuilder>(
        self,
        map_func: impl FnOnce(CRB) -> CRB2,
    ) -> BVGraph<CRB2> {
        BVGraph {
            codes_reader_builder: map_func(self.codes_reader_builder),
            number_of_nodes: self.number_of_nodes,
            number_of_arcs: self.number_of_arcs,
            compression_window: self.compression_window,
            min_interval_length: self.min_interval_length,
        }
    }

    /* TODO
        #[inline(always)]
        /// Change the offsets (monad style)
        pub fn map_offsets<OFF2, F>(self, map_func: F) -> BVGraph<CRB2>
        where
            F: FnOnce(MemCase<OFF>) -> MemCase<OFF2>,
        {
            BVGraph {
                codes_reader_builder: self.codes_reader_builder,
                number_of_nodes: self.number_of_nodes,
                number_of_arcs: self.number_of_arcs,
                compression_window: self.compression_window,
                min_interval_length: self.min_interval_length,
            }
        }
    */

    /* TODO
    #[inline(always)]
    /// Consume self and return the codes reader builder and the offsets
    pub fn unwrap(self) -> (CRB, MemCase<OFF>) {
        (self.codes_reader_builder, self.offsets)
    }*/
}

impl<CRB> SequentialLabelling for BVGraph<CRB>
where
    CRB: BVGraphCodesReaderBuilder,
{
    type Label = usize;
    type Iterator<'b> = WebgraphSequentialIter<CRB::Reader<'b>>
    where
        Self: 'b,
        CRB: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(self.number_of_arcs)
    }

    /// Return a fast sequential iterator over the nodes of the graph and their successors.
    fn iter_from(&self, start_node: usize) -> Self::Iterator<'_> {
        let codes_reader = self.codes_reader_builder.get_reader(start_node).unwrap();
        // we have to pre-fill the buffer
        let mut backrefs = CircularBufferVec::new(self.compression_window + 1);

        // TODO!: this can be optimized, but usually the chunk on which we iter is
        // much bigger than the compression window so it's not urgent
        for node_id in start_node.saturating_sub(self.compression_window)..start_node {
            backrefs.push(node_id, self.successors(node_id).collect());
        }

        WebgraphSequentialIter {
            codes_reader,
            backrefs,
            compression_window: self.compression_window,
            min_interval_length: self.min_interval_length,
            number_of_nodes: self.number_of_nodes,
            current_node: start_node,
        }
    }
}

impl<CRB> SequentialGraph for BVGraph<CRB> where CRB: BVGraphCodesReaderBuilder {}

impl<CRB> RandomAccessLabelling for BVGraph<CRB>
where
    CRB: BVGraphCodesReaderBuilder,
{
    type Labels<'a> = RandomSuccessorIter<CRB::Reader<'a>>
    where Self: 'a, CRB: 'a;

    fn num_arcs(&self) -> u64 {
        self.number_of_arcs
    }

    /// Return the outdegree of a node.
    fn outdegree(&self, node_id: usize) -> usize {
        let mut codes_reader = self
            .codes_reader_builder
            .get_reader(node_id)
            .expect("Cannot create reader");
        codes_reader.read_outdegree() as usize
    }

    #[inline(always)]
    /// Return a random access iterator over the successors of a node.
    fn labels(&self, node_id: usize) -> RandomSuccessorIter<CRB::Reader<'_>> {
        let codes_reader = self
            .codes_reader_builder
            .get_reader(node_id)
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

impl<CRB> RandomAccessGraph for BVGraph<CRB> where CRB: BVGraphCodesReaderBuilder {}

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

unsafe impl<CR: BVGraphCodesReader> SortedLabels for RandomSuccessorIter<CR> {}

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

impl<'a, CRB: BVGraphCodesReaderBuilder> IntoLender for &'a BVGraph<CRB> {
    type Lender = <BVGraph<CRB> as SequentialLabelling>::Iterator<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}
