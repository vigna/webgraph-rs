/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use bitflags::Flags;
use dsi_bitstream::traits::BE;
use lender::IntoLender;
use std::path::PathBuf;

use self::sequential::Iter;

/// BVGraph is an highly compressed graph format that can be traversed
/// sequentially or randomly without having to decode the whole graph.
#[derive(Debug, Clone)]
pub struct BVGraph<F> {
    factory: F,
    number_of_nodes: usize,
    number_of_arcs: u64,
    compression_window: usize,
    min_interval_length: usize,
}

impl BVGraph<()> {
    pub fn with_basename(
        basename: impl AsRef<std::path::Path>,
    ) -> LoadConfig<BE, Random, Dynamic, Mmap, Mmap> {
        LoadConfig {
            basename: PathBuf::from(basename.as_ref()),
            graph_load_flags: Flags::empty(),
            offsets_load_flags: Flags::empty(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F: RandomAccessDecoderFactory> SplitLabeling for BVGraph<F>
where
    for<'a> <F as RandomAccessDecoderFactory>::Decoder<'a>: Send + Sync,
{
    type SplitLender<'a> = split::ra::Lender<'a, BVGraph<F>> where Self: 'a;
    type IntoIterator<'a> = split::ra::IntoIterator<'a, BVGraph<F>> where Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::ra::Iter::new(self, how_many)
    }
}

impl<F> BVGraph<F>
where
    F: RandomAccessDecoderFactory,
{
    /// Create a new BVGraph from the given parameters.
    ///
    /// # Arguments
    /// - `reader_factory`: backend that can create objects that allows
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
        factory: F,
        number_of_nodes: usize,
        number_of_arcs: u64,
        compression_window: usize,
        min_interval_length: usize,
    ) -> Self {
        Self {
            factory,
            number_of_nodes,
            number_of_arcs,
            compression_window,
            min_interval_length,
        }
    }

    #[inline(always)]
    /// Consume self and return the factory
    pub fn into_inner(self) -> F {
        self.factory
    }
}

impl<F> SequentialLabeling for BVGraph<F>
where
    F: RandomAccessDecoderFactory,
{
    type Label = usize;
    type Lender<'b> = Iter<F::Decoder<'b>>
    where
        Self: 'b,
        F: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(self.number_of_arcs)
    }

    /// Return a fast sequential iterator over the nodes of the graph and their successors.
    fn iter_from(&self, start_node: usize) -> Self::Lender<'_> {
        let codes_reader = self.factory.new_decoder(start_node).unwrap();
        // we have to pre-fill the buffer
        let mut backrefs = CircularBuffer::new(self.compression_window + 1);

        for node_id in start_node.saturating_sub(self.compression_window)..start_node {
            backrefs.replace(node_id, self.successors(node_id).collect());
        }

        Iter {
            decoder: codes_reader,
            backrefs,
            compression_window: self.compression_window,
            min_interval_length: self.min_interval_length,
            number_of_nodes: self.number_of_nodes,
            current_node: start_node,
        }
    }
}

impl<F> SequentialGraph for BVGraph<F> where F: RandomAccessDecoderFactory {}

impl<F> RandomAccessLabeling for BVGraph<F>
where
    F: RandomAccessDecoderFactory,
{
    type Labels<'a> = Succ<F::Decoder<'a>>
    where Self: 'a, F: 'a;

    fn num_arcs(&self) -> u64 {
        self.number_of_arcs
    }

    /// Return the outdegree of a node.
    fn outdegree(&self, node_id: usize) -> usize {
        let mut codes_reader = self
            .factory
            .new_decoder(node_id)
            .expect("Cannot create reader");
        codes_reader.read_outdegree() as usize
    }

    #[inline(always)]
    /// Return a random access iterator over the successors of a node.
    fn labels(&self, node_id: usize) -> Succ<F::Decoder<'_>> {
        let codes_reader = self
            .factory
            .new_decoder(node_id)
            .expect("Cannot create reader");

        let mut result = Succ::new(codes_reader);
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
                blocks.push(result.reader.read_block() as usize);
                // while the other can't
                for _ in 1..number_of_blocks {
                    blocks.push(result.reader.read_block() as usize + 1);
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

impl<F: RandomAccessDecoderFactory> BVGraph<F>
where
    for<'a> F::Decoder<'a>: Decode,
{
    #[inline(always)]
    /// Creates an iterator specialized in the degrees of the nodes.
    /// This is slightly faster because it can avoid decoding some of the nodes
    /// and completely skip the merging step.
    pub fn offset_deg_iter(&self) -> OffsetDegIter<F::Decoder<'_>> {
        OffsetDegIter::new(
            self.factory.new_decoder(0).unwrap(),
            self.number_of_nodes,
            self.compression_window,
            self.min_interval_length,
        )
    }
}
impl<F> RandomAccessGraph for BVGraph<F> where F: RandomAccessDecoderFactory {}

/// The iterator returend from [`BVGraph`] that returns the successors of a
/// node in sorted order.
#[derive(Debug, Clone)]
pub struct Succ<D: Decode> {
    reader: D,
    /// The number of values left
    size: usize,
    /// Iterator over the destinations that we are going to copy
    /// from another node
    copied_nodes_iter: Option<MaskedIterator<Succ<D>>>,

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

impl<D: Decode> ExactSizeIterator for Succ<D> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }
}

unsafe impl<D: Decode> SortedIterator for Succ<D> {}

impl<D: Decode> Succ<D> {
    /// Create an empty iterator
    fn new(reader: D) -> Self {
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

impl<D: Decode> Iterator for Succ<D> {
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

impl<'a, F: RandomAccessDecoderFactory> IntoLender for &'a BVGraph<F> {
    type Lender = <BVGraph<F> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}
