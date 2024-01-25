/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;

/// Fast iterator over the offsets and degrees of a [`BVGraph`].
///
/// This iterator is faster than scanning the graph. In particular, it can be
/// used to build the offsets of a graph or to enumerate the graph degrees when
/// the offsets are not available.
pub struct OffsetDegIter<D: Decoder> {
    decoder: D,
    backrefs: Vec<usize>,
    node_id: usize,
    min_interval_length: usize,
    compression_window: usize,
    number_of_nodes: usize,
}

impl<D: Decoder + BitSeek> OffsetDegIter<D> {
    /// Get the current bit offset in the bitstream.
    pub fn get_pos(&mut self) -> u64 {
        self.decoder.get_bit_pos().unwrap()
    }
}

impl<D: Decoder + BitSeek> Iterator for OffsetDegIter<D> {
    type Item = (u64, usize);
    fn next(&mut self) -> Option<(u64, usize)> {
        if self.node_id >= self.number_of_nodes {
            return None;
        }
        let offset = self.get_pos();
        Some((offset, self.next_degree().unwrap()))
    }
}

impl<D: Decoder> OffsetDegIter<D> {
    /// Create a new iterator over the degrees of the graph.
    pub fn new(
        decoder: D,
        min_interval_length: usize,
        compression_window: usize,
        number_of_nodes: usize,
    ) -> Self {
        Self {
            decoder,
            backrefs: vec![0; compression_window + 1],
            node_id: 0,
            min_interval_length,
            compression_window,
            number_of_nodes,
        }
    }

    /// Get the number of nodes in the graph
    #[inline(always)]
    pub fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }

    #[inline(always)]
    /// Manually get the next degree, this is what the iterator calls internally
    /// but it calls `.unwrap()` on it because the trait Graph doesn't allows
    /// errors.
    pub fn next_degree(&mut self) -> Result<usize> {
        let degree = self.decoder.read_outdegree() as usize;
        // no edges, we are done!
        if degree == 0 {
            self.backrefs[self.node_id % self.compression_window] = degree;
            self.node_id += 1;
            return Ok(degree);
        }

        let mut nodes_left_to_decode = degree;

        // read the reference offset
        let ref_delta = if self.compression_window != 0 {
            self.decoder.read_reference_offset() as usize
        } else {
            0
        };
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = self.node_id - ref_delta;
            // retrieve the data
            let ref_degree = self.backrefs[reference_node_id % self.compression_window];
            // get the info on which destinations to copy
            let number_of_blocks = self.decoder.read_block_count() as usize;

            // no blocks, we copy everything
            if number_of_blocks == 0 {
                nodes_left_to_decode -= ref_degree;
            } else {
                // otherwise we copy only the blocks of even index

                // the first block could be zero
                let mut idx = self.decoder.read_block() as usize;
                nodes_left_to_decode -= idx;

                // while the other can't
                for block_id in 1..number_of_blocks {
                    let block = self.decoder.read_block() as usize;
                    let end = idx + block + 1;
                    if block_id % 2 == 0 {
                        nodes_left_to_decode -= block + 1;
                    }
                    idx = end;
                }
                if number_of_blocks & 1 == 0 {
                    nodes_left_to_decode -= ref_degree - idx;
                }
            }
        };

        // if we still have to read nodes
        if nodes_left_to_decode != 0 && self.min_interval_length != 0 {
            // read the number of intervals
            let number_of_intervals = self.decoder.read_interval_count() as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                let _ = self.decoder.read_interval_start();
                let mut delta = self.decoder.read_interval_len() as usize;
                delta += self.min_interval_length;
                // save the first interval
                nodes_left_to_decode -= delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    let _ = self.decoder.read_interval_start();
                    delta = self.decoder.read_interval_len() as usize;
                    delta += self.min_interval_length;

                    nodes_left_to_decode -= delta;
                }
            }
        }

        // decode the extra nodes if needed
        if nodes_left_to_decode != 0 {
            // pre-allocate with capacity for efficency
            let _ = self.decoder.read_first_residual();
            for _ in 1..nodes_left_to_decode {
                let _ = self.decoder.read_residual();
            }
        }

        self.backrefs[self.node_id % self.compression_window] = degree;
        self.node_id += 1;
        Ok(degree)
    }
}
