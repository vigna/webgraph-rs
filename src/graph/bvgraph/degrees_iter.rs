/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;

/// Fast iterator over the degrees of each node in the graph without having
/// the offsets.
/// This has limited uses, but is very fast. Most notably, this can be used to
/// build the offsets of a graph.
pub struct DegreesIter<CR: BVGraphCodesReader + BVGraphCodesSkipper> {
    codes_reader: CR,
    backrefs: Vec<usize>,
    node_id: usize,
    min_interval_length: usize,
    compression_window: usize,
    number_of_nodes: usize,
}

impl<CR: BVGraphCodesReader + BVGraphCodesSkipper + BitSeek> DegreesIter<CR> {
    /// Get the current bit-offset in the bitstream
    pub fn get_pos(&self) -> usize {
        self.codes_reader.get_pos()
    }
}

impl<CR: BVGraphCodesReader + BVGraphCodesSkipper + BitSeek> Iterator for DegreesIter<CR> {
    type Item = (usize, usize, usize);
    fn next(&mut self) -> Option<(usize, usize, usize)> {
        if self.node_id >= self.number_of_nodes {
            return None;
        }
        let offset = self.get_pos();
        Some((offset, self.node_id, self.next_degree().unwrap()))
    }
}

impl<CR: BVGraphCodesReader + BVGraphCodesSkipper> DegreesIter<CR> {
    /// Create a new iterator over the degrees of the graph.
    pub fn new(
        codes_reader: CR,
        min_interval_length: usize,
        compression_window: usize,
        number_of_nodes: usize,
    ) -> Self {
        Self {
            codes_reader,
            backrefs: vec![0; compression_window + 1],
            node_id: 0,
            min_interval_length,
            compression_window,
            number_of_nodes,
        }
    }

    /// Get the number of nodes in the graph
    #[inline(always)]
    pub fn get_number_of_nodes(&self) -> usize {
        self.number_of_nodes
    }

    #[inline(always)]
    /// Manually get the next degree, this is what the iterator calls internally
    /// but it calls `.unwrap()` on it because the trait Graph doesn't allows
    /// errors.
    pub fn next_degree(&mut self) -> Result<usize> {
        let degree = self.codes_reader.read_outdegree() as usize;
        // no edges, we are done!
        if degree == 0 {
            self.backrefs[self.node_id % self.compression_window] = degree;
            self.node_id += 1;
            return Ok(degree);
        }

        let mut nodes_left_to_decode = degree;

        // read the reference offset
        let ref_delta = if self.compression_window != 0 {
            self.codes_reader.read_reference_offset() as usize
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
            let number_of_blocks = self.codes_reader.read_block_count() as usize;

            // no blocks, we copy everything
            if number_of_blocks == 0 {
                nodes_left_to_decode -= ref_degree;
            } else {
                // otherwise we copy only the blocks of even index

                // the first block could be zero
                let mut idx = self.codes_reader.read_blocks() as usize;
                nodes_left_to_decode -= idx;

                // while the other can't
                for block_id in 1..number_of_blocks {
                    let block = self.codes_reader.read_blocks() as usize;
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
            let number_of_intervals = self.codes_reader.read_interval_count() as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                #[cfg(feature = "skips")]
                let _ = self.codes_reader.skip_interval_starts(1);
                #[cfg(not(feature = "skips"))]
                let _ = self.codes_reader.read_interval_start();
                let mut delta = self.codes_reader.read_interval_len() as usize;
                delta += self.min_interval_length;
                // save the first interval
                nodes_left_to_decode -= delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    #[cfg(feature = "skips")]
                    let _ = self.codes_reader.skip_interval_starts(1);
                    #[cfg(not(feature = "skips"))]
                    let _ = self.codes_reader.read_interval_start();
                    delta = self.codes_reader.read_interval_len() as usize;
                    delta += self.min_interval_length;

                    nodes_left_to_decode -= delta;
                }
            }
        }

        // decode the extra nodes if needed
        if nodes_left_to_decode != 0 {
            // pre-allocate with capacity for efficency
            #[cfg(feature = "skips")]
            let _ = self.codes_reader.skip_first_residuals(1);
            #[cfg(not(feature = "skips"))]
            let _ = self.codes_reader.read_first_residual();

            #[cfg(feature = "skips")]
            let _ = self
                .codes_reader
                .skip_residuals(nodes_left_to_decode.saturating_sub(1));
            #[cfg(not(feature = "skips"))]
            for _ in 1..nodes_left_to_decode {
                let _ = self.codes_reader.read_residual();
            }
        }

        self.backrefs[self.node_id % self.compression_window] = degree;
        self.node_id += 1;
        Ok(degree)
    }
}
