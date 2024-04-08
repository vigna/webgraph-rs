/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::path::PathBuf;

use super::*;
use crate::utils::nat2int;
use crate::utils::CircularBuffer;
use anyhow::Result;
use bitflags::Flags;
use dsi_bitstream::traits::BitSeek;
use dsi_bitstream::traits::BE;
use lender::*;

/// A sequential BVGraph that can be read from a `codes_reader_builder`.
/// The builder is needed because we should be able to create multiple iterators
/// and this allows us to have a single place where to store the mmaped file.
#[derive(Debug, Clone)]
pub struct BVGraphSeq<F> {
    factory: F,
    number_of_nodes: usize,
    number_of_arcs: Option<u64>,
    compression_window: usize,
    min_interval_length: usize,
}

impl BVGraphSeq<()> {
    pub fn with_basename(
        basename: impl AsRef<std::path::Path>,
    ) -> LoadConfig<BE, Sequential, Dynamic, Mmap, Mmap> {
        LoadConfig {
            basename: PathBuf::from(basename.as_ref()),
            graph_load_flags: Flags::empty(),
            offsets_load_flags: Flags::empty(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F: SequentialDecoderFactory> SplitLabeling for BVGraphSeq<F>
where
    for<'a> <F as SequentialDecoderFactory>::Decoder<'a>: Clone + Send + Sync,
{
    type SplitLender<'a> = split::seq::Lender<'a, BVGraphSeq<F>> where Self: 'a;
    type IntoIterator<'a> = split::seq::IntoIterator<'a, BVGraphSeq<F>> where Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::seq::Iter::new(self.iter(), how_many)
    }
}

impl<F: SequentialDecoderFactory> SequentialLabeling for BVGraphSeq<F> {
    type Label = usize;
    type Lender<'a> = Iter<F::Decoder<'a>>
    where
        Self: 'a;

    #[inline(always)]
    /// Return the number of nodes in the graph
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        self.number_of_arcs
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        let mut iter = Iter::new(
            self.factory.new_decoder().unwrap(),
            self.number_of_nodes,
            self.compression_window,
            self.min_interval_length,
        );

        for _ in 0..from {
            iter.next();
        }

        iter
    }
}

impl<F: SequentialDecoderFactory> SequentialGraph for BVGraphSeq<F> {}

impl<'a, F: SequentialDecoderFactory> IntoLender for &'a BVGraphSeq<F> {
    type Lender = <BVGraphSeq<F> as SequentialLabeling>::Lender<'a>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<F: SequentialDecoderFactory> BVGraphSeq<F> {
    /// Create a new sequential graph from a codes reader builder
    /// and the number of nodes.
    pub fn new(
        codes_reader_builder: F,
        number_of_nodes: usize,
        number_of_arcs: Option<u64>,
        compression_window: usize,
        min_interval_length: usize,
    ) -> Self {
        Self {
            factory: codes_reader_builder,
            number_of_nodes,
            number_of_arcs,
            compression_window,
            min_interval_length,
        }
    }

    #[inline(always)]
    pub fn map_factory<F1, F0>(self, map_func: F0) -> BVGraphSeq<F1>
    where
        F0: FnOnce(F) -> F1,
        F1: SequentialDecoderFactory,
    {
        BVGraphSeq {
            factory: map_func(self.factory),
            number_of_nodes: self.number_of_nodes,
            number_of_arcs: self.number_of_arcs,
            compression_window: self.compression_window,
            min_interval_length: self.min_interval_length,
        }
    }

    #[inline(always)]
    /// Consume self and return the factory
    pub fn into_inner(self) -> F {
        self.factory
    }
}

impl<F: SequentialDecoderFactory> BVGraphSeq<F>
where
    for<'a> F::Decoder<'a>: Decode,
{
    #[inline(always)]
    /// Creates an iterator specialized in the degrees of the nodes.
    /// This is slightly faster because it can avoid decoding some of the nodes
    /// and completely skip the merging step.
    pub fn offset_deg_iter(&self) -> OffsetDegIter<F::Decoder<'_>> {
        OffsetDegIter::new(
            self.factory.new_decoder().unwrap(),
            self.number_of_nodes,
            self.compression_window,
            self.min_interval_length,
        )
    }
}

/// A fast sequential iterator over the nodes of the graph and their successors.
/// This iterator does not require to know the offsets of each node in the graph.
#[derive(Debug, Clone)]
pub struct Iter<D: Decode> {
    pub(crate) number_of_nodes: usize,
    pub(crate) compression_window: usize,
    pub(crate) min_interval_length: usize,
    pub(crate) decoder: D,
    pub(crate) backrefs: CircularBuffer<Vec<usize>>,
    pub(crate) current_node: usize,
}

impl<D: Decode + BitSeek> Iter<D> {
    #[inline(always)]
    /// Forward the call of `get_pos` to the inner `codes_reader`.
    /// This returns the current bits offset in the bitstream.
    pub fn bit_pos(&mut self) -> Result<u64, <D as BitSeek>::Error> {
        self.decoder.bit_pos()
    }
}

impl<D: Decode> Iter<D> {
    /// Create a new iterator from a codes reader
    pub fn new(
        decoder: D,
        number_of_nodes: usize,
        compression_window: usize,
        min_interval_length: usize,
    ) -> Self {
        Self {
            number_of_nodes,
            compression_window,
            min_interval_length,
            decoder,
            backrefs: CircularBuffer::new(compression_window + 1),
            current_node: 0,
        }
    }

    /// Get the successors of the next node in the stream
    pub fn next_successors(&mut self) -> Result<&[usize]> {
        let mut res = self.backrefs.take(self.current_node);
        res.clear();
        self.get_successors_iter_priv(self.current_node, &mut res)?;
        let res = self.backrefs.replace(self.current_node, res);
        self.current_node += 1;
        Ok(res)
    }

    #[inline(always)]
    /// Inner method called by `next_successors` and the iterator `next` method
    fn get_successors_iter_priv(&mut self, node_id: usize, results: &mut Vec<usize>) -> Result<()> {
        let degree = self.decoder.read_outdegree() as usize;
        // no edges, we are done!
        if degree == 0 {
            return Ok(());
        }

        // ensure that we have enough capacity in the vector for not reallocating
        results.reserve(degree.saturating_sub(results.capacity()));
        // read the reference offset
        let ref_delta = if self.compression_window != 0 {
            self.decoder.read_reference_offset() as usize
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
            let number_of_blocks = self.decoder.read_block_count() as usize;
            // no blocks, we copy everything
            if number_of_blocks == 0 {
                results.extend_from_slice(neighbours);
            } else {
                // otherwise we copy only the blocks of even index
                // the first block could be zero
                let mut idx = self.decoder.read_block() as usize;
                results.extend_from_slice(&neighbours[..idx]);

                // while the other can't
                for block_id in 1..number_of_blocks {
                    let block = self.decoder.read_block() as usize;
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
            let number_of_intervals = self.decoder.read_interval_count() as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                let node_id_offset = nat2int(self.decoder.read_interval_start());
                let mut start = (node_id as i64 + node_id_offset) as usize;
                let mut delta = self.decoder.read_interval_len() as usize;
                delta += self.min_interval_length;
                // save the first interval
                results.extend(start..(start + delta));
                start += delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + self.decoder.read_interval_start() as usize;
                    delta = self.decoder.read_interval_len() as usize;
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
            let node_id_offset = nat2int(self.decoder.read_first_residual());
            let mut extra = (node_id as i64 + node_id_offset) as usize;
            results.push(extra);
            // decode the successive extra nodes
            for _ in 1..nodes_left_to_decode {
                extra += 1 + self.decoder.read_residual() as usize;
                results.push(extra);
            }
        }

        results.sort();
        Ok(())
    }
}

impl<'succ, D: Decode> NodeLabelsLender<'succ> for Iter<D> {
    type Label = usize;
    type IntoIterator = std::iter::Copied<std::slice::Iter<'succ, Self::Label>>;
}

impl<'succ, D: Decode> Lending<'succ> for Iter<D> {
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<D: Decode> Lender for Iter<D> {
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        if self.current_node >= self.number_of_nodes as _ {
            return None;
        }
        let mut res = self.backrefs.take(self.current_node);
        res.clear();
        self.get_successors_iter_priv(self.current_node, &mut res)
            .unwrap();

        let res = self.backrefs.replace(self.current_node, res);
        let node_id = self.current_node;
        self.current_node += 1;
        Some((node_id, res.iter().copied()))
    }
}

unsafe impl<D: Decode> SortedLender for Iter<D> {}

impl<D: Decode> ExactSizeLender for Iter<D> {
    fn len(&self) -> usize {
        self.number_of_nodes - self.current_node
    }
}
