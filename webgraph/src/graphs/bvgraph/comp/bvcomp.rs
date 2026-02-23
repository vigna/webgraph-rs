/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::OffsetsWriter;
use crate::prelude::*;
use core::cmp::Ordering;
use dsi_bitstream::codes::ToNat;
use lender::prelude::*;
use std::{io::Write, path::Path};

/// Compression statistics for a BvGraph compression.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompStats {
    /// Number of source nodes compressed.
    pub num_nodes: usize,
    /// Number of arcs compressed.
    pub num_arcs: u64,
    /// Length of the compressed graph's bitstream.
    pub written_bits: u64,
    /// Length of the offsets bitstream.
    pub offsets_written_bits: u64,
}

/// Compresses a graph into the [BV graph format](super::super).
///
/// This is the standard compressor: for each node, it considers the
/// preceding nodes in a window of configurable size and greedily selects the
/// reference that minimizes the bitstream length, subject to a maximum
/// reference-chain depth (`max_ref_count`). It then splits the "extra" nodes
/// (those that cannot be copied from the reference list) into intervals and
/// residuals, as documented in the [module-level documentation](super::super).
///
/// The compressor writes two bitstreams:
///
/// - the _graph_ bitstream, through the encoder `E`;
/// - the _offsets_ bitstream, through the [`OffsetsWriter`].
///
/// Nodes must be pushed in order via [`push`](Self::push) (or
/// [`extend`](Self::extend)) and the compressor must be finalized with
/// [`flush`](Self::flush), which returns the [`CompStats`].
///
/// In most cases you do not need to instantiate this struct directly: use
/// [`BvComp::with_basename`] to obtain a [`BvCompConfig`] with suitable
/// defaults, then call [`comp_graph`](BvCompConfig::comp_graph) or
/// [`par_comp_graph`](BvCompConfig::par_comp_graph) on it.
///
/// For a compressor that uses an alternative reference-selection strategy
/// based on dynamic programming, see [`BvCompZ`](super::BvCompZ).
#[derive(Debug)]
pub struct BvComp<E, W: Write> {
    /// The ring-buffer that stores the neighbors of the last
    /// `compression_window` neighbors
    backrefs: CircularBuffer<Vec<usize>>,
    /// The ring-buffer that stores how many recursion steps are needed to
    /// decode the last `compression_window` nodes, this is used for
    /// `max_ref_count` which is used to modulate the compression / decoding
    /// speed tradeoff
    ref_counts: CircularBuffer<usize>,
    /// The bitstream writer, this implements the mock function so we can
    /// do multiple tentative compressions and use the real one once we figured
    /// out how to compress the graph best
    encoder: E,
    /// The offset writer, we should push the number of bits used by each node.
    pub offsets_writer: OffsetsWriter<W>,
    /// When compressing we need to store metadata. So we store the compressors
    /// to reuse the allocations for perf reasons.
    compressors: Vec<Compressor>,
    /// The number of previous nodes that will be considered during the compression
    compression_window: usize,
    /// The maximum recursion depth that will be used to decompress a node
    max_ref_count: usize,
    /// The minimum length of sequences that will be compressed as a (start, len)
    min_interval_length: usize,
    /// The current node we are compressing
    curr_node: usize,
    /// The first node we are compressing, this is needed because during
    /// parallel compression we need to work on different chunks
    start_node: usize,
    /// The statistics of the compression process.
    stats: CompStats,
}

impl BvComp<(), std::io::Sink> {
    /// Convenience method returning a [`BvCompConfig`] with
    /// settings suitable for the standard Boldi–Vigna compressor.
    pub fn with_basename(basename: impl AsRef<Path>) -> BvCompConfig {
        BvCompConfig::new(basename)
    }
}

/// Computes how to encode the successors of a node, given a reference
/// node. This could be a function, but we made it a struct so we can
/// reuse the allocations for performance reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Compressor {
    /// The outdegree of the node we are compressing
    outdegree: usize,
    /// The blocks of nodes we are copying from the reference node
    blocks: Vec<usize>,
    /// The non-copied nodes
    extra_nodes: Vec<usize>,
    /// The starts of the intervals
    left_interval: Vec<usize>,
    /// The lengths of the intervals
    len_interval: Vec<usize>,
    /// The nodes left to encode as gaps
    residuals: Vec<usize>,
}

impl Compressor {
    /// Constant used only to make the code more readable.
    /// When min_interval_length is 0, we don't use intervals, which might be
    /// counter-intuitive
    pub(crate) const NO_INTERVALS: usize = 0;

    /// Creates a new empty compressor
    pub(crate) fn new() -> Self {
        Compressor {
            outdegree: 0,
            blocks: Vec::with_capacity(1024),
            extra_nodes: Vec::with_capacity(1024),
            left_interval: Vec::with_capacity(1024),
            len_interval: Vec::with_capacity(1024),
            residuals: Vec::with_capacity(1024),
        }
    }

    /// Writes the current node to the bitstream, this dumps the internal
    /// buffers which are initialized by calling `compress` so this has to be
    /// called only after `compress`.
    ///
    /// This returns the number of bits written.
    pub(crate) fn write<E: Encode>(
        &self,
        writer: &mut E,
        curr_node: usize,
        reference_offset: Option<usize>,
        min_interval_length: usize,
    ) -> Result<u64, E::Error> {
        let mut written_bits: u64 = 0;
        written_bits += writer.start_node(curr_node)? as u64;
        // write the outdegree
        written_bits += writer.write_outdegree(self.outdegree as u64)? as u64;
        // write the references
        if self.outdegree != 0 {
            if let Some(reference_offset) = reference_offset {
                written_bits += writer.write_reference_offset(reference_offset as u64)? as u64;
                if reference_offset != 0 {
                    written_bits += writer.write_block_count(self.blocks.len() as _)? as u64;
                    if !self.blocks.is_empty() {
                        for i in 0..self.blocks.len() {
                            written_bits += writer.write_block((self.blocks[i] - 1) as u64)? as u64;
                        }
                    }
                }
            }
        }
        // write the intervals
        if !self.extra_nodes.is_empty() && min_interval_length != Self::NO_INTERVALS {
            written_bits += writer.write_interval_count(self.left_interval.len() as _)? as u64;

            if !self.left_interval.is_empty() {
                written_bits += writer.write_interval_start(
                    (self.left_interval[0] as i64 - curr_node as i64).to_nat(),
                )? as u64;
                written_bits += writer
                    .write_interval_len((self.len_interval[0] - min_interval_length) as u64)?
                    as u64;
                let mut prev = self.left_interval[0] + self.len_interval[0];

                for i in 1..self.left_interval.len() {
                    written_bits += writer
                        .write_interval_start((self.left_interval[i] - prev - 1) as u64)?
                        as u64;
                    written_bits += writer
                        .write_interval_len((self.len_interval[i] - min_interval_length) as u64)?
                        as u64;
                    prev = self.left_interval[i] + self.len_interval[i];
                }
            }
        }
        // write the residuals
        // first signal the number of residuals to the encoder
        writer.num_of_residuals(self.residuals.len());
        if !self.residuals.is_empty() {
            written_bits += writer
                .write_first_residual((self.residuals[0] as i64 - curr_node as i64).to_nat())?
                as u64;

            for i in 1..self.residuals.len() {
                written_bits += writer
                    .write_residual((self.residuals[i] - self.residuals[i - 1] - 1) as u64)?
                    as u64;
            }
        }

        written_bits += writer.end_node(curr_node)? as u64;
        Ok(written_bits)
    }

    /// Resets the compressor for a new compression.
    #[inline(always)]
    pub(crate) fn clear(&mut self) {
        self.outdegree = 0;
        self.blocks.clear();
        self.extra_nodes.clear();
        self.left_interval.clear();
        self.len_interval.clear();
        self.residuals.clear();
    }

    /// setup the internal buffers for the compression of the given values
    pub(crate) fn compress(
        &mut self,
        curr_list: &[usize],
        ref_list: Option<&[usize]>,
        min_interval_length: usize,
    ) -> anyhow::Result<()> {
        self.clear();
        self.outdegree = curr_list.len();

        if self.outdegree != 0 {
            if let Some(ref_list) = ref_list {
                self.diff_comp(curr_list, ref_list);
            } else {
                self.extra_nodes.extend(curr_list)
            }

            if !self.extra_nodes.is_empty() {
                if min_interval_length != Self::NO_INTERVALS {
                    self.intervalize(min_interval_length);
                } else {
                    self.residuals.extend(&self.extra_nodes);
                }
            }
        }
        debug_assert_eq!(self.left_interval.len(), self.len_interval.len());
        Ok(())
    }

    /// Get the extra nodes, compute all the intervals of consecutive nodes
    /// longer than min_interval_length and put the rest in the residuals
    fn intervalize(&mut self, min_interval_length: usize) {
        let vl = self.extra_nodes.len();
        let mut i = 0;

        while i < vl {
            let mut j = 0;
            if i < vl - 1 && self.extra_nodes[i] + 1 == self.extra_nodes[i + 1] {
                j += 1;
                while i + j < vl - 1 && self.extra_nodes[i + j] + 1 == self.extra_nodes[i + j + 1] {
                    j += 1;
                }
                j += 1;

                // Now j is the number of integers in the interval.
                if j >= min_interval_length {
                    self.left_interval.push(self.extra_nodes[i]);
                    self.len_interval.push(j);
                    i += j - 1;
                }
            }
            if j < min_interval_length {
                self.residuals.push(self.extra_nodes[i]);
            }

            i += 1;
        }
    }

    /// Compute the copy blocks and the ignore blocks.
    /// The copy blocks are blocks of nodes we will copy from the reference node.
    fn diff_comp(&mut self, curr_list: &[usize], ref_list: &[usize]) {
        // j is the index of the next successor of the current node we must examine
        let mut j = 0;
        // k is the index of the next successor of the reference node we must examine
        let mut k = 0;
        // currBlockLen is the number of entries (in the reference list) we have already copied/ignored (in the current block)
        let mut curr_block_len = 0;
        // copying is true iff we are producing a copy block (instead of an ignore block)
        let mut copying = true;

        while j < curr_list.len() && k < ref_list.len() {
            // First case: we are currently copying entries from the reference list
            if copying {
                match curr_list[j].cmp(&ref_list[k]) {
                    Ordering::Greater => {
                        /* If while copying we trespass the current element of the reference list,
                        we must stop copying. */
                        self.blocks.push(curr_block_len);
                        copying = false;
                        curr_block_len = 0;
                    }
                    Ordering::Less => {
                        /* If while copying we find a non-matching element of the reference list which
                        is larger than us, we can just add the current element to the extra list
                        and move on. j gets increased. */
                        self.extra_nodes.push(curr_list[j]);
                        j += 1;
                    }
                    Ordering::Equal => {
                        // currList[j] == refList[k]
                        /* If the current elements of the two lists are equal, we just increase the block length.
                        both j and k get increased. */
                        j += 1;
                        k += 1;
                        curr_block_len += 1;
                        // if (forReal) copiedArcs++;
                    }
                }
            } else {
                match curr_list[j].cmp(&ref_list[k]) {
                    Ordering::Greater => {
                        /* If we trespassed the current element of the reference list, we
                        increase the block length. k gets increased. */
                        k += 1;
                        curr_block_len += 1;
                    }
                    Ordering::Less => {
                        /* If we did not trespass the current element of the reference list, we just
                        add the current element to the extra list and move on. j gets increased. */
                        self.extra_nodes.push(curr_list[j]);
                        j += 1;
                    }
                    Ordering::Equal => {
                        // currList[j] == refList[k]
                        /* If we found a match we flush the current block and start a new copying phase. */
                        self.blocks.push(curr_block_len);
                        copying = true;
                        curr_block_len = 0;
                    }
                }
            }
        }
        /* We do not record the last block. The only case when we have to enqueue the last block's length
         * is when we were copying and we did not copy up to the end of the reference list.
         */
        if copying && k < ref_list.len() {
            self.blocks.push(curr_block_len);
        }

        // If there are still missing elements, we add them to the extra list.
        while j < curr_list.len() {
            self.extra_nodes.push(curr_list[j]);
            j += 1;
        }
        // add a 1 to the first block so we can uniformly write them later
        if !self.blocks.is_empty() {
            self.blocks[0] += 1;
        }
    }
}

impl<E: EncodeAndEstimate, W: Write> BvComp<E, W> {
    /// This value for `min_interval_length` implies that no intervalization will be performed.
    pub const NO_INTERVALS: usize = Compressor::NO_INTERVALS;

    /// Creates a new BvGraph compressor.
    pub fn new(
        encoder: E,
        offsets_writer: OffsetsWriter<W>,
        compression_window: usize,
        max_ref_count: usize,
        min_interval_length: usize,
        start_node: usize,
    ) -> Self {
        BvComp {
            backrefs: CircularBuffer::new(compression_window + 1),
            ref_counts: CircularBuffer::new(compression_window + 1),
            encoder,
            offsets_writer,
            min_interval_length,
            compression_window,
            max_ref_count,
            start_node,
            curr_node: start_node,
            compressors: (0..compression_window + 1)
                .map(|_| Compressor::new())
                .collect(),
            stats: CompStats::default(),
        }
    }

    /// Push a new node to the compressor.
    /// The iterator must yield the successors of the node and the nodes HAVE
    /// TO BE CONTIGUOUS (i.e. if a node has no neighbors you have to pass an
    /// empty iterator)
    pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
        // collect the iterator inside the backrefs, to reuse the capacity already
        // allocated
        {
            let succ_vec = &mut self.backrefs[self.curr_node];
            succ_vec.clear();
            succ_vec.extend(succ_iter);
            if succ_vec.len() < succ_vec.capacity() / 4 {
                succ_vec.shrink_to(succ_vec.capacity() / 2);
            }
        }
        // get the ref
        let curr_list = &self.backrefs[self.curr_node];
        self.stats.num_nodes += 1;
        self.stats.num_arcs += curr_list.len() as u64;
        // first try to compress the current node without references
        let compressor = &mut self.compressors[0];
        // Compute how we would compress this
        compressor.compress(curr_list, None, self.min_interval_length)?;
        // avoid the mock writing
        if self.compression_window == 0 {
            let written_bits = compressor.write(
                &mut self.encoder,
                self.curr_node,
                None,
                self.min_interval_length,
            )?;
            // update the current node
            self.curr_node += 1;

            // write the offset
            self.stats.offsets_written_bits += self.offsets_writer.push(written_bits)? as u64;
            self.stats.written_bits += written_bits;
            return Ok(());
        }
        // The delta of the best reference, by default 0 which is no compression
        let mut ref_delta = 0;
        let mut min_bits = {
            let mut estimator = self.encoder.estimator();
            // Write the compressed data
            compressor.write(
                &mut estimator,
                self.curr_node,
                Some(0),
                self.min_interval_length,
            )?
        };

        let mut ref_count = 0;

        let deltas = 1 + self
            .compression_window
            .min(self.curr_node - self.start_node);
        // compression windows is not zero, so compress the current node
        for delta in 1..deltas {
            let ref_node = self.curr_node - delta;
            // If the reference node is too far, we don't consider it
            let count = self.ref_counts[ref_node];
            if count >= self.max_ref_count {
                continue;
            }
            // Get the neighbors of this previous ref_node
            let ref_list = &self.backrefs[ref_node];
            // No neighbors, no compression
            if ref_list.is_empty() {
                continue;
            }
            // Get its compressor
            let compressor = &mut self.compressors[delta];
            // Compute how we would compress this
            compressor.compress(curr_list, Some(ref_list), self.min_interval_length)?;
            // Compute how many bits it would use, using the mock writer
            let bits = {
                let mut estimator = self.encoder.estimator();
                compressor.write(
                    &mut estimator,
                    self.curr_node,
                    Some(delta),
                    self.min_interval_length,
                )?
            };
            // keep track of the best, it's strictly less so we keep the
            // nearest one in the case of multiple equal ones
            if bits < min_bits {
                min_bits = bits;
                ref_delta = delta;
                ref_count = count + 1;
            }
        }
        // write the best result reusing the precomputed compression
        let compressor = &mut self.compressors[ref_delta];
        let written_bits = compressor.write(
            &mut self.encoder,
            self.curr_node,
            Some(ref_delta),
            self.min_interval_length,
        )?;
        self.ref_counts[self.curr_node] = ref_count;
        // consistency check
        // debug_assert_eq!(written_bits, min_bits);
        // update the current node
        self.curr_node += 1;
        // write the offset
        self.stats.offsets_written_bits += self.offsets_writer.push(written_bits)? as u64;
        self.stats.written_bits += written_bits;
        Ok(())
    }

    /// Consume the compressor and return the statistics about compression.
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        Ok(self.stats)
    }

    /// Given an iterator over the nodes successors iterators, push them all.
    /// The iterator must yield the successors of the node and the nodes HAVE
    /// TO BE CONTIGUOUS (i.e. if a node has no neighbors you have to pass an
    /// empty iterator).
    ///
    /// This most commonly is called with a reference to a graph.
    pub fn extend<L>(&mut self, iter_nodes: L) -> anyhow::Result<()>
    where
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        for_! ( (_, succ) in iter_nodes {
            self.push(succ.into_iter())?;
        });
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dsi_bitstream::prelude::*;
    use tempfile::Builder;

    #[test]
    fn test_compressor_no_ref() -> anyhow::Result<()> {
        let mut compressor = Compressor::new();
        compressor.compress(&[0, 1, 2, 5, 7, 8, 9], None, 2)?;
        assert_eq!(
            compressor,
            Compressor {
                outdegree: 7,
                blocks: vec![],
                extra_nodes: vec![0, 1, 2, 5, 7, 8, 9],
                left_interval: vec![0, 7],
                len_interval: vec![3, 3],
                residuals: vec![5],
            }
        );
        Ok(())
    }

    #[test]
    fn test_compressor1() -> anyhow::Result<()> {
        let mut compressor = Compressor::new();
        compressor.compress(&[0, 1, 2, 5, 7, 8, 9], Some(&[0, 1, 2]), 2)?;
        assert_eq!(
            compressor,
            Compressor {
                outdegree: 7,
                blocks: vec![],
                extra_nodes: vec![5, 7, 8, 9],
                left_interval: vec![7],
                len_interval: vec![3],
                residuals: vec![5],
            }
        );
        Ok(())
    }

    #[test]
    fn test_compressor2() -> anyhow::Result<()> {
        let mut compressor = Compressor::new();
        compressor.compress(&[0, 1, 2, 5, 7, 8, 9], Some(&[0, 1, 2, 100]), 2)?;
        assert_eq!(
            compressor,
            Compressor {
                outdegree: 7,
                blocks: vec![4],
                extra_nodes: vec![5, 7, 8, 9],
                left_interval: vec![7],
                len_interval: vec![3],
                residuals: vec![5],
            }
        );
        Ok(())
    }

    #[test]
    fn test_compressor3() -> anyhow::Result<()> {
        let mut compressor = Compressor::new();
        compressor.compress(
            &[0, 1, 2, 5, 7, 8, 9, 100],
            Some(&[0, 1, 2, 4, 7, 8, 9, 101]),
            2,
        )?;
        assert_eq!(
            compressor,
            Compressor {
                outdegree: 8,
                blocks: vec![4, 1, 3],
                extra_nodes: vec![5, 100],
                left_interval: vec![],
                len_interval: vec![],
                residuals: vec![5, 100],
            }
        );
        Ok(())
    }

    #[test]
    fn test_writer_window_zero() -> anyhow::Result<()> {
        test_compression(0, 0)?;
        test_compression(0, 1)?;
        test_compression(0, 2)?;
        Ok(())
    }

    #[test]
    fn test_writer_window_one() -> anyhow::Result<()> {
        test_compression(1, 0)?;
        test_compression(1, 1)?;
        test_compression(1, 2)?;
        Ok(())
    }

    #[test]
    fn test_writer_window_two() -> anyhow::Result<()> {
        test_compression(2, 0)?;
        test_compression(2, 1)?;
        test_compression(2, 2)?;
        Ok(())
    }

    #[test]
    fn test_writer_cnr() -> anyhow::Result<()> {
        let cnr_2000 = BvGraphSeq::with_basename("../data/cnr-2000")
            .endianness::<BE>()
            .load()?;

        let tmp_dir = Builder::new().prefix("bvcomp_test").tempdir()?;
        let basename = tmp_dir.path().join("cnr-2000");
        BvComp::with_basename(&basename).comp_graph::<BE>(&cnr_2000)?;
        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;

        BvComp::with_basename(&basename).par_comp_graph::<BE>(&cnr_2000)?;

        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;
        Ok(())
    }

    fn test_compression(
        compression_window: usize,
        min_interval_length: usize,
    ) -> anyhow::Result<()> {
        let cnr_2000 = BvGraphSeq::with_basename("../data/cnr-2000").load()?;

        let tmp_dir = Builder::new().prefix("bvcomp_test").tempdir()?;
        let basename = tmp_dir.path().join("cnr-2000");

        BvComp::with_basename(&basename)
            .with_comp_flags(CompFlags {
                compression_window,
                min_interval_length,
                ..Default::default()
            })
            .comp_graph::<BE>(&cnr_2000)?;

        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;

        labels::eq_sorted(&cnr_2000, &seq_graph)?;
        Ok(())
    }
}
