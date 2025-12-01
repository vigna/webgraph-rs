/*
 * SPDX-FileCopyrightText: 2024 Davide Cologni
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::io::Write;

use super::bvcomp::{CompStats, Compressor};
use super::OffsetsWriter;
use crate::prelude::*;
use common_traits::Sequence;

/// An Entry for the table used to save the intermediate computation
/// of the dynamic algorithm to select the best references.
/// It represents if a reference to a node, with a know amount of previous
/// references chain length, is chosen and how much less it costs to all his
/// referent with respect to compress the node without any selected reference.
#[derive(Default, Clone)]
struct ReferenceTableEntry {
    saved_cost: f32,
    chosen: bool,
}

/// A BvGraph compressor based on the approximate algorithm described in
/// "[Zuckerli: A New Compressed Representation for Graphs](
/// https://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=9272613)",
/// which is used to compress a graph into a BvGraph.   
/// This compressor uses a dynamic algorithm to find the best allowed references,
/// based on the the result of the greedy selection without the reference constraint.
/// In the end, it greedily adds references that are valid but not included in the first
/// selection.
/// To perform the dynamic part of the algorithm, all references and backrefs should be
/// should be stored.
/// To avoid high memory consumption, the algorithm is executed on chunks of `chunk_size`
/// elements.
/// Note that unlike the standard reference selection algorithm (`BVComp`), it
/// only writes the adjacency list to the child compressor when the chunk is full or when
/// `flush` is called.
#[derive(Debug)]
pub struct BvCompZ<E, W: Write> {
    /// The ring-buffer that stores the neighbors of the last
    /// `compression_window` neighbors
    backrefs: CircularBuffer<Vec<usize>>,
    /// The references to the adjacency list to copy
    references: Vec<usize>,
    /// Saved costs of each reference in the chunk and his compression window
    reference_costs: Matrix<u64>,
    /// Estimate costs in saved bits using the current reference selection versus the extensive list   
    saved_costs: Vec<f32>,
    /// The number of nodes for which the reference selection algorithm is executed.
    /// Used in the dynamic algorithm to manage the tradeoff between memory consumption
    /// and space gained in compression.
    chunk_size: usize,
    /// The bitstream writer, this implements the mock function so we can
    /// do multiple tentative compressions and use the real one once we figured
    /// out how to compress the graph best
    encoder: E,
    /// The offset writer to write the offsets of each node.
    offsets_writer: OffsetsWriter<W>,
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
    /// The first node of the chunk in which the nodes' references are calculated together
    start_chunk_node: usize,
    /// The statistics of the compression process.
    stats: CompStats,
}

impl<E: EncodeAndEstimate, W: Write> GraphCompressor for BvCompZ<E, W> {
    /// Push a new node to the compressor.
    /// The iterator must yield the successors of the node and the nodes HAVE
    /// TO BE CONTIGUOUS (i.e. if a node has no neighbors you have to pass an
    /// empty iterator).
    /// It returns a non-zero value only if is the last element of a chunk and
    /// so all the pending adjacency lists are optimized and then written to
    /// encoder.
    fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
        // collect the iterator inside the backrefs, to reuse the capacity already
        // allocated
        {
            let mut succ_vec = self.backrefs.take(self.curr_node);
            succ_vec.clear();
            succ_vec.extend(succ_iter);
            self.backrefs.replace(self.curr_node, succ_vec);
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
            compressor.write(
                &mut self.encoder,
                self.curr_node,
                None,
                self.min_interval_length,
            )?;
            // update the current node
            self.curr_node += 1;
            return Ok(());
        }
        let relative_index_in_chunk = self.curr_node - self.start_chunk_node;
        // The delta of the best reference, by default 0 which is no compression
        let mut ref_delta = 0;
        let cost = {
            let mut estimator = self.encoder.estimator();
            // Write the compressed data
            compressor.write(
                &mut estimator,
                self.curr_node,
                Some(0),
                self.min_interval_length,
            )?
        };
        let mut saved_cost = 0;
        self.reference_costs[(relative_index_in_chunk, 0)] = cost;
        let mut min_bits = cost;

        let deltas = 1 + self
            .compression_window
            .min(self.curr_node - self.start_chunk_node);
        // compression windows is not zero, so compress the current node
        for delta in 1..deltas {
            let ref_node = self.curr_node - delta;
            // Get the neighbors of this previous len_zeta_node
            let ref_list = &self.backrefs[ref_node];
            // No neighbors, no compression
            if ref_list.is_empty() {
                continue;
            }
            // We don't check the reference selection constraint because
            // here we are constructing what in the paper is calls the
            // "maximum-weight directed forest", which is the same as executing
            // the standard reference selection algorithm without the max_ref
            // constraint

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
            self.reference_costs[(relative_index_in_chunk, delta)] = bits;
            // keep track of the best, it's strictly less so we keep the
            // nearest one in the case of multiple equal ones
            if bits < min_bits {
                saved_cost = cost - bits;
                min_bits = bits;
                ref_delta = delta;
            }
        }
        // consistency check
        assert_eq!(
            self.references.len(),
            self.curr_node - self.start_chunk_node
        );
        // save the cost and the chosen reference
        // the `references` array represents the maximum forest: each node
        // contains the index of its parent.
        // Note that in the forest exists a node from A to B
        // if B choose A as a reference, so it's a forest because can exists
        // multiple children but each node have at most one parent (my
        // reference).
        self.saved_costs.push(saved_cost as f32);
        self.references.push(ref_delta);
        self.curr_node += 1;
        if self.references.len() >= self.chunk_size {
            self.comp_refs()?;
        }
        Ok(())
    }

    /// Consumes the compressor and returns the number of bits written by
    /// flushing the encoder and writing the pending chunk
    fn flush(mut self) -> anyhow::Result<CompStats> {
        // Flush bits are just padding
        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        Ok(self.stats)
    }
}

impl<E: EncodeAndEstimate, W: Write> BvCompZ<E, W> {
    /// This value for `min_interval_length` implies that no intervalization will be performed.
    pub const NO_INTERVALS: usize = Compressor::NO_INTERVALS;

    /// Creates a new BvGraph compressor.
    pub fn new(
        encoder: E,
        offsets_writer: OffsetsWriter<W>,
        compression_window: usize,
        chunk_size: usize,
        max_ref_count: usize,
        min_interval_length: usize,
        start_node: usize,
    ) -> Self {
        BvCompZ {
            backrefs: CircularBuffer::new(chunk_size + 1),
            reference_costs: Matrix::new(chunk_size + 1, compression_window + 1),
            references: Vec::with_capacity(chunk_size + 1),
            saved_costs: Vec::with_capacity(chunk_size + 1),
            chunk_size,
            encoder,
            offsets_writer,
            min_interval_length,
            compression_window,
            max_ref_count,
            start_chunk_node: start_node,
            curr_node: start_node,
            compressors: (0..compression_window + 1)
                .map(|_| Compressor::new())
                .collect(),
            stats: CompStats::default(),
        }
    }

    fn comp_refs(&mut self) -> anyhow::Result<()> {
        let n = self.references.len();
        self.update_references_for_max_length();
        assert_eq!(n, self.curr_node - self.start_chunk_node);
        assert_eq!(self.start_chunk_node, self.curr_node - n);

        // Completing Zuckerli algorithm using greedy algorithm
        // to add back the available references that are now valid
        // and not included in the maximum forest
        // calculate length of previous references' chains
        let mut chain_length = vec![0usize; self.chunk_size];
        for i in 0..n {
            if self.references[i] != 0 {
                let parent = i - self.references[i];
                chain_length[i] = chain_length[parent] + 1;
            }
        }
        // calculate the length of next reference chain
        let mut forward_chain_length = vec![0usize; self.chunk_size];
        for i in (0..n).rev() {
            if self.references[i] != 0 {
                // check if the subsequent length of my chain is greater than the one of
                // other children of my parent
                let parent = i - self.references[i];
                forward_chain_length[parent] =
                    forward_chain_length[parent].max(forward_chain_length[i] + 1);
            }
        }
        for relative_index_in_chunk in 0..n {
            let node_index = self.curr_node - n + relative_index_in_chunk;
            // recalculate the chain length because the reference can be changed
            // after a greedy re-add in a previous iteration
            if self.references[relative_index_in_chunk] != 0 {
                let parent = relative_index_in_chunk - self.references[relative_index_in_chunk];
                chain_length[relative_index_in_chunk] = chain_length[parent] + 1;
            }
            // first get the number of bits used to compress the current node without references
            let mut min_bits = self.reference_costs[(relative_index_in_chunk, 0)];

            let deltas = 1 + self.compression_window.min(relative_index_in_chunk);
            // compression windows is not zero, so compress the current node
            for delta in 1..deltas {
                // Repeat the reference selection only on the arcs that don't
                // violate the maximum reference constraint
                if chain_length[relative_index_in_chunk - delta]
                    + forward_chain_length[relative_index_in_chunk]
                    + 1
                    > self.max_ref_count
                {
                    continue;
                }
                let reference_index = node_index - delta;
                let ref_list = &self.backrefs[reference_index];
                // No neighbors, no compression
                if ref_list.is_empty() {
                    continue;
                }
                // Read how many bits it would use for this reference
                let bits = self.reference_costs[(relative_index_in_chunk, delta)];
                // keep track of the best, it's strictly less so we keep the
                // nearest one in the case of multiple equal ones
                if bits < min_bits {
                    min_bits = bits;
                    self.references[relative_index_in_chunk] = delta;
                }
            }
            if self.references[relative_index_in_chunk] != 0 {
                let parent = relative_index_in_chunk - self.references[relative_index_in_chunk];
                chain_length[relative_index_in_chunk] = chain_length[parent] + 1;
            }
        }

        let mut compressor = Compressor::new();
        for i in 0..n {
            let node_index = self.curr_node - n + i;
            let curr_list = &self.backrefs[node_index];
            let reference = self.references[i];
            let ref_list = if reference == 0 {
                None
            } else {
                let reference_index = node_index - reference;
                Some(self.backrefs[reference_index].as_slice()).filter(|list| !list.is_empty())
            };
            compressor.compress(curr_list, ref_list, self.min_interval_length)?;
            let bits = compressor.write(
                &mut self.encoder,
                node_index,
                Some(reference),
                self.min_interval_length,
            )?;
            self.stats.written_bits += bits;
            self.stats.offsets_written_bits += self.offsets_writer.push(bits)? as u64;
        }
        // reset the chunk starting point
        self.start_chunk_node = self.curr_node;
        // clear the refs array and the backrefs
        self.references.clear();
        self.saved_costs.clear();
        Ok(())
    }

    // Dynamic algorithm to calculate the best subforest of the maximum one
    // that satisfy the maximum reference constraint.
    fn update_references_for_max_length(&mut self) {
        // consistency checks
        let n = self.references.len();
        debug_assert!(self.saved_costs.len() == n);
        for i in 0..n {
            debug_assert!(self.references[i] <= i);
            debug_assert!(self.saved_costs[i] >= 0.0);
            if self.references[i] == 0 {
                debug_assert!(self.saved_costs[i] == 0.0);
            }
        }

        // dag of nodes that points to the i-th element of the vector
        let mut out_edges: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, reference) in self.references.iter().enumerate() {
            // 0 <= references[i] <= windows_size
            if reference != 0 {
                // for each j in out_edges, for each i in out_edges[j]: j + window_size >= i
                out_edges[i - reference].push(i);
            }
        }
        // table for dynamic programming: the entry x, i of the table represent
        // the maximum weight of the subforest rooted in x that has no paths
        // longer than i.
        // So using dyn[(node, max_length)] denotes the weight where we are
        // considering "node" to be the root (so without reference).
        let mut dyn_table: Matrix<ReferenceTableEntry> = Matrix::new(n, self.max_ref_count + 1);

        for i in (0..n).rev() {
            // in the paper M_r(i) so the case where I don't choose this node to be referred from other lists
            // and favor the children so they can be have paths of the maximum length (n)
            let mut child_sum_full_chain = 0.0;
            for child in out_edges[i].iter() {
                child_sum_full_chain += dyn_table[(child, self.max_ref_count)].saved_cost;
            }

            dyn_table[(i, 0)] = ReferenceTableEntry {
                saved_cost: child_sum_full_chain,
                chosen: false,
            };

            // counting parent link, if any.
            for links_to_use in 1..=self.max_ref_count {
                // Now we are choosing i to have at most children chains of 'links_to_use'
                // (because we used 'max_length - links_to_use' links before somewhere)
                let mut child_sum = self.saved_costs[i];
                // Take it.
                for child in out_edges[i].iter() {
                    child_sum += dyn_table[(child, links_to_use - 1)].saved_cost;
                }
                dyn_table[(i, links_to_use)] = if child_sum > child_sum_full_chain {
                    ReferenceTableEntry {
                        saved_cost: child_sum,
                        chosen: true,
                    }
                } else {
                    ReferenceTableEntry {
                        saved_cost: child_sum_full_chain,
                        chosen: false,
                    }
                };
            }
        }

        let mut available_length = vec![self.max_ref_count; n];
        // always choose the maximum available lengths calculated in the previous step
        for i in 0..self.references.len() {
            if dyn_table[(i, available_length[i])].chosen {
                // Taken: push available_length.
                for child in out_edges[i].iter() {
                    available_length[child] = available_length[i] - 1;
                }
            } else {
                // Not taken: remove reference.
                self.references[i] = 0;
            }
        }
    }
}

#[cfg(test)]
mod test {

    use self::sequential::Iter;

    use super::*;
    use dsi_bitstream::prelude::*;
    use itertools::Itertools;
    use lender::prelude::*;
    use std::fs::File;
    use std::io::{BufReader, BufWriter};

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
        let compression_window = 32;
        let min_interval_length = 4;

        let seq_graph = BvGraphSeq::with_basename("../data/cnr-2000")
            .endianness::<BE>()
            .load()?;

        // Compress the graph
        let file_path = "../data/cnr-2000.bvcompz";
        let bit_write = <BufBitWriter<BE, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
            File::create(file_path)?,
        )));

        // Compress the graph
        let offsets_path = "tests/data/cnr-2000.offsetsz";
        let offsets_writer = OffsetsWriter::from_path(offsets_path)?;

        let comp_flags = CompFlags {
            ..Default::default()
        };

        let codes_writer = <ConstCodesEncoder<BE, _>>::new(bit_write);

        let mut bvcomp = BvCompZ::new(
            codes_writer,
            offsets_writer,
            compression_window,
            1000,
            3,
            min_interval_length,
            0,
        );

        bvcomp.extend(&seq_graph).unwrap();
        bvcomp.flush()?;

        // Read it back

        let bit_read = <BufBitReader<BE, _>>::new(<WordAdapter<u32, _>>::new(BufReader::new(
            File::open(file_path)?,
        )));

        //let codes_reader = <DynamicCodesReader<LE, _>>::new(bit_read, &comp_flags)?;
        let codes_reader = <ConstCodesDecoder<BE, _>>::new(bit_read, &comp_flags)?;

        let mut seq_iter = Iter::new(
            codes_reader,
            seq_graph.num_nodes(),
            compression_window,
            min_interval_length,
        );
        // Check that the graph is the same
        let mut iter = seq_graph.iter().enumerate();
        while let Some((i, (true_node_id, true_succ))) = iter.next() {
            let (seq_node_id, seq_succ) = seq_iter.next().unwrap();

            assert_eq!(true_node_id, i);
            assert_eq!(true_node_id, seq_node_id);
            assert_eq!(
                true_succ.collect_vec(),
                seq_succ.into_iter().collect_vec(),
                "node_id: {}",
                i
            );
        }
        std::fs::remove_file(file_path).unwrap();

        Ok(())
    }

    fn test_compression(
        compression_window: usize,
        min_interval_length: usize,
    ) -> anyhow::Result<()> {
        let seq_graph = BvGraphSeq::with_basename("../data/cnr-2000")
            .endianness::<BE>()
            .load()?;

        // Compress the graph
        let mut buffer: Vec<u64> = Vec::new();
        let bit_write = <BufBitWriter<LE, _>>::new(MemWordWriterVec::new(&mut buffer));

        // Compress the graph
        let mut buffer: Vec<u8> = Vec::new();
        let offsets_writer = OffsetsWriter::from_write(&mut buffer)?;

        let comp_flags = CompFlags {
            ..Default::default()
        };

        let codes_writer = <ConstCodesEncoder<LE, _>>::new(bit_write);

        let max_ref_count = 3;
        let mut bvcomp = BvCompZ::new(
            codes_writer,
            offsets_writer,
            compression_window,
            10000,
            max_ref_count,
            min_interval_length,
            0,
        );

        bvcomp.extend(&seq_graph).unwrap();
        bvcomp.flush()?;

        // Read it back
        let buffer_32: &[u32] = unsafe { buffer.align_to().1 };
        let bit_read = <BufBitReader<LE, _>>::new(MemWordReader::new(buffer_32));

        //let codes_reader = <DynamicCodesReader<LE, _>>::new(bit_read, &comp_flags)?;
        let codes_reader = <ConstCodesDecoder<LE, _>>::new(bit_read, &comp_flags)?;

        let mut seq_iter = Iter::new(
            codes_reader,
            seq_graph.num_nodes(),
            compression_window,
            min_interval_length,
        );
        // Check that the graph is the same
        let mut iter = seq_graph.iter().enumerate();
        while let Some((i, (true_node_id, true_succ))) = iter.next() {
            let (seq_node_id, seq_succ) = seq_iter.next().unwrap();

            assert_eq!(true_node_id, i);
            assert_eq!(true_node_id, seq_node_id);
            assert_eq!(
                true_succ.collect_vec(),
                seq_succ.collect_vec(),
                "node_id: {}",
                i
            );
        }

        Ok(())
    }
}
