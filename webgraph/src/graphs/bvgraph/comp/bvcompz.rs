/*
 * SPDX-FileCopyrightText: 2024 Davide Cologni
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::io::Write;
use std::path::Path;

use super::OffsetsWriter;
use super::bvcomp::{CompStats, Compressor};
use crate::prelude::*;
use crate::utils::RaggedArray;
use common_traits::Sequence;
use lender::prelude::*;

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

/// Compresses a graph into the [BV graph format](super::super) using the
/// reference-selection algorithm inspired by "[Zuckerli: A New Compressed
/// Representation for Graphs][Zuckerli paper]", by Daniel Marzocchi, Luca
/// Versari, Robert Obryk, and Jyrki Alakuijala, _Proc. 2020 Data
/// Compression Conference (DCC)_, IEEE, 2020.
///
/// In the standard [`BvComp`] compressor, each node greedily picks the
/// single best reference in its window, subject to the maximum
/// reference-chain depth. This compressor instead first builds the
/// _maximum-weight forest_ of references (ignoring the chain-depth
/// constraint) over a chunk of `chunk_size` consecutive nodes, then uses a
/// dynamic-programming algorithm to extract the maximum-weight subforest
/// whose paths respect the `max_ref_count` constraint. Finally, it
/// greedily re-introduces valid references that were pruned by the
/// dynamic-programming step.
///
/// Because the algorithm needs global information about costs within each
/// chunk, successor lists are buffered and written to the encoder only when
/// the chunk is full or when [`flush`](Self::flush) is called.
///
/// In most cases you do not need to instantiate this struct directly: use
/// [`BvCompZ::with_basename`] to obtain a [`BvCompConfig`] with suitable
/// defaults (including a larger compression window of 16), then call
/// [`comp_graph`](BvCompConfig::comp_graph) or
/// [`par_comp_graph`](BvCompConfig::par_comp_graph) on it.
///
/// [Zuckerli paper]: <https://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=9272613>
#[derive(Debug)]
pub struct BvCompZ<E, W: Write> {
    /// The successors of each node in the chunk.
    backrefs: RaggedArray<usize>,
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

impl BvCompZ<(), std::io::Sink> {
    /// Convenience method returning a [`BvCompConfig`] with
    /// settings suitable for the Zuckerli-based compressor.
    pub fn with_basename(basename: impl AsRef<Path>) -> BvCompConfig {
        BvCompConfig::new(basename)
            .with_bvgraphz()
            .with_comp_flags(CompFlags {
                compression_window: 16,
                ..Default::default()
            })
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
            backrefs: RaggedArray::new(),
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

    /// Push a new node to the compressor.
    /// The iterator must yield the successors of the node and the nodes HAVE
    /// TO BE CONTIGUOUS (i.e. if a node has no neighbors you have to pass an
    /// empty iterator).
    /// It returns a non-zero value only if is the last element of a chunk and
    /// so all the pending adjacency lists are optimized and then written to
    /// encoder.
    pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
        // collect the iterator inside the backrefs, to reuse the capacity already
        // allocated
        self.backrefs.push(succ_iter);
        let offset_in_chunk = self.curr_node - self.start_chunk_node;
        // get the ref
        let curr_list = &self.backrefs[offset_in_chunk];
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
        self.reference_costs[(offset_in_chunk, 0)] = cost;
        let mut min_bits = cost;

        let deltas = 1 + self.compression_window.min(offset_in_chunk);
        // compression windows is not zero, so compress the current node
        for delta in 1..deltas {
            // Get the neighbors of this previous len_zeta_node
            let ref_list = &self.backrefs[offset_in_chunk - delta];
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
            self.reference_costs[(offset_in_chunk, delta)] = bits;
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
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        if self.compression_window > 0 {
            self.comp_refs()?;
        }
        // Flush bits are just padding
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

    fn comp_refs(&mut self) -> anyhow::Result<()> {
        // in the case of high compression (R=+inf), the best references can be found
        // by constructing a maximum-weight directed forest greedily
        if self.max_ref_count != usize::MAX {
            // the number of nodes in the chunk is different than the chunk size in the flush case
            let nodes_in_chunk = self.references.len();
            self.update_references_for_max_length();
            assert_eq!(nodes_in_chunk, self.curr_node - self.start_chunk_node);

            self.find_additional_references_greedily();
        }

        self.write_and_clear_current_chunk()?;
        Ok(())
    }

    /// Dynamic algorithm to compute the best subforest of the maximum one
    /// that satisfy the maximum reference constraint.
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
        // limit the dimensions of the dynamic programming table for edge cases where the maximum reference
        // chain exceeds the number of elements in the chunk (e.g. high compression, R=+inf).
        let max_available_references = self.max_ref_count.min(n);
        // table for dynamic programming: the entry x, i of the table represent
        // the maximum weight of the subforest rooted in x that has no paths
        // longer than i.
        // So using dyn[(node, max_length)] denotes the weight where we are
        // considering "node" to be the root (so without reference).
        let mut dyn_table: Matrix<ReferenceTableEntry> =
            Matrix::new(n, max_available_references + 1);

        for i in (0..n).rev() {
            // in the paper M_r(i) so the case where I don't choose this node to be referred from other lists
            // and favor the children so they can be have paths of the maximum length (n)
            let mut child_sum_full_chain = 0.0;
            for child in out_edges[i].iter() {
                child_sum_full_chain += dyn_table[(child, max_available_references)].saved_cost;
            }

            dyn_table[(i, 0)] = ReferenceTableEntry {
                saved_cost: child_sum_full_chain,
                chosen: false,
            };

            // counting parent link, if any.
            for links_to_use in 1..=max_available_references {
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

        let mut available_length = vec![max_available_references; n];
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

    /// Greedily adds to the reference forest new references that are not in
    /// the maximum DAG in the first step, but are still valid.
    fn find_additional_references_greedily(&mut self) {
        // Completing Zuckerli algorithm using greedy algorithm
        // to add back the available references that are now valid
        // and not included in the maximum forest
        let n = self.references.len();
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
                let reference_index = relative_index_in_chunk - delta;
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
    }

    /// Write the current chunk to the encoder and clear the compressor's internal
    /// state to start compressing the next chunk.
    fn write_and_clear_current_chunk(&mut self) -> anyhow::Result<()> {
        let n = self.references.len();
        // Reuse an existing compressor buffer to avoid per-chunk allocations
        let compressor = self
            .compressors
            .first_mut()
            .expect("at least one compressor available");
        for i in 0..n {
            let node_index = self.curr_node - n + i;
            let curr_list = &self.backrefs[node_index - self.start_chunk_node];
            let reference = self.references[i];
            let ref_list = if reference == 0 {
                None
            } else {
                let reference_index = node_index - reference - self.start_chunk_node;
                Some(&self.backrefs[reference_index]).filter(|list| !list.is_empty())
            };
            compressor.clear();
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
        // clear the refs array and the backrefs so the next chunk starts fresh
        self.references.clear();
        self.saved_costs.clear();

        // Custom resizing logic
        if self.backrefs.num_values() < self.backrefs.values_capacity() / 4 {
            self.backrefs
                .shrink_values_to(self.backrefs.values_capacity() / 2);
        }
        self.backrefs.clear();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dsi_bitstream::prelude::*;
    use tempfile::Builder;

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

        BvCompZ::with_basename(&basename).comp_graph::<BE>(&cnr_2000)?;

        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;

        BvCompZ::with_basename(&basename).par_comp_graph::<BE>(&cnr_2000)?;
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

        BvCompZ::with_basename(&basename)
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
