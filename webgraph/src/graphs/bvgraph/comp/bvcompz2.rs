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
use dary_heap::QuaternaryHeap;
use lender::prelude::*;

/// A BvGraph compressor based on a Kruskal-like greedy algorithm applied to chunks.
///
/// This compressor collects nodes into chunks and then uses a heap-based greedy
/// algorithm (similar to Kruskal's MST algorithm) to select the globally best
/// references across the entire chunk, respecting the `max_ref_count` constraint.
///
/// Unlike [`BvCompZ`] which uses dynamic programming, this compressor:
/// 1. Computes savings for each possible reference in the chunk
/// 2. Uses a max-heap to greedily select the best references globally
/// 3. Maintains a disjoint-set-like structure to track reference chain depths
///
/// This approach provides a different trade-off: it may find better global
/// solutions by considering all references simultaneously rather than using
/// the DP approach.
#[derive(Debug)]
pub struct BvCompZ2<E, W: Write> {
    /// The successors of each node in the chunk.
    backrefs: RaggedArray<usize>,
    /// The chosen reference delta for each node in the chunk (0 = no reference).
    references: Vec<usize>,
    /// Savings for each node and reference delta.
    /// savings[node][delta-1] = bits saved by using reference delta instead of no reference.
    /// We don't store delta=0 since it always has 0 savings.
    savings: Vec<Vec<u64>>,
    /// The number of nodes for which the reference selection algorithm is executed.
    chunk_size: usize,
    /// The bitstream writer.
    encoder: E,
    /// The offset writer to write the offsets of each node.
    offsets_writer: OffsetsWriter<W>,
    /// When compressing we need to store metadata. So we store the compressors
    /// to reuse the allocations for perf reasons.
    compressors: Vec<Compressor>,
    /// The number of previous nodes that will be considered during the compression.
    compression_window: usize,
    /// The maximum recursion depth that will be used to decompress a node.
    max_ref_count: usize,
    /// The minimum length of sequences that will be compressed as a (start, len).
    min_interval_length: usize,
    /// The current node we are compressing.
    curr_node: usize,
    /// The first node of the chunk.
    start_chunk_node: usize,
    /// The statistics of the compression process.
    stats: CompStats,
}

impl BvCompZ2<(), std::io::Sink> {
    /// Convenience method returning a [`BvCompConfig`] with
    /// settings suitable for the Zuckerli-based compressor.
    pub fn with_basename(basename: impl AsRef<Path>) -> BvCompConfig {
        BvCompConfig::new(basename)
            .with_bvgraphz2()
            .with_comp_flags(CompFlags {
                compression_window: 32,
                ..Default::default()
            })
    }
}

impl<E: EncodeAndEstimate, W: Write> BvCompZ2<E, W> {
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
        BvCompZ2 {
            backrefs: RaggedArray::new(),
            references: Vec::with_capacity(chunk_size + 1),
            savings: Vec::with_capacity(chunk_size + 1),
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
    pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
        // Collect the iterator inside the backrefs
        self.backrefs.push(succ_iter);
        let offset_in_chunk = self.curr_node - self.start_chunk_node;
        let curr_list = &self.backrefs[offset_in_chunk];
        self.stats.num_nodes += 1;
        self.stats.num_arcs += curr_list.len() as u64;

        // Compress with no reference (delta = 0) - this is our baseline
        let compressor = &mut self.compressors[0];
        compressor.compress(curr_list, None, self.min_interval_length)?;

        // Handle compression_window == 0 case: write immediately
        if self.compression_window == 0 {
            let written_bits = compressor.write(
                &mut self.encoder,
                self.curr_node,
                None,
                self.min_interval_length,
            )?;
            self.curr_node += 1;
            self.stats.offsets_written_bits += self.offsets_writer.push(written_bits)? as u64;
            self.stats.written_bits += written_bits;
            return Ok(());
        }

        // Compute base cost (no reference)
        let base_cost = {
            let mut estimator = self.encoder.estimator();
            compressor.write(
                &mut estimator,
                self.curr_node,
                Some(0),
                self.min_interval_length,
            )?
        };

        // Compute savings for each possible reference delta
        let mut node_savings = Vec::with_capacity(self.compression_window);
        let max_delta = self.compression_window.min(offset_in_chunk);

        for delta in 1..=max_delta {
            let ref_list = &self.backrefs[offset_in_chunk - delta];
            if ref_list.is_empty() {
                // No savings if reference list is empty
                node_savings.push(0);
                continue;
            }

            let compressor = &mut self.compressors[delta];
            compressor.compress(curr_list, Some(ref_list), self.min_interval_length)?;
            let cost = {
                let mut estimator = self.encoder.estimator();
                compressor.write(
                    &mut estimator,
                    self.curr_node,
                    Some(delta),
                    self.min_interval_length,
                )?
            };
            // Savings = base_cost - cost (saturating to 0 if cost > base_cost)
            node_savings.push(base_cost.saturating_sub(cost));
        }

        self.savings.push(node_savings);
        self.curr_node += 1;

        // Process chunk when full
        if self.savings.len() >= self.chunk_size {
            self.process_chunk()?;
        }

        Ok(())
    }

    /// Consumes the compressor and returns the statistics.
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        if self.compression_window > 0 && !self.savings.is_empty() {
            self.process_chunk()?;
        }
        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        Ok(self.stats)
    }

    /// Given an iterator over the nodes successors iterators, push them all.
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

    /// Process the current chunk: compute optimal references and write all nodes.
    fn process_chunk(&mut self) -> anyhow::Result<()> {
        self.compute_references_greedy();
        self.write_chunk()?;
        self.clear_chunk();
        Ok(())
    }

    /// Use a Kruskal-like greedy algorithm to select references for the entire chunk.
    fn compute_references_greedy(&mut self) {
        let n = self.savings.len();
        if n == 0 {
            return;
        }

        // Initialize references to 0 (no reference)
        self.references.clear();
        self.references.resize(n, 0);

        if self.max_ref_count == 0 {
            return;
        }

        /// An arc representing a possible reference from a node to another.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        struct Arc {
            /// How many bits we save by taking this reference
            savings: u64,
            /// The delta of the reference (negated for ordering: smaller delta wins ties)
            neg_delta: isize,
            /// The index in the chunk that is using this reference
            node_idx: usize,
        }

        impl Ord for Arc {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // Max-heap: higher savings first, then higher neg_delta (= smaller delta), then smaller node_idx
                (self.savings, self.neg_delta, other.node_idx).cmp(&(
                    other.savings,
                    other.neg_delta,
                    self.node_idx,
                ))
            }
        }

        impl PartialOrd for Arc {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        // Build heap with all positive-savings arcs
        let mut heap = QuaternaryHeap::with_capacity(n * self.compression_window);
        for node_idx in 0..n {
            let max_delta = self.compression_window.min(node_idx);
            for delta in 1..=max_delta {
                let savings = self.savings[node_idx][delta - 1];
                if savings > 0 {
                    heap.push(Arc {
                        savings,
                        neg_delta: -(delta as isize),
                        node_idx,
                    });
                }
            }
        }

        // Disjoint-set-like structure to track reference chains
        // parent[i] = parent node index, or usize::MAX if unassigned
        let mut parent = vec![usize::MAX; n];
        // height[i] = maximum distance from this node to any of its descendants
        let mut height = vec![0usize; n];

        // Kruskal-like greedy: repeatedly pick the globally best arc
        while let Some(Arc {
            neg_delta,
            node_idx,
            ..
        }) = heap.pop()
        {
            let delta = (-neg_delta) as usize;

            // Skip if already assigned a reference
            if parent[node_idx] != usize::MAX {
                continue;
            }

            // Compute the depth this node would have with this delta
            let ref_idx = node_idx - delta;
            let depth = {
                let mut d = 1;
                let mut idx = ref_idx;
                while idx < n && parent[idx] != usize::MAX {
                    d += 1;
                    idx = parent[idx];
                }
                d
            };

            let h = height[node_idx];

            // Check if the resulting chain depth would exceed max_ref_count
            if depth + h > self.max_ref_count {
                continue;
            }

            // Accept this reference
            self.references[node_idx] = delta;
            parent[node_idx] = ref_idx;

            // Propagate height to ancestors within the chunk
            {
                let mut idx = ref_idx;
                let mut propagated_h = h + 1;
                while idx < n {
                    if height[idx] >= propagated_h {
                        break;
                    }
                    height[idx] = propagated_h;
                    if parent[idx] == usize::MAX {
                        break;
                    }
                    idx = parent[idx];
                    propagated_h += 1;
                }
            }
        }
    }

    /// Write all nodes in the chunk to the encoder.
    fn write_chunk(&mut self) -> anyhow::Result<()> {
        let n = self.references.len();
        let compressor = self
            .compressors
            .first_mut()
            .expect("at least one compressor available");

        for i in 0..n {
            let node_index = self.start_chunk_node + i;
            let curr_list = &self.backrefs[i];
            let reference = self.references[i];
            let ref_list = if reference == 0 {
                None
            } else {
                let reference_index = i - reference;
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

        Ok(())
    }

    /// Clear the chunk data structures for the next chunk.
    fn clear_chunk(&mut self) {
        self.start_chunk_node = self.curr_node;
        self.references.clear();
        self.savings.clear();

        // Custom resizing logic for backrefs: shrink if over-allocated
        if self.backrefs.values_capacity() > 4 * self.backrefs.num_values() {
            self.backrefs
                .shrink_values_to(self.backrefs.values_capacity() / 2);
        }
        self.backrefs.clear();
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

        BvCompZ2::with_basename(&basename).comp_graph::<BE>(&cnr_2000)?;

        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;

        BvCompZ2::with_basename(&basename).par_comp_graph::<BE>(&cnr_2000)?;
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

        BvCompZ2::with_basename(&basename)
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
