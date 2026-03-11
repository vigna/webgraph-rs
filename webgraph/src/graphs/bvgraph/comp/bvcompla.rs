/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dary_heap::QuaternaryHeap;

use super::OffsetsWriter;
use crate::graphs::bvgraph::comp::bvcomp::*;
use crate::prelude::*;
use lender::prelude::*;
use std::{io::Write, path::Path};

/// An arc representing a possible reference from a node in the buffer
/// to another node in the buffer (or outside).
/// Ordering: highest savings first, then smallest delta (prefer delta=0 for ties).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GreedyArc {
    /// How many bits we save taking this reference
    savings: u64,
    /// The delta of the reference
    delta: usize,
    /// The idx in the buffer that is using this reference
    buf_idx: usize,
}

impl Ord for GreedyArc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // sort by the arc that saves the most bits
        self.savings
            .cmp(&other.savings)
            // then give priority to the node that is oldest, as it's the
            // one that will be written sooner
            .then_with(|| {
                let self_ref_node = self.buf_idx as isize - self.delta as isize;
                let other_ref_node = other.buf_idx as isize - other.delta as isize;
                self_ref_node.cmp(&other_ref_node).reverse()
                // we reverse because it's a max-heap and we prefer small node ids
            })
    }
}

impl PartialOrd for GreedyArc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A BvGraph compressor with configurable look-ahead buffer.
///
/// This compressor maintains a buffer of `look_ahead` nodes and optimizes
/// their reference assignments before writing. When a new node is pushed:
/// 1. Its successors are stored and compression costs are computed
/// 2. If the buffer is full, the optimal path is computed and the oldest node is written
/// 3. The buffer slides forward
///
/// This provides a middle ground between the greedy `BvComp` (look_ahead=1)
/// and the chunk-based `BvCompZ` (look_ahead=chunk_size).
#[derive(Debug)]
pub struct BvCompLa<E, W: Write> {
    /// The ring-buffer that stores the successors of `compression_window + look_ahead + 1` nodes.
    backrefs: CircularBuffer<Vec<usize>>,
    /// The ring-buffer that stores the finalized ref_counts for written nodes.
    /// Size: compression_window + 1
    ref_counts: CircularBuffer<usize>,
    /// The ring-buffer that stores the savings of compressing each of the `look_ahead`
    /// nodes using each reference delta in [1, compression_window] compared to delta=0.
    /// savings[node][delta-1] = bits saved by using reference delta instead of no reference
    /// We don't store delta=0 since it always has 0 savings.
    savings: CircularBuffer<Vec<u64>>,
    /// The bitstream writer
    encoder: E,
    /// The offset writer, we should push the number of bits used by each node.
    pub offsets_writer: OffsetsWriter<W>,
    /// Reusable compressor for computing compression costs and writing.
    /// Unlike BvComp which needs per-delta compressor states for the final
    /// write, BvCompLa re-compresses in write_oldest_node, so a single
    /// compressor suffices. This also improves cache locality (~40KB working
    /// set vs ~680KB with compression_window+1 compressors).
    compressor: Compressor,
    /// The number of previous nodes that will be considered during the compression
    compression_window: usize,
    /// The maximum recursion depth that will be used to decompress a node
    max_ref_count: usize,
    /// The minimum length of sequences that will be compressed as a (start, len)
    min_interval_length: usize,
    /// The look-ahead buffer size
    look_ahead: usize,
    /// The first node in the buffer that hasn't been written yet
    oldest_unwritten: usize,
    /// The next node to be pushed
    curr_node: usize,
    /// The first node we are compressing (for parallel compression)
    start_node: usize,
    /// The statistics of the compression process.
    stats: CompStats,
    /// Reusable buffer for arcs in the greedy algorithm (stored as Vec,
    /// converted to/from QuaternaryHeap each call to avoid reallocations).
    greedy_arcs: Vec<GreedyArc>,
    /// Reusable buffer for parent pointers in the DSU.
    greedy_parent: Vec<usize>,
    /// Reusable buffer for heights in the DSU.
    greedy_height: Vec<usize>,
    /// Discount factor for look-ahead savings (1.0 = no discount).
    discount: f64,
    /// Precomputed fixed-point table: gamma_pow[k] = round(discount^k * (1<<32)).
    /// Used to discount interior arc savings in the greedy heap.
    gamma_pow: Vec<u64>,
}

impl BvCompLa<(), std::io::Sink> {
    /// Convenience method returning a [`BvCompConfig`] with
    /// settings suitable for the lookahead compressor.
    pub fn with_basename(basename: impl AsRef<Path>) -> BvCompConfig {
        BvCompConfig::new(basename)
    }
}

impl<E: EncodeAndEstimate, W: Write> BvCompLa<E, W> {
    /// This value for `min_interval_length` implies that no intervalization will be performed.
    pub const NO_INTERVALS: usize = Compressor::NO_INTERVALS;

    /// Creates a new BvGraph compressor with look-ahead.
    pub fn new(
        encoder: E,
        offsets_writer: OffsetsWriter<W>,
        compression_window: usize,
        max_ref_count: usize,
        min_interval_length: usize,
        look_ahead: usize,
        start_node: usize,
        discount: f64,
    ) -> Self {
        BvCompLa {
            backrefs: CircularBuffer::new(compression_window + look_ahead + 1),
            ref_counts: CircularBuffer::new(compression_window + 1),
            savings: CircularBuffer::new(look_ahead + 1),
            encoder,
            offsets_writer,
            min_interval_length,
            compression_window,
            max_ref_count,
            look_ahead,
            oldest_unwritten: start_node,
            start_node,
            curr_node: start_node,
            compressor: Compressor::new(),
            stats: CompStats::default(),
            greedy_arcs: Vec::new(),
            greedy_parent: Vec::new(),
            greedy_height: Vec::new(),
            discount,
            gamma_pow: {
                let scale = 1u64 << 32;
                let mut table = Vec::with_capacity(look_ahead + 1);
                let mut pow = 1.0_f64;
                for _ in 0..=look_ahead {
                    // Clamp to 1 so non-zero savings never collapse to 0
                    table.push((pow * scale as f64).round().max(1.0) as u64);
                    pow *= discount;
                }
                table
            },
        }
    }

    /// Compute compression savings for a node at position `node_id` against all valid references.
    /// Writes directly to self.savings[node_id] where savings[delta-1] = bits saved by using delta instead of 0.
    fn compute_savings(&mut self, node_id: usize) -> anyhow::Result<()> {
        let curr_list = &self.backrefs[node_id];
        let savings = &mut self.savings[node_id];
        savings.clear();

        // Cost with no reference (delta = 0) - this is our baseline
        self.compressor
            .compress(curr_list, None, self.min_interval_length)?;
        let base_cost = {
            let mut estimator = self.encoder.estimator();
            self.compressor
                .write(&mut estimator, node_id, Some(0), self.min_interval_length)?
        };

        // Savings with each possible reference (delta >= 1)
        let max_delta = self.compression_window.min(node_id - self.start_node);
        for delta in 1..=max_delta {
            let ref_node = node_id - delta;

            // If ref_node is already written, check max_ref_count constraint
            if ref_node < self.oldest_unwritten && self.ref_counts[ref_node] >= self.max_ref_count {
                savings.push(0);
                continue;
            }

            let ref_list = &self.backrefs[ref_node];

            // Skip empty reference lists — they can't improve compression
            if ref_list.is_empty() {
                savings.push(0);
                continue;
            }

            self.compressor
                .compress(curr_list, Some(ref_list), self.min_interval_length)?;
            let cost = {
                let mut estimator = self.encoder.estimator();
                self.compressor.write(
                    &mut estimator,
                    node_id,
                    Some(delta),
                    self.min_interval_length,
                )?
            };
            // Savings = base_cost - cost (saturating to 0 if cost > base_cost)
            savings.push(base_cost.saturating_sub(cost));
        }

        Ok(())
    }

    /// Add references greedily using a kruskal-like algorithm: repeatedly pick the
    /// (node, delta) assignment that saves the most bits globally, respecting
    /// the max_ref_count constraint.
    ///
    /// Returns (ref_delta, ref_count) for the oldest unwritten node:
    /// - ref_delta: the reference delta to use (0 = no reference)
    /// - ref_count: the depth of the reference chain for this node
    fn greedy_with_lookahead(&mut self) -> (usize, usize) {
        // compute the **real** size of the lookahead and the compression window
        let lookahead_size = self.curr_node - self.oldest_unwritten;
        let compression_window = self
            .compression_window
            .min(self.oldest_unwritten - self.start_node);
        // this is the id of the node we are writing
        let base_idx = self.oldest_unwritten;

        if lookahead_size == 0 {
            return (0, 0);
        }

        // Build heap with only positive-savings arcs, reusing the buffer.
        // Arcs with savings=0 are worse than no reference (increase chain depth with no benefit).
        self.greedy_arcs.clear();
        for buf_idx in 0..lookahead_size {
            let window = self.compression_window.min(buf_idx + compression_window);
            for delta in 1..=window {
                let raw_savings = self.savings[base_idx + buf_idx][delta - 1];
                if raw_savings > 0 {
                    // Apply geometric discount: interior arcs (large buf_idx) are
                    // devalued relative to the oldest node (buf_idx=0) which is
                    // the one actually committed this round.
                    let savings = ((raw_savings as u128 * self.gamma_pow[buf_idx] as u128) >> 32) as u64;
                    self.greedy_arcs.push(GreedyArc {
                        savings: savings.max(1), // preserve non-zero
                        delta,
                        buf_idx,
                    });
                }
            }
        }
        // Take the Vec out, build O(n) heap, then recover the buffer after use.
        let arcs = std::mem::take(&mut self.greedy_arcs);
        let mut heap = QuaternaryHeap::from(arcs);

        // DSU buffers, reused across calls.
        self.greedy_parent.clear();
        self.greedy_parent.resize(lookahead_size, usize::MAX);
        self.greedy_height.clear();
        self.greedy_height.resize(lookahead_size, 0);

        // Kruskal / Prim greedy: repeatedly pick the globally best arc.
        let result = loop {
            let Some(GreedyArc {
                delta, buf_idx, ..
            }) = heap.pop()
            else {
                // finished all arcs, use no reference
                break (0, 0);
            };

            // skip if already assigned: parent stores absolute node ID,
            // so assigned means parent <= node's own absolute ID
            if self.greedy_parent[buf_idx] <= base_idx + buf_idx {
                continue;
            }

            // compute the depth (ref_count) this node would have with this delta
            let depth = {
                let mut d = 0;
                let mut ref_id = base_idx + buf_idx - delta;
                loop {
                    d += 1;
                    if ref_id < base_idx {
                        // Outside buffer - add finalized ref_count
                        d += self.ref_counts[ref_id];
                        break;
                    }
                    // In buffer - check parent
                    let p = self.greedy_parent[ref_id - base_idx];
                    if p >= ref_id {
                        // Not assigned (MAX) or root (p == ref_id)
                        break;
                    }
                    ref_id = p;
                }
                d
            };

            let h = self.greedy_height[buf_idx];

            // using this reference means that we are merging the tree of which
            // this node is root, and the tree of which the referenced node is a leaf
            // so we need to check that the resulting tree height is valid
            if depth + h > self.max_ref_count {
                continue;
            }

            // if we reached the oldest node, we can stop because we would
            // still throw away any more work
            if buf_idx == 0 {
                break (delta, depth);
            }

            // choose this reference: store absolute node ID of parent
            self.greedy_parent[buf_idx] = base_idx + buf_idx - delta;

            // propagate height to ancestors within the buffer
            // (skip if root or parent is outside buffer)
            if delta <= buf_idx {
                let mut ref_id = base_idx + buf_idx - delta;
                let mut propagated_h = h + 1;
                while ref_id >= base_idx {
                    let ref_buf_idx = ref_id - base_idx;
                    if self.greedy_height[ref_buf_idx] >= propagated_h {
                        break;
                    }
                    self.greedy_height[ref_buf_idx] = propagated_h;
                    let p = self.greedy_parent[ref_buf_idx];
                    if p >= ref_id {
                        // Not assigned (MAX) or root
                        break;
                    }
                    ref_id = p;
                    propagated_h += 1;
                }
            }
        };

        // Recover the buffer from the heap for reuse.
        self.greedy_arcs = heap.into_vec();

        result
    }

    /// Write the oldest node in the buffer and remove it from the buffer.
    fn write_oldest_node(&mut self) -> anyhow::Result<()> {
        let node_id = self.oldest_unwritten;

        // Select the best reference using lookahead
        let (ref_delta, ref_count) = self.greedy_with_lookahead();
        assert!(ref_delta <= self.compression_window);
        assert!(ref_count <= self.max_ref_count);
        assert!(ref_delta == 0 || self.ref_counts[node_id - ref_delta] + 1 == ref_count);

        // Get the successor list and reference list
        let curr_list = &self.backrefs[node_id];
        let ref_list: Option<&[usize]> = if ref_delta > 0 {
            Some(&self.backrefs[node_id - ref_delta])
        } else {
            None
        };

        // Re-compress with the chosen delta and write
        self.compressor
            .compress(curr_list, ref_list, self.min_interval_length)?;
        let written_bits = self.compressor.write(
            &mut self.encoder,
            node_id,
            Some(ref_delta),
            self.min_interval_length,
        )?;

        // Finalize ref_count into circular buffer
        self.ref_counts[node_id] = ref_count;

        // Update statistics
        self.stats.written_bits += written_bits;
        self.stats.offsets_written_bits += self.offsets_writer.push(written_bits)? as u64;

        // Advance buffer
        self.oldest_unwritten += 1;

        Ok(())
    }

    /// Push a new node to the compressor.
    /// The iterator must yield the successors of the node and the nodes HAVE
    /// TO BE CONTIGUOUS (i.e. if a node has no neighbors you have to pass an
    /// empty iterator)
    pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
        self.stats.num_nodes += 1;
        // Handle degenerate case: no compression window means no look-ahead benefit
        if self.compression_window == 0 {
            // use the vec in backref for temporary storage
            let curr_list = &mut self.backrefs[0_usize];
            curr_list.clear();
            curr_list.extend(succ_iter);
            // compress and write immediately
            self.compressor
                .compress(curr_list, None, self.min_interval_length)?;
            let written_bits = self.compressor.write(
                &mut self.encoder,
                self.curr_node,
                None,
                self.min_interval_length,
            )?;
            self.curr_node += 1;
            self.oldest_unwritten += 1;
            self.stats.offsets_written_bits += self.offsets_writer.push(written_bits)? as u64;
            self.stats.written_bits += written_bits;
            self.stats.num_arcs += curr_list.len() as u64;
            return Ok(());
        }

        // Store successors in backrefs
        {
            let succ_vec = &mut self.backrefs[self.curr_node];
            succ_vec.clear();
            succ_vec.extend(succ_iter);
            // Shrink if over-allocated
            if succ_vec.capacity() > 4 * succ_vec.len() {
                let old_vec = std::mem::replace(succ_vec, Vec::with_capacity(2 * succ_vec.len()));
                succ_vec.extend(old_vec);
            }
        }

        // Update statistics
        let num_arcs = self.backrefs[self.curr_node].len();
        self.stats.num_arcs += num_arcs as u64;

        // Compute savings for this node
        self.compute_savings(self.curr_node)?;

        self.curr_node += 1;

        // If buffer is full, write oldest node
        if (self.curr_node - self.oldest_unwritten) > self.look_ahead {
            self.write_oldest_node()?;
        }

        Ok(())
    }

    /// Consume the compressor and return the statistics about compression.
    /// This writes all remaining nodes in the buffer.
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        // Write all remaining buffer nodes
        while (self.curr_node - self.oldest_unwritten) > 0 {
            self.write_oldest_node()?;
        }

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
    fn test_writer_cnr() -> anyhow::Result<()> {
        let cnr_2000 = BvGraphSeq::with_basename("../data/cnr-2000")
            .endianness::<BE>()
            .load()?;

        let tmp_dir = Builder::new().prefix("bvcompla_test").tempdir()?;
        let basename = tmp_dir.path().join("cnr-2000");
        BvCompLa::with_basename(&basename).comp_graph::<BE>(&cnr_2000)?;
        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;

        BvCompLa::with_basename(&basename).par_comp_graph::<BE>(&cnr_2000)?;

        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;
        Ok(())
    }
}
