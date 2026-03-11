/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use super::OffsetsWriter;
use super::bvcomp::{CompStats, Compressor};
use crate::prelude::*;
use lender::prelude::*;

/// An entry in the DP state table.
#[derive(Clone, Copy, Debug)]
struct DpEntry {
    /// Cumulative savings from the start.
    total_savings: u64,
    /// The delta chosen for the node that produced this state.
    chosen_delta: u8,
    /// The predecessor state (for backtracking).
    prev_state: u32,
}

/// A BvGraph compressor using exact sliding-window Viterbi DP.
///
/// This compressor finds the globally optimal reference assignment by
/// modeling the problem as finding a maximum-weight depth-bounded forest.
/// Each node can reference one of its `w` predecessors (compression window),
/// forming trees whose height must not exceed `R` (max_ref_count).
///
/// The state is the packed depths of the last `w` nodes, and the DP
/// transitions try all possible reference deltas for each new node.
/// A fixed-lag commitment strategy ensures that nodes are committed
/// once their influence horizon has passed, yielding the exact optimum.
#[derive(Debug)]
pub struct BvCompDP<E, W: Write> {
    /// Ring buffer of successor lists (capacity: w + lag + 1).
    backrefs: CircularBuffer<Vec<usize>>,
    /// Ring buffer of savings vectors (capacity: lag + 1).
    /// savings[node][delta-1] = bits saved by using delta instead of no reference.
    savings: CircularBuffer<Vec<u64>>,
    /// Ring buffer of DP snapshots (capacity: lag + 1).
    /// dp_states[node] maps packed depth state -> DpEntry.
    dp_states: CircularBuffer<HashMap<u32, DpEntry>>,
    /// Deltas committed by the DP but not yet written.
    committed_deltas: Vec<(usize, usize)>,
    /// Reusable compressor for computing costs and writing.
    compressor: Compressor,
    /// The bitstream encoder.
    encoder: E,
    /// The offset writer.
    pub offsets_writer: OffsetsWriter<W>,
    /// The compression window size.
    compression_window: usize,
    /// The maximum reference chain depth.
    max_ref_count: usize,
    /// The minimum interval length for intervalization.
    min_interval_length: usize,
    /// The commitment lag (R * w).
    lag: usize,
    /// Bits per depth field in the packed state.
    bits_per_depth: u32,
    /// Mask for a single depth field.
    depth_mask: u32,
    /// The next node to be pushed.
    curr_node: usize,
    /// The oldest uncommitted node.
    frontier: usize,
    /// The oldest unwritten node.
    oldest_unwritten: usize,
    /// The first node (for parallel compression).
    start_node: usize,
    /// Compression statistics.
    stats: CompStats,
}

impl BvCompDP<(), std::io::Sink> {
    /// Convenience method returning a [`BvCompConfig`] with
    /// settings suitable for the DP compressor.
    pub fn with_basename(basename: impl AsRef<Path>) -> BvCompConfig {
        BvCompConfig::new(basename)
    }
}

impl<E: EncodeAndEstimate, W: Write> BvCompDP<E, W> {
    /// This value for `min_interval_length` implies that no intervalization will be performed.
    pub const NO_INTERVALS: usize = Compressor::NO_INTERVALS;

    /// Creates a new BvGraph compressor with exact DP reference selection.
    pub fn new(
        encoder: E,
        offsets_writer: OffsetsWriter<W>,
        compression_window: usize,
        max_ref_count: usize,
        min_interval_length: usize,
        start_node: usize,
    ) -> Self {
        let lag = max_ref_count * compression_window;
        let bits_per_depth = if max_ref_count == 0 {
            1
        } else {
            (max_ref_count as u32 + 1).next_power_of_two().trailing_zeros().max(1)
        };
        let depth_mask = (1u32 << bits_per_depth) - 1;

        BvCompDP {
            backrefs: CircularBuffer::new(compression_window + lag + 2),
            savings: CircularBuffer::new(lag + 2),
            dp_states: CircularBuffer::new(lag + 2),
            committed_deltas: Vec::new(),
            compressor: Compressor::new(),
            encoder,
            offsets_writer,
            compression_window,
            max_ref_count,
            min_interval_length,
            lag,
            bits_per_depth,
            depth_mask,
            curr_node: start_node,
            frontier: start_node,
            oldest_unwritten: start_node,
            start_node,
            stats: CompStats::default(),
        }
    }

    /// Extract the depth at position `pos` (0 = newest, w-1 = oldest) from a packed state.
    #[inline]
    fn get_depth(&self, state: u32, pos: usize) -> u8 {
        ((state >> (pos as u32 * self.bits_per_depth)) & self.depth_mask) as u8
    }

    /// Shift state left by one depth slot and set the newest depth.
    #[inline]
    fn shift_and_set(&self, state: u32, new_depth: u8) -> u32 {
        let total_bits = self.compression_window as u32 * self.bits_per_depth;
        let mask = if total_bits >= 32 { u32::MAX } else { (1u32 << total_bits) - 1 };
        ((state << self.bits_per_depth) & mask) | (new_depth as u32 & self.depth_mask)
    }

    /// Compute compression savings for a node against all valid references.
    fn compute_savings(&mut self, node_id: usize) -> anyhow::Result<()> {
        let curr_list = &self.backrefs[node_id];
        let savings = &mut self.savings[node_id];
        savings.clear();

        // Cost with no reference (delta = 0) - baseline
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
            let ref_list = &self.backrefs[node_id - delta];

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
            savings.push(base_cost.saturating_sub(cost));
        }

        Ok(())
    }

    /// Run one DP step for node at `node_id`.
    fn dp_step(&mut self, node_id: usize) {
        let w = self.compression_window;

        // Get the previous DP states (from node_id - 1).
        // If this is the first node, start with a single state: all depths = 0.
        let prev_states = if node_id == self.start_node {
            let mut init = HashMap::new();
            init.insert(0u32, DpEntry {
                total_savings: 0,
                chosen_delta: 0,
                prev_state: 0,
            });
            init
        } else {
            // Clone to avoid borrow issues
            self.dp_states[node_id - 1].clone()
        };

        let mut new_states: HashMap<u32, DpEntry> = HashMap::new();
        let savings = &self.savings[node_id];
        let max_delta = w.min(node_id - self.start_node);

        for (&state, &entry) in &prev_states {
            // Option 1: no reference (delta = 0), depth becomes 0
            let new_state = self.shift_and_set(state, 0);
            let new_entry = DpEntry {
                total_savings: entry.total_savings,
                chosen_delta: 0,
                prev_state: state,
            };
            match new_states.entry(new_state) {
                std::collections::hash_map::Entry::Vacant(e) => { e.insert(new_entry); }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if new_entry.total_savings > e.get().total_savings {
                        e.insert(new_entry);
                    }
                }
            }

            // Option 2: reference delta d (1..=max_delta)
            for d in 1..=max_delta {
                let saving = if d <= savings.len() { savings[d - 1] } else { 0 };
                if saving == 0 {
                    continue;
                }

                // The referenced node is at position (w - d) in the state vector.
                // Position 0 = newest (the node added just before this DP step).
                // Position w-1 = oldest.
                // Delta d means referencing the node d positions back.
                // In the previous state, position 0 is node_id-1, position 1 is node_id-2, etc.
                // So delta d references position (d-1) in the previous state.
                let ref_pos = d - 1;
                if ref_pos >= w {
                    continue;
                }
                let ref_depth = self.get_depth(state, ref_pos);
                if ref_depth >= self.max_ref_count as u8 {
                    continue;
                }

                let new_depth = ref_depth + 1;
                let new_state = self.shift_and_set(state, new_depth);
                let new_entry = DpEntry {
                    total_savings: entry.total_savings + saving,
                    chosen_delta: d as u8,
                    prev_state: state,
                };
                match new_states.entry(new_state) {
                    std::collections::hash_map::Entry::Vacant(e) => { e.insert(new_entry); }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if new_entry.total_savings > e.get().total_savings {
                            e.insert(new_entry);
                        }
                    }
                }
            }
        }

        self.dp_states[node_id] = new_states;
    }

    /// Commit the oldest uncommitted node by tracing back from the best current state.
    fn commit_oldest(&mut self) {
        let node_to_commit = self.frontier;
        let steps_back = self.curr_node - 1 - node_to_commit;

        // Find the best state at the current frontier (curr_node - 1)
        let latest_node = self.curr_node - 1;
        let best_state = self.dp_states[latest_node]
            .iter()
            .max_by_key(|(_, e)| e.total_savings)
            .map(|(&s, _)| s)
            .unwrap_or(0);

        // Trace back to find the delta for node_to_commit
        let mut state = best_state;
        let mut deltas: Vec<u8> = Vec::with_capacity(steps_back + 1);

        // Collect deltas from latest_node back to node_to_commit
        for node in (node_to_commit..=latest_node).rev() {
            let entry = self.dp_states[node]
                .get(&state)
                .copied()
                .unwrap_or(DpEntry {
                    total_savings: 0,
                    chosen_delta: 0,
                    prev_state: 0,
                });
            deltas.push(entry.chosen_delta);
            state = entry.prev_state;
        }

        // deltas is in reverse order: [latest, ..., node_to_commit]
        // The last element is the delta for node_to_commit
        let delta = *deltas.last().unwrap_or(&0) as usize;

        self.committed_deltas.push((node_to_commit, delta));
        self.frontier += 1;
    }

    /// Write all committed but unwritten nodes.
    fn write_committed(&mut self) -> anyhow::Result<()> {
        // Drain committed deltas
        let deltas: Vec<(usize, usize)> = std::mem::take(&mut self.committed_deltas);

        for (node_id, delta) in deltas {
            assert_eq!(node_id, self.oldest_unwritten);
            self.write_node(node_id, delta)?;
            self.oldest_unwritten += 1;
        }

        Ok(())
    }

    /// Write a single node with the given reference delta.
    fn write_node(&mut self, node_id: usize, delta: usize) -> anyhow::Result<()> {
        let curr_list = &self.backrefs[node_id];
        let ref_list: Option<&[usize]> = if delta > 0 {
            Some(&self.backrefs[node_id - delta])
        } else {
            None
        };

        self.compressor
            .compress(curr_list, ref_list, self.min_interval_length)?;
        let written_bits = self.compressor.write(
            &mut self.encoder,
            node_id,
            Some(delta),
            self.min_interval_length,
        )?;

        self.stats.written_bits += written_bits;
        self.stats.offsets_written_bits += self.offsets_writer.push(written_bits)? as u64;

        Ok(())
    }

    /// Push a new node to the compressor.
    pub fn push<I: IntoIterator<Item = usize>>(&mut self, succ_iter: I) -> anyhow::Result<()> {
        self.stats.num_nodes += 1;

        // Handle degenerate case: no compression window
        if self.compression_window == 0 {
            let curr_list = &mut self.backrefs[0_usize];
            curr_list.clear();
            curr_list.extend(succ_iter);
            self.compressor
                .compress(curr_list, None, self.min_interval_length)?;
            let written_bits = self.compressor.write(
                &mut self.encoder,
                self.curr_node,
                None,
                self.min_interval_length,
            )?;
            self.stats.num_arcs += curr_list.len() as u64;
            self.curr_node += 1;
            self.frontier += 1;
            self.oldest_unwritten += 1;
            self.stats.offsets_written_bits += self.offsets_writer.push(written_bits)? as u64;
            self.stats.written_bits += written_bits;
            return Ok(());
        }

        // Store successors
        {
            let succ_vec = &mut self.backrefs[self.curr_node];
            succ_vec.clear();
            succ_vec.extend(succ_iter);
            if succ_vec.capacity() > 4 * succ_vec.len() {
                let old_vec = std::mem::replace(succ_vec, Vec::with_capacity(2 * succ_vec.len()));
                succ_vec.extend(old_vec);
            }
        }

        let num_arcs = self.backrefs[self.curr_node].len();
        self.stats.num_arcs += num_arcs as u64;

        // Compute savings for this node
        self.compute_savings(self.curr_node)?;

        // Run DP step
        self.dp_step(self.curr_node);

        self.curr_node += 1;

        // If we've passed the lag, commit the oldest node
        if self.curr_node - self.frontier > self.lag {
            self.commit_oldest();
            self.write_committed()?;
        }

        Ok(())
    }

    /// Consume the compressor and return compression statistics.
    pub fn flush(mut self) -> anyhow::Result<CompStats> {
        // Commit and write all remaining nodes
        while self.frontier < self.curr_node {
            self.commit_oldest();
        }
        self.write_committed()?;

        self.encoder.flush()?;
        self.offsets_writer.flush()?;
        Ok(self.stats)
    }

    /// Push all nodes from a lender.
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

        let tmp_dir = Builder::new().prefix("bvcompdp_test").tempdir()?;
        let basename = tmp_dir.path().join("cnr-2000");

        BvCompDP::with_basename(&basename).comp_graph::<BE>(&cnr_2000)?;

        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;

        BvCompDP::with_basename(&basename).par_comp_graph::<BE>(&cnr_2000)?;
        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;
        Ok(())
    }

    fn test_compression(
        compression_window: usize,
        min_interval_length: usize,
    ) -> anyhow::Result<()> {
        let cnr_2000 = BvGraphSeq::with_basename("../data/cnr-2000").load()?;

        let tmp_dir = Builder::new().prefix("bvcompdp_test").tempdir()?;
        let basename = tmp_dir.path().join("cnr-2000");

        BvCompDP::with_basename(&basename)
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
