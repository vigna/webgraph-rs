/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph::prelude::*;

struct MockDecoder {
    value: u64,
}

impl MockDecoder {
    fn new(value: u64) -> Self {
        Self { value }
    }
}

impl webgraph::graphs::bvgraph::Decode for MockDecoder {
    fn read_outdegree(&mut self) -> u64 {
        self.value
    }
    fn read_reference_offset(&mut self) -> u64 {
        self.value
    }
    fn read_block_count(&mut self) -> u64 {
        self.value
    }
    fn read_block(&mut self) -> u64 {
        self.value
    }
    fn read_interval_count(&mut self) -> u64 {
        self.value
    }
    fn read_interval_start(&mut self) -> u64 {
        self.value
    }
    fn read_interval_len(&mut self) -> u64 {
        self.value
    }
    fn read_first_residual(&mut self) -> u64 {
        self.value
    }
    fn read_residual(&mut self) -> u64 {
        self.value
    }
}

#[test]
fn test_decoder_stats_default_and_update() {
    let mut stats1 = DecoderStats::default();
    let stats2 = DecoderStats::default();
    stats1.update(&stats2);
}

#[test]
fn test_decoder_stats_add_assign() {
    let mut stats1 = DecoderStats::default();
    let stats2 = DecoderStats::default();
    stats1 += &stats2;
}

#[test]
fn test_decoder_stats_sum() {
    let stats_vec = vec![DecoderStats::default(), DecoderStats::default()];
    let _summed: DecoderStats = stats_vec.into_iter().sum();
}

#[test]
fn test_stats_decoder_all_read_methods() {
    use webgraph::graphs::bvgraph::{DecoderStats, StatsDecoder};

    let mock = MockDecoder::new(42);
    let mut stats_dec = StatsDecoder::new(mock, DecoderStats::default());

    assert_eq!(stats_dec.read_outdegree(), 42);
    assert_eq!(stats_dec.read_reference_offset(), 42);
    assert_eq!(stats_dec.read_block_count(), 42);
    assert_eq!(stats_dec.read_block(), 42);
    assert_eq!(stats_dec.read_interval_count(), 42);
    assert_eq!(stats_dec.read_interval_start(), 42);
    assert_eq!(stats_dec.read_interval_len(), 42);
    assert_eq!(stats_dec.read_first_residual(), 42);
    assert_eq!(stats_dec.read_residual(), 42);

    // Stats should have been updated for each field
    assert_eq!(stats_dec.stats.outdegrees.total, 1);
    assert_eq!(stats_dec.stats.reference_offsets.total, 1);
    assert_eq!(stats_dec.stats.block_counts.total, 1);
    assert_eq!(stats_dec.stats.blocks.total, 1);
    assert_eq!(stats_dec.stats.interval_counts.total, 1);
    assert_eq!(stats_dec.stats.interval_starts.total, 1);
    assert_eq!(stats_dec.stats.interval_lens.total, 1);
    assert_eq!(stats_dec.stats.first_residuals.total, 1);
    assert_eq!(stats_dec.stats.residuals.total, 1);
}

#[test]
fn test_stats_decoder_update_accumulation() {
    use webgraph::graphs::bvgraph::{DecoderStats, StatsDecoder};

    let mock = MockDecoder::new(10);
    let mut stats_dec = StatsDecoder::new(mock, DecoderStats::default());

    // Call multiple times to accumulate stats
    for _ in 0..5 {
        stats_dec.read_outdegree();
        stats_dec.read_residual();
    }

    assert_eq!(stats_dec.stats.outdegrees.total, 5);
    assert_eq!(stats_dec.stats.residuals.total, 5);

    // Test that stats can be extracted and combined
    let mut combined = DecoderStats::default();
    combined.update(&stats_dec.stats);
    assert_eq!(combined.outdegrees.total, 5);
}

#[test]
fn test_debug_decoder_all_methods() {
    use webgraph::graphs::bvgraph::DebugDecoder;

    let mock = MockDecoder::new(7);
    let mut dbg_dec = DebugDecoder::new(mock);

    // All methods print to stderr and delegate to inner decoder
    assert_eq!(dbg_dec.read_outdegree(), 7);
    assert_eq!(dbg_dec.read_reference_offset(), 7);
    assert_eq!(dbg_dec.read_block_count(), 7);
    assert_eq!(dbg_dec.read_block(), 7);
    assert_eq!(dbg_dec.read_interval_count(), 7);
    assert_eq!(dbg_dec.read_interval_start(), 7);
    assert_eq!(dbg_dec.read_interval_len(), 7);
    assert_eq!(dbg_dec.read_first_residual(), 7);
    assert_eq!(dbg_dec.read_residual(), 7);
}

#[test]
fn test_const_codes_estimator_all_methods() {
    use dsi_bitstream::dispatch::code_consts;
    use webgraph::graphs::bvgraph::{ConstCodesEstimator, Encode};

    let mut est: ConstCodesEstimator<
        { code_consts::GAMMA },
        { code_consts::UNARY },
        { code_consts::GAMMA },
        { code_consts::GAMMA },
        { code_consts::ZETA3 },
    > = ConstCodesEstimator::new();

    // All write methods return Ok(bit_length) for the given value
    let outdeg = est.write_outdegree(5).unwrap();
    assert!(outdeg > 0);
    let refoff = est.write_reference_offset(3).unwrap();
    assert!(refoff > 0);
    let bc = est.write_block_count(2).unwrap();
    assert!(bc > 0);
    let bl = est.write_block(1).unwrap();
    assert!(bl > 0);
    let ic = est.write_interval_count(4).unwrap();
    assert!(ic > 0);
    let is = est.write_interval_start(10).unwrap();
    assert!(is > 0);
    let il = est.write_interval_len(3).unwrap();
    assert!(il > 0);
    let fr = est.write_first_residual(7).unwrap();
    assert!(fr > 0);
    let r = est.write_residual(15).unwrap();
    assert!(r > 0);
    let f = est.flush().unwrap();
    assert_eq!(f, 0);

    // start_node and end_node return 0
    assert_eq!(est.start_node(0).unwrap(), 0);
    assert_eq!(est.end_node(0).unwrap(), 0);
}

#[test]
fn test_const_codes_estimator_zero_values() {
    use dsi_bitstream::dispatch::code_consts;
    use webgraph::graphs::bvgraph::{ConstCodesEstimator, Encode};

    let mut est: ConstCodesEstimator<
        { code_consts::GAMMA },
        { code_consts::UNARY },
        { code_consts::GAMMA },
        { code_consts::GAMMA },
        { code_consts::ZETA3 },
    > = ConstCodesEstimator::new();
    // Gamma encoding of 0 should be 1 bit
    let bits = est.write_outdegree(0).unwrap();
    assert!(bits > 0);
}

#[test]
fn test_converter_decode_all_methods() {
    use dsi_bitstream::dispatch::code_consts;
    use webgraph::graphs::bvgraph::{ConstCodesEstimator, Decode};
    use webgraph::utils::Converter;

    let decoder = MockDecoder::new(3);
    let encoder: ConstCodesEstimator<
        { code_consts::GAMMA },
        { code_consts::UNARY },
        { code_consts::GAMMA },
        { code_consts::GAMMA },
        { code_consts::ZETA3 },
    > = ConstCodesEstimator::new();
    let mut conv = Converter {
        decoder,
        encoder,
        offset: 0,
    };

    // Exercise all Decode methods — each reads from decoder, writes to encoder
    assert_eq!(conv.read_outdegree(), 3);
    assert!(conv.offset > 0);

    let prev_offset = conv.offset;
    assert_eq!(conv.read_reference_offset(), 3);
    assert!(conv.offset > prev_offset);

    assert_eq!(conv.read_block_count(), 3);
    assert_eq!(conv.read_block(), 3);
    assert_eq!(conv.read_interval_count(), 3);
    assert_eq!(conv.read_interval_start(), 3);
    assert_eq!(conv.read_interval_len(), 3);
    assert_eq!(conv.read_first_residual(), 3);
    assert_eq!(conv.read_residual(), 3);

    // num_of_residuals just delegates
    conv.num_of_residuals(5);
}

