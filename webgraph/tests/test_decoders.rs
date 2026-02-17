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
fn test_decoder_stats_update() {
    use webgraph::graphs::bvgraph::{DecoderStats, StatsDecoder};

    let mock = MockDecoder::new(10);
    let mut dec = StatsDecoder::new(mock, DecoderStats::default());
    dec.read_outdegree();
    dec.read_residual();
    dec.read_residual();

    let mut combined = DecoderStats::default();
    combined.update(&dec.stats);
    combined.update(&dec.stats);
    assert_eq!(combined.outdegrees.total, 2);
    assert_eq!(combined.residuals.total, 4);
    assert_eq!(combined.blocks.total, 0);
}

#[test]
fn test_decoder_stats_add_assign() {
    use webgraph::graphs::bvgraph::{DecoderStats, StatsDecoder};

    let mock = MockDecoder::new(10);
    let mut dec = StatsDecoder::new(mock, DecoderStats::default());
    dec.read_outdegree();
    dec.read_block();
    dec.read_block();
    dec.read_block();

    let mut acc = DecoderStats::default();
    acc += &dec.stats;
    acc += &dec.stats;
    assert_eq!(acc.outdegrees.total, 2);
    assert_eq!(acc.blocks.total, 6);
    assert_eq!(acc.residuals.total, 0);
}

#[test]
fn test_decoder_stats_sum() {
    use webgraph::graphs::bvgraph::{DecoderStats, StatsDecoder};

    let mock1 = MockDecoder::new(5);
    let mut dec1 = StatsDecoder::new(mock1, DecoderStats::default());
    dec1.read_outdegree();

    let mock2 = MockDecoder::new(7);
    let mut dec2 = StatsDecoder::new(mock2, DecoderStats::default());
    dec2.read_outdegree();
    dec2.read_residual();

    let summed: DecoderStats = vec![dec1.stats, dec2.stats].into_iter().sum();
    assert_eq!(summed.outdegrees.total, 2);
    assert_eq!(summed.residuals.total, 1);
    assert_eq!(summed.interval_counts.total, 0);
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

    // gamma(n) = 2*floor(log2(n+1))+1 bits; unary(n) = n+1 bits; zeta3 from regression
    assert_eq!(est.write_outdegree(5).unwrap(), 5); // gamma(5) = 2*2+1
    assert_eq!(est.write_reference_offset(3).unwrap(), 4); // unary(3) = 4
    assert_eq!(est.write_block_count(2).unwrap(), 3); // gamma(2) = 2*1+1
    assert_eq!(est.write_block(1).unwrap(), 3); // gamma(1) = 2*1+1
    assert_eq!(est.write_interval_count(4).unwrap(), 5); // gamma(4) = 2*2+1
    assert_eq!(est.write_interval_start(10).unwrap(), 7); // gamma(10) = 2*3+1
    assert_eq!(est.write_interval_len(3).unwrap(), 5); // gamma(3) = 2*2+1
    assert_eq!(est.write_first_residual(7).unwrap(), 7); // zeta3(7)
    assert_eq!(est.write_residual(15).unwrap(), 8); // zeta3(15)
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
    // gamma(0) = 2*floor(log2(1))+1 = 1 bit
    assert_eq!(est.write_outdegree(0).unwrap(), 1);
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
