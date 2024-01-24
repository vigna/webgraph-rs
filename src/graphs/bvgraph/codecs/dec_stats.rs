/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use dsi_bitstream::prelude::CodesStats;

/// A struct that keeps track of how much bits each piece would take using
#[derive(Debug, Default)]
pub struct BVGraphCodesStats {
    /// The statistics for the outdegrees values
    pub outdegree: CodesStats,
    /// The statistics for the reference_offset values
    pub reference_offset: CodesStats,
    /// The statistics for the block_count values
    pub block_count: CodesStats,
    /// The statistics for the blocks values
    pub blocks: CodesStats,
    /// The statistics for the interval_count values
    pub interval_count: CodesStats,
    /// The statistics for the interval_start values
    pub interval_start: CodesStats,
    /// The statistics for the interval_len values
    pub interval_len: CodesStats,
    /// The statistics for the first_residual values
    pub first_residual: CodesStats,
    /// The statistics for the residual values
    pub residual: CodesStats,
}

/// A wrapper that keeps track of how much bits each piece would take using
/// different codes for compressions for a [`SequentialDecoderFactory`]
/// implementation and returns the stats.
pub struct StatsDecoderFactory<F: SequentialDecoderFactory> {
    factory: F,
}

impl<F> StatsDecoderFactory<F>
where
    F: SequentialDecoderFactory,
{
    /// Create a new builder
    pub fn new(factory: F) -> Self {
        Self { factory }
    }

    #[inline(always)]
    /// Consume the builder and return the inner reader
    pub fn unwrap(self) -> F {
        self.factory
    }
}

impl<F> From<F> for StatsDecoderFactory<F>
where
    F: SequentialDecoderFactory,
{
    #[inline(always)]
    fn from(value: F) -> Self {
        Self::new(value)
    }
}

impl<F> SequentialDecoderFactory for StatsDecoderFactory<F>
where
    F: SequentialDecoderFactory,
{
    type Decoder<'a> = StatsDecoder<F::Decoder<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>> {
        Ok(StatsDecoder::new(
            self.factory.new_decoder()?,
            BVGraphCodesStats::default(),
        ))
    }
}

/// A wrapper over a generic [`Decoder`] that keeps track of how much
/// bits each piece would take using different codes for compressions
pub struct StatsDecoder<D: Decoder> {
    codes_reader: D,
    stats: BVGraphCodesStats,
}

impl<D: Decoder> StatsDecoder<D> {
    /// Wrap a reader
    #[inline(always)]
    pub fn new(codes_reader: D, stats: BVGraphCodesStats) -> Self {
        Self {
            codes_reader,
            stats,
        }
    }

    /// Return the wrapped codes reader and the stats
    #[inline(always)]
    pub fn into_inner(self) -> BVGraphCodesStats {
        self.stats
    }
}

impl<'a, D: Decoder> Decoder for StatsDecoder<D> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        self.stats
            .outdegree
            .update(self.codes_reader.read_outdegree())
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        self.stats
            .reference_offset
            .update(self.codes_reader.read_reference_offset())
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        self.stats
            .block_count
            .update(self.codes_reader.read_block_count())
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> u64 {
        self.stats.blocks.update(self.codes_reader.read_blocks())
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        self.stats
            .interval_count
            .update(self.codes_reader.read_interval_count())
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        self.stats
            .interval_start
            .update(self.codes_reader.read_interval_start())
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        self.stats
            .interval_len
            .update(self.codes_reader.read_interval_len())
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        self.stats
            .first_residual
            .update(self.codes_reader.read_first_residual())
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        self.stats
            .residual
            .update(self.codes_reader.read_residual())
    }
}
