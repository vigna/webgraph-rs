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
///
/// The statistics can be updated in a concurrent way using atomic operations.
/// So this struct can be used in a parallel compression scenario but
/// you might want to have finished reading the graph before looking at the
/// statistics.
pub struct CodesReaderStatsBuilder<SDF: SequentialDecoderFactory> {
    codes_reader_builder: SDF,
    /// The statistics for the codes
    pub stats: BVGraphCodesStats,
}

impl<SDF> CodesReaderStatsBuilder<SDF>
where
    SDF: SequentialDecoderFactory,
{
    /// Create a new builder
    pub fn new(codes_reader_builder: SDF) -> Self {
        Self {
            codes_reader_builder,
            stats: BVGraphCodesStats::default(),
        }
    }

    #[inline(always)]
    /// Consume the builder and return the inner reader
    pub fn unwrap(self) -> SDF {
        self.codes_reader_builder
    }
}

impl<SDF> From<SDF> for CodesReaderStatsBuilder<SDF>
where
    SDF: SequentialDecoderFactory,
{
    #[inline(always)]
    fn from(value: SDF) -> Self {
        Self::new(value)
    }
}

impl<SDF> SequentialDecoderFactory for CodesReaderStatsBuilder<SDF>
where
    SDF: SequentialDecoderFactory,
{
    type Decoder<'a> = CodesReaderStats<'a, SDF::Decoder<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>> {
        Ok(CodesReaderStats::new(
            self.codes_reader_builder.new_decoder()?,
            &self.stats,
        ))
    }
}

/// A wrapper over a generic [`BVGraphCodesReader`] that keeps track of how much
/// bits each piece would take using different codes for compressions
pub struct CodesReaderStats<'a, WGCR: Decoder> {
    codes_reader: WGCR,
    stats: &'a BVGraphCodesStats,
}

impl<'a, WGCR: Decoder> CodesReaderStats<'a, WGCR> {
    /// Wrap a reader
    #[inline(always)]
    pub fn new(codes_reader: WGCR, stats: &'a BVGraphCodesStats) -> Self {
        Self {
            codes_reader,
            stats,
        }
    }

    /// Return the wrapped codes reader and the stats
    #[inline(always)]
    pub fn unwrap(self) -> WGCR {
        self.codes_reader
    }
}

impl<'a, WGCR: Decoder> Decoder for CodesReaderStats<'a, WGCR> {
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
