/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use dsi_bitstream::{prelude::CodesStats, traits::BitSeek};

/// A struct that keeps track of how much bits each piece would take
/// using different codes for compression.
#[derive(Debug, Default)]
pub struct DecoderStats {
    /// The statistics for the outdegrees values
    pub outdegrees: CodesStats,
    /// The statistics for the reference_offset values
    pub reference_offsets: CodesStats,
    /// The statistics for the block_count values
    pub block_counts: CodesStats,
    /// The statistics for the blocks values
    pub blocks: CodesStats,
    /// The statistics for the interval_count values
    pub interval_counts: CodesStats,
    /// The statistics for the interval_start values
    pub interval_starts: CodesStats,
    /// The statistics for the interval_len values
    pub interval_lens: CodesStats,
    /// The statistics for the first_residual values
    pub first_residuals: CodesStats,
    /// The statistics for the residual values
    pub residuals: CodesStats,
}

impl DecoderStats {
    pub fn update(&mut self, rhs: &Self) {
        self.outdegrees.add(&rhs.outdegrees);
        self.reference_offsets.add(&rhs.reference_offsets);
        self.block_counts.add(&rhs.block_counts);
        self.blocks.add(&rhs.blocks);
        self.interval_counts.add(&rhs.interval_counts);
        self.interval_starts.add(&rhs.interval_starts);
        self.interval_lens.add(&rhs.interval_lens);
        self.first_residuals.add(&rhs.first_residuals);
        self.residuals.add(&rhs.residuals);
    }
}

impl std::ops::AddAssign<&Self> for DecoderStats {
    fn add_assign(&mut self, rhs: &Self) {
        self.update(rhs);
    }
}

impl core::iter::Sum for DecoderStats {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |mut acc, x| {
            acc.update(&x);
            acc
        })
    }
}

/// A wrapper over a generic [`Decode`] that keeps track of how much
/// bits each piece would take using different codes for compressions
pub struct StatsDecoder<D: Decode> {
    pub codes_reader: D,
    pub stats: DecoderStats,
}

impl<D: Decode> StatsDecoder<D> {
    /// Wrap a reader
    #[inline(always)]
    pub fn new(codes_reader: D, stats: DecoderStats) -> Self {
        Self {
            codes_reader,
            stats,
        }
    }
}

impl<D: Decode> BitSeek for StatsDecoder<D>
where
    D: BitSeek,
{
    type Error = <D as BitSeek>::Error;

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.codes_reader.bit_pos()
    }

    fn set_bit_pos(&mut self, bit_pos: u64) -> Result<(), Self::Error> {
        self.codes_reader.set_bit_pos(bit_pos)
    }
}

impl<D: Decode> Decode for StatsDecoder<D> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        self.stats
            .outdegrees
            .update(self.codes_reader.read_outdegree())
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        self.stats
            .reference_offsets
            .update(self.codes_reader.read_reference_offset())
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        self.stats
            .block_counts
            .update(self.codes_reader.read_block_count())
    }

    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        self.stats.blocks.update(self.codes_reader.read_block())
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        self.stats
            .interval_counts
            .update(self.codes_reader.read_interval_count())
    }

    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        self.stats
            .interval_starts
            .update(self.codes_reader.read_interval_start())
    }

    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        self.stats
            .interval_lens
            .update(self.codes_reader.read_interval_len())
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        self.stats
            .first_residuals
            .update(self.codes_reader.read_first_residual())
    }

    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        self.stats
            .residuals
            .update(self.codes_reader.read_residual())
    }
}
