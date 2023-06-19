use crate::prelude::*;
use dsi_bitstream::prelude::CodesStats;

/// A struct that keeps track of how much bits each piece would take using
#[derive(Debug, Clone, Copy, Default)]
pub struct BVGraphCodesStats {
    pub outdegree: CodesStats,
    pub reference_offset: CodesStats,
    pub block_count: CodesStats,
    pub blocks: CodesStats,
    pub interval_count: CodesStats,
    pub interval_start: CodesStats,
    pub interval_len: CodesStats,
    pub first_residual: CodesStats,
    pub residual: CodesStats,
}

/// A wrapper over a generic [`WebGraphCodesReader`] that keeps track of how much
/// bits each piece would take using different codes for compressions
pub struct CodesReaderStats<'a, WGCR: WebGraphCodesReader> {
    codes_reader: WGCR,
    stats: &'a mut BVGraphCodesStats,
}

impl<'a, WGCR: WebGraphCodesReader> CodesReaderStats<'a, WGCR> {
    /// Wrap a reader
    pub fn new(codes_reader: WGCR, stats: &'a mut BVGraphCodesStats) -> Self {
        Self {
            codes_reader,
            stats,
        }
    }

    /// Return the wrapped codes reader and the stats
    pub fn unwrap(self) -> WGCR {
        self.codes_reader
    }
}

impl<'a, WGCR: WebGraphCodesReader> WebGraphCodesReader for CodesReaderStats<'a, WGCR> {
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