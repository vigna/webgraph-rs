use crate::prelude::*;
use anyhow::Result;
use dsi_bitstream::prelude::CodesStats;

/// A struct that keeps track of how much bits each piece would take using
#[derive(Debug, Default)]
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

pub struct CodesReaderStatsBuilder<WGCRB: WebGraphCodesReaderBuilder> {
    codes_reader_builder: WGCRB,
    pub stats: BVGraphCodesStats,
}

impl<WGCRB> CodesReaderStatsBuilder<WGCRB>
where
    WGCRB: WebGraphCodesReaderBuilder,
{
    pub fn new(codes_reader_builder: WGCRB) -> Self {
        Self {
            codes_reader_builder,
            stats: BVGraphCodesStats::default(),
        }
    }

    #[inline(always)]
    pub fn unwrap(self) -> WGCRB {
        self.codes_reader_builder
    }
}

impl<WGCRB> From<WGCRB> for CodesReaderStatsBuilder<WGCRB>
where
    WGCRB: WebGraphCodesReaderBuilder,
{
    #[inline(always)]
    fn from(value: WGCRB) -> Self {
        Self::new(value)
    }
}

impl<WGCRB> WebGraphCodesReaderBuilder for CodesReaderStatsBuilder<WGCRB>
where
    WGCRB: WebGraphCodesReaderBuilder,
{
    type Reader<'a> = CodesReaderStats<'a, WGCRB::Reader<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>> {
        Ok(CodesReaderStats::new(
            self.codes_reader_builder.get_reader(offset)?,
            &self.stats,
        ))
    }
}

/// A wrapper over a generic [`WebGraphCodesReader`] that keeps track of how much
/// bits each piece would take using different codes for compressions
pub struct CodesReaderStats<'a, WGCR: WebGraphCodesReader> {
    codes_reader: WGCR,
    stats: &'a BVGraphCodesStats,
}

impl<'a, WGCR: WebGraphCodesReader> CodesReaderStats<'a, WGCR> {
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

impl<'a, WGCR: WebGraphCodesReader + WebGraphCodesSkipper> WebGraphCodesSkipper
    for CodesReaderStats<'a, WGCR>
{
    #[inline(always)]
    fn skip_outdegrees(&mut self, n: usize) {
        self.codes_reader.skip_outdegrees(n)
    }
    #[inline(always)]
    fn skip_reference_offsets(&mut self, n: usize) {
        self.codes_reader.skip_reference_offsets(n)
    }
    #[inline(always)]
    fn skip_block_counts(&mut self, n: usize) {
        self.codes_reader.skip_block_counts(n)
    }
    #[inline(always)]
    fn skip_blocks(&mut self, n: usize) {
        self.codes_reader.skip_blocks(n)
    }
    #[inline(always)]
    fn skip_interval_counts(&mut self, n: usize) {
        self.codes_reader.skip_interval_counts(n)
    }
    #[inline(always)]
    fn skip_interval_starts(&mut self, n: usize) {
        self.codes_reader.skip_interval_starts(n)
    }
    #[inline(always)]
    fn skip_interval_lens(&mut self, n: usize) {
        self.codes_reader.skip_interval_lens(n)
    }
    #[inline(always)]
    fn skip_first_residuals(&mut self, n: usize) {
        self.codes_reader.skip_first_residuals(n)
    }
    #[inline(always)]
    fn skip_residuals(&mut self, n: usize) {
        self.codes_reader.skip_residuals(n)
    }
}
