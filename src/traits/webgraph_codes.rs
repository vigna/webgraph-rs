use anyhow::Result;
use dsi_bitstream::prelude::*;

// A trait combining the codes used by BVGraph when reading.
pub trait ReadCodes<E: Endianness>: GammaRead<E> + DeltaRead<E> + ZetaRead<E> {}
// A trait combining the codes used by BVGraph when writing.
pub trait WriteCodes<E: Endianness>: GammaWrite<E> + DeltaWrite<E> + ZetaWrite<E> {}

/// Blanket implementation so we can consider [`ReadCodes`] just as an alias for
/// a sum of traits
impl<E: Endianness, T> ReadCodes<E> for T where T: GammaRead<E> + DeltaRead<E> + ZetaRead<E> {}
/// Blanket implementation so we can consider [`WriteCodes`] just as an alias for
/// a sum of traits
impl<E: Endianness, T> WriteCodes<E> for T where T: GammaWrite<E> + DeltaWrite<E> + ZetaWrite<E> {}

/// An object that can create code readers, this is done so that the builder can
/// own the data, and the readers can be created and thrown away freely
pub trait WebGraphCodesReaderBuilder {
    type Reader<'a>: WebGraphCodesReader + BitSeek + 'a
    where
        Self: 'a;

    /// Create a new reader at bit-offset `offset`
    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>>;
}

pub trait WebGraphCodesReader {
    fn read_outdegree(&mut self) -> u64;

    // node reference
    fn read_reference_offset(&mut self) -> u64;

    // run length reference copy
    fn read_block_count(&mut self) -> u64;
    fn read_blocks(&mut self) -> u64;

    // intervallizzation
    fn read_interval_count(&mut self) -> u64;
    fn read_interval_start(&mut self) -> u64;
    fn read_interval_len(&mut self) -> u64;

    // extra nodes
    fn read_first_residual(&mut self) -> u64;
    fn read_residual(&mut self) -> u64;
}

pub trait WebGraphCodesWriter {
    type MockWriter: WebGraphCodesWriter;
    /// Returns a mock writer that does not write anything.
    fn mock(&self) -> Self::MockWriter;

    fn write_outdegree(&mut self, value: u64) -> Result<usize>;

    // node reference
    fn write_reference_offset(&mut self, value: u64) -> Result<usize>;

    // run length reference copy
    fn write_block_count(&mut self, value: u64) -> Result<usize>;
    fn write_blocks(&mut self, value: u64) -> Result<usize>;

    // intervallizzation
    fn write_interval_count(&mut self, value: u64) -> Result<usize>;
    fn write_interval_start(&mut self, value: u64) -> Result<usize>;
    fn write_interval_len(&mut self, value: u64) -> Result<usize>;

    // extra nodes
    fn write_first_residual(&mut self, value: u64) -> Result<usize>;
    fn write_residual(&mut self, value: u64) -> Result<usize>;

    fn flush(self) -> Result<()>;
}
