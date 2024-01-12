/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::error::Error;

/// An object that can create code readers, this is done so that the builder can
/// own the data, and the readers can be created and thrown away freely
pub trait BVGraphCodesReaderBuilder {
    /// The type of the reader that we are building
    type Reader<'a>: BVGraphCodesReader + 'a
    where
        Self: 'a;

    /// Create a new reader at bit-offset `offset`
    fn get_reader(&self, offset: u64) -> Result<Self::Reader<'_>, Box<dyn Error>>;
}

/// The generic interface we need to skip codes
pub trait BVGraphCodesSkipper {
    /// skip a outdegree code
    fn skip_outdegree(&mut self);

    /// skip a reference offset code
    fn skip_reference_offset(&mut self);

    /// skip a block count code
    fn skip_block_count(&mut self);
    /// skip a block code
    fn skip_block(&mut self);

    /// skip a interval count code
    fn skip_interval_count(&mut self);
    /// skip a interval start code
    fn skip_interval_start(&mut self);
    /// skip a interval len code
    fn skip_interval_len(&mut self);

    /// skip a first residual code
    fn skip_first_residual(&mut self);
    /// skip a residual code
    fn skip_residual(&mut self);
}

/// The generic interface we need to read codes to decode a [`BVGraph`]
pub trait BVGraphCodesReader {
    /// read a outdegree code
    fn read_outdegree(&mut self) -> u64;

    /// read a reference offset code
    fn read_reference_offset(&mut self) -> u64;

    /// read a blocks count code
    fn read_block_count(&mut self) -> u64;
    /// read a block code
    fn read_blocks(&mut self) -> u64;

    /// read a interval count code
    fn read_interval_count(&mut self) -> u64;
    /// read a interval start code
    fn read_interval_start(&mut self) -> u64;
    /// read a interval len code
    fn read_interval_len(&mut self) -> u64;

    /// read a first residual code
    fn read_first_residual(&mut self) -> u64;
    /// read a residual code
    fn read_residual(&mut self) -> u64;
}

/// The generic interface we need to write codes to write a [`BVGraph`] to
/// a bitstream
pub trait BVGraphCodesWriter {
    type Error: Error + Send + Sync;
    /// A mock writer that does not write anything but returns how many bits
    /// this writer with this configuration would have written
    type MockWriter: BVGraphCodesWriter;
    /// Returns a mock writer that does not write anything.
    fn mock(&self) -> Self::MockWriter;

    /// Write `value` as a outdegree code and return the number of bits written
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error>;

    /// Write `value` as a reference offset code and return the number of bits written
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error>;

    /// Write `value` as a block count code and return the number of bits written
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error>;
    /// Write `value` as a block  code and return the number of bits written
    fn write_blocks(&mut self, value: u64) -> Result<usize, Self::Error>;

    /// Write `value` as a interval count code and return the number of bits written
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error>;
    /// Write `value` as a interval start code and return the number of bits written
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error>;
    /// Write `value` as a interval len code and return the number of bits written
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error>;

    /// Write `value` as a first residual code and return the number of bits written
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error>;
    /// Write `value` as a residual code and return the number of bits written
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error>;

    /// Consume the writer and call flush on the underlying writer
    fn flush(self) -> Result<(), Self::Error>;
}
