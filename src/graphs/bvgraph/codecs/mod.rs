/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

pub mod factories;
pub use factories::*;

mod dec_const;
pub use dec_const::*;

mod dec_dyn;
pub use dec_dyn::*;

mod enc_const;
pub use enc_const::*;

mod enc_dyn;
pub use enc_dyn::*;

use dsi_bitstream::{
    codes::{DeltaRead, DeltaWrite, GammaRead, GammaWrite, ZetaRead, ZetaWrite},
    traits::Endianness,
};

use std::error::Error;

/// A trait combining the codes used by [`DynCodesDecoder`] and [`ConstCodesDecoder`].
pub trait CodeRead<E: Endianness>: GammaRead<E> + DeltaRead<E> + ZetaRead<E> {}
/// A trait combining the codes used by [`DynCodesEncoder`] and [`ConstCodesEncoder`].
pub trait CodeWrite<E: Endianness>: GammaWrite<E> + DeltaWrite<E> + ZetaWrite<E> {}

/// Blanket implementation so we can consider [`CodeRead`] just as an alias for
/// a sum of traits
impl<E: Endianness, T> CodeRead<E> for T where T: GammaRead<E> + DeltaRead<E> + ZetaRead<E> {}
/// Blanket implementation so we can consider [`CodeWrite`] just as an alias for
/// a sum of traits
impl<E: Endianness, T> CodeWrite<E> for T where T: GammaWrite<E> + DeltaWrite<E> + ZetaWrite<E> {}

/// The generic interface we need to read codes to decode a [`BVGraph`]
pub trait Decoder {
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
pub trait Encoder {
    type Error: Error + Send + Sync;
    /// A mock writer that does not write anything but returns how many bits
    /// this writer with this configuration would have written
    type MockEncoder: Encoder;
    /// Returns a mock writer that does not write anything.
    fn mock(&self) -> Self::MockEncoder;

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

    /// Call flush on the underlying writer
    fn flush(&mut self) -> Result<(), Self::Error>;
}

/// A trait providing decoders with random access.
pub trait RandomAccessDecoderFactory {
    /// The type of the reader that we are building
    type Decoder<'a>: Decoder + 'a
    where
        Self: 'a;

    /// Create a new reader starting at the given node.
    fn new_decoder(&self, node: usize) -> anyhow::Result<Self::Decoder<'_>>;
}

/// A trait providing decoders on the whole graph.
pub trait SequentialDecoderFactory {
    /// The type xof the reader that we are building
    type Decoder<'a>: Decoder + 'a
    where
        Self: 'a;

    /// Create a new reader starting at the given node.
    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>>;
}
