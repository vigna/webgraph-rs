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

mod dec_dbg;
pub use dec_dbg::*;

mod dec_dyn;
pub use dec_dyn::*;

mod dec_stats;
pub use dec_stats::*;

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
/// a sum of traits.
impl<E: Endianness, T> CodeRead<E> for T where T: GammaRead<E> + DeltaRead<E> + ZetaRead<E> {}
/// Blanket implementation so we can consider [`CodeWrite`] just as an alias for
/// a sum of traits.
impl<E: Endianness, T> CodeWrite<E> for T where T: GammaWrite<E> + DeltaWrite<E> + ZetaWrite<E> {}

/// Methods to decode the component of a [`BVGraph`].
pub trait Decoder {
    fn read_outdegree(&mut self) -> u64;
    fn read_reference_offset(&mut self) -> u64;
    fn read_block_count(&mut self) -> u64;
    fn read_block(&mut self) -> u64;
    fn read_interval_count(&mut self) -> u64;
    fn read_interval_start(&mut self) -> u64;
    fn read_interval_len(&mut self) -> u64;
    fn read_first_residual(&mut self) -> u64;
    fn read_residual(&mut self) -> u64;
}

/// Methods to encode the component of a [`BVGraph`].
pub trait Encoder {
    type Error: Error + Send + Sync + 'static;
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_blocks(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}

pub trait MeasurableEncoder: Encoder {
    /// An associated (usually stateless) encoder that returns
    /// integers estimating the amount of space used by each
    /// operation of this measurable encoder.
    type Estimator: Encoder;
    /// Return an estimator for this measurable encoder.
    fn estimator(&self) -> Self::Estimator;
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
