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

use std::error::Error;

/// Methods to decode the component of a [`super::BvGraph`] or [`super::BvGraphSeq`].
pub trait Decode {
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

use impl_tools::autoimpl;

/// Methods to encode the component of a [`super::BvGraph`] or [`super::BvGraphSeq`].
#[autoimpl(for<T: trait + ?Sized> &mut T, Box<T>)]
pub trait Encode {
    type Error: Error + Send + Sync + 'static;
    fn start_node(&mut self, node: usize) -> Result<usize, Self::Error>;
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_block(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error>;
    fn flush(&mut self) -> Result<usize, Self::Error>;
    fn end_node(&mut self, node: usize) -> Result<usize, Self::Error>;
}

#[autoimpl(for<T: trait + ?Sized> &mut T, Box<T>)]
pub trait EncodeAndEstimate: Encode {
    /// An associated encoder that returns
    /// integers estimating the amount of space used by each
    /// operation of this measurable encoder.
    type Estimator<'a>: Encode
    where
        Self: 'a;
    /// Return an estimator for this measurable encoder.
    /// This is expected to be a fast operation as its called many times.
    fn estimator(&mut self) -> Self::Estimator<'_>;
}

/// A trait providing decoders with random access.
#[autoimpl(for<T: trait + ?Sized> & T, Box<T>)]
pub trait RandomAccessDecoderFactory {
    type Decoder<'a>: Decode + 'a
    where
        Self: 'a;

    /// Create a new reader starting at the given node.
    fn new_decoder(&self, node: usize) -> anyhow::Result<Self::Decoder<'_>>;
}

/// A trait providing decoders on the whole graph.
#[autoimpl(for<T: trait + ?Sized> & T, Box<T>)]
pub trait SequentialDecoderFactory {
    type Decoder<'a>: Decode + 'a
    where
        Self: 'a;

    /// Create a new reader starting at the given node.
    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>>;
}
