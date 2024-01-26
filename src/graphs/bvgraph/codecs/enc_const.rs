/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_bitstream::prelude::*;
use std::convert::Infallible;

use super::{const_codes, CodeWrite, Encoder, MeasurableEncoder};

#[repr(transparent)]
/// An implementation of [`BVGraphCodesWriter`] with compile time defined codes
#[derive(Clone)]
pub struct ConstCodesEncoder<
    E: Endianness,
    CW: CodeWrite<E>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
> {
    code_writer: CW,
    _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        CW: CodeWrite<E> + BitSeek,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > BitSeek
    for ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type Error = <CW as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_writer.set_bit_pos(bit_index)
    }

    fn get_bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_writer.get_bit_pos()
    }
}

impl<
        E: Endianness,
        CW: CodeWrite<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    /// Creates a new [`ConstCodesWriter`] with the given [`CodeWrite`] implementation
    pub fn new(code_writer: CW) -> Self {
        Self {
            code_writer,
            _marker: core::marker::PhantomData,
        }
    }
}

macro_rules! select_code_write {
    ($self:ident, $code:expr, $k: expr, $value:expr) => {
        match $code {
            const_codes::UNARY => $self.code_writer.write_unary($value),
            const_codes::GAMMA => $self.code_writer.write_gamma($value),
            const_codes::DELTA => $self.code_writer.write_delta($value),
            const_codes::ZETA if $k == 1 => $self.code_writer.write_gamma($value),
            const_codes::ZETA if $k == 3 => $self.code_writer.write_zeta3($value),
            const_codes::ZETA => $self.code_writer.write_zeta($value, K),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
        }
    };
}

impl<
        E: Endianness,
        CW: CodeWrite<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > Encoder
    for ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type Error = <CW as BitWrite<E>>::Error;

    #[inline(always)]
    fn start_node(node: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn end_node(node: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, OUTDEGREES, K, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, REFERENCES, K, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, BLOCKS, K, value)
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, BLOCKS, K, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, INTERVALS, K, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, RESIDUALS, K, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_write!(self, RESIDUALS, K, value)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.code_writer.flush()
    }
}

impl<
        E: Endianness,
        CW: CodeWrite<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > MeasurableEncoder
    for ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type Estimator = ConstCodesEstimator<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>;
    fn estimator(&self) -> Self::Estimator {
        ConstCodesEstimator::new()
    }
}

#[repr(transparent)]
#[derive(Clone, Default)]
pub struct ConstCodesEstimator<
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
>;

impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > ConstCodesEstimator<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    pub fn new() -> Self {
        Self
    }
}

macro_rules! select_code_mock_write {
    ( $code:expr, $k: expr, $value:expr) => {
        Ok(match $code {
            const_codes::UNARY => $value as usize + 1,
            const_codes::GAMMA => len_gamma($value),
            const_codes::DELTA => len_delta($value),
            const_codes::ZETA => len_zeta($value, K),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
        })
    };
}

impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > Encoder for ConstCodesEstimator<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type Error = Infallible;

    #[inline(always)]
    fn start_node(node: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn end_node(node: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(OUTDEGREES, K, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(REFERENCES, K, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(BLOCKS, K, value)
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(BLOCKS, K, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(INTERVALS, K, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(INTERVALS, K, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(RESIDUALS, K, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        select_code_mock_write!(RESIDUALS, K, value)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
