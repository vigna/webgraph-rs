/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_bitstream::codes::dispatch::code_consts;
use dsi_bitstream::prelude::*;
use std::convert::Infallible;

use super::{Encode, EncodeAndEstimate};

#[repr(transparent)]
/// An implementation of [`EncodeAndEstimate`] with compile time defined codes
#[derive(Debug, Clone)]
pub struct ConstCodesEncoder<
    E: Endianness,
    CW: CodesWrite<E>,
    const OUTDEGREES: usize = { code_consts::GAMMA },
    const REFERENCES: usize = { code_consts::UNARY },
    const BLOCKS: usize = { code_consts::GAMMA },
    const INTERVALS: usize = { code_consts::GAMMA },
    const RESIDUALS: usize = { code_consts::ZETA3 },
> {
    code_writer: CW,
    _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        CW: CodesWrite<E> + BitSeek,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > BitSeek for ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    type Error = <CW as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_writer.set_bit_pos(bit_index)
    }

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_writer.bit_pos()
    }
}

impl<
        E: Endianness,
        CW: CodesWrite<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    /// Create a new [`ConstCodesEncoder`] with the given [`CodeWrite`] implementation.
    pub fn new(code_writer: CW) -> Self {
        Self {
            code_writer,
            _marker: core::marker::PhantomData,
        }
    }
}

impl<
        E: Endianness,
        CW: CodesWrite<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > Encode for ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    type Error = <CW as BitWrite<E>>::Error;

    #[inline(always)]
    fn start_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn end_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<OUTDEGREES>.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<REFERENCES>.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<BLOCKS>.write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<BLOCKS>.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<INTERVALS>.write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<INTERVALS>.write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<INTERVALS>.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<RESIDUALS>.write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        ConstCode::<RESIDUALS>.write(&mut self.code_writer, value)
    }

    fn flush(&mut self) -> Result<usize, Self::Error> {
        self.code_writer.flush()
    }
}

impl<
        E: Endianness,
        CW: CodesWrite<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > EncodeAndEstimate
    for ConstCodesEncoder<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    type Estimator<'a>
        = ConstCodesEstimator<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
    where
        Self: 'a;
    fn estimator(&mut self) -> Self::Estimator<'_> {
        ConstCodesEstimator::new()
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Default)]
pub struct ConstCodesEstimator<
    const OUTDEGREES: usize = { code_consts::GAMMA },
    const REFERENCES: usize = { code_consts::UNARY },
    const BLOCKS: usize = { code_consts::GAMMA },
    const INTERVALS: usize = { code_consts::GAMMA },
    const RESIDUALS: usize = { code_consts::ZETA3 },
>;

impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > ConstCodesEstimator<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    pub fn new() -> Self {
        Self
    }
}

impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > Encode for ConstCodesEstimator<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    type Error = Infallible;

    #[inline(always)]
    fn start_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn end_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<OUTDEGREES>.len(value))
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<REFERENCES>.len(value))
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<BLOCKS>.len(value))
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<BLOCKS>.len(value))
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<INTERVALS>.len(value))
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<INTERVALS>.len(value))
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<INTERVALS>.len(value))
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<RESIDUALS>.len(value))
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(ConstCode::<RESIDUALS>.len(value))
    }

    fn flush(&mut self) -> Result<usize, Self::Error> {
        Ok(0)
    }
}
