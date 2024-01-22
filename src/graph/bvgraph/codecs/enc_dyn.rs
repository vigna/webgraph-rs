/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::convert::Infallible;

use super::{CodeWrite, Encoder};
use crate::prelude::CompFlags;
use dsi_bitstream::prelude::*;

pub struct DynamicCodesWriter<E: Endianness, CW: CodeWrite<E>> {
    code_writer: CW,
    write_outdegree: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_reference_offset: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_block_count: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_blocks: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_interval_count: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_interval_start: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_interval_len: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_first_residual: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_residual: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CW: CodeWrite<E>> DynamicCodesWriter<E, CW> {
    fn select_code(code: &Code) -> fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error> {
        match code {
            Code::Unary => CW::write_unary,
            Code::Gamma => CW::write_gamma,
            Code::Delta => CW::write_delta,
            Code::Zeta { k: 3 } => CW::write_zeta3,
            // TODO: all other zeta codes
            code => panic!("Only unary, ɣ, δ, and ζ₃ codes are allowed. Got {:?}", code),
        }
    }

    /// Create a new [`ConstCodesReaderBuilder`] from a [`CodeRead`] implementation
    /// This will be called by [`DynamicCodesReaderBuilder`] in the [`get_reader`]
    /// method
    pub fn new(code_writer: CW, cf: &CompFlags) -> Self {
        Self {
            code_writer,
            write_outdegree: Self::select_code(&cf.outdegrees),
            write_reference_offset: Self::select_code(&cf.references),
            write_block_count: Self::select_code(&cf.blocks),
            write_blocks: Self::select_code(&cf.blocks),
            write_interval_count: Self::select_code(&cf.intervals),
            write_interval_start: Self::select_code(&cf.intervals),
            write_interval_len: Self::select_code(&cf.intervals),
            write_first_residual: Self::select_code(&cf.residuals),
            write_residual: Self::select_code(&cf.residuals),
            _marker: core::marker::PhantomData,
        }
    }
}

impl<E: Endianness, CW: CodeWrite<E> + BitSeek + Clone> BitSeek for DynamicCodesWriter<E, CW> {
    type Error = <CW as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_writer.set_bit_pos(bit_index)
    }

    fn get_bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_writer.get_bit_pos()
    }
}

fn len_unary(value: u64) -> usize {
    value as usize + 1
}

impl<E: Endianness, CW: CodeWrite<E>> Encoder for DynamicCodesWriter<E, CW>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Error = <CW as BitWrite<E>>::Error;
    type MockEncoder = DynamicCodesMockWriter;

    fn mock(&self) -> Self::MockEncoder {
        macro_rules! reconstruct_code {
            ($code:expr) => {{
                let code = $code as usize;
                if code == CW::write_unary as usize {
                    len_unary
                } else if code == CW::write_gamma as usize {
                    len_gamma
                } else if code == CW::write_delta as usize {
                    len_delta
                } else if code == CW::write_zeta3 as usize {
                    |x| len_zeta(x, 3)
                } else {
                    unreachable!()
                }
            }};
        }
        DynamicCodesMockWriter {
            len_outdegree: reconstruct_code!(self.write_outdegree),
            len_reference_offset: reconstruct_code!(self.write_reference_offset),
            len_block_count: reconstruct_code!(self.write_block_count),
            len_blocks: reconstruct_code!(self.write_blocks),
            len_interval_count: reconstruct_code!(self.write_interval_count),
            len_interval_start: reconstruct_code!(self.write_interval_start),
            len_interval_len: reconstruct_code!(self.write_interval_len),
            len_first_residual: reconstruct_code!(self.write_first_residual),
            len_residual: reconstruct_code!(self.write_residual),
        }
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_outdegree)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_reference_offset)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_block_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_blocks)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_interval_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_interval_start)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_interval_len)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_first_residual)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_residual)(&mut self.code_writer, value)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.code_writer.flush()
    }
}

/// An implementation of [`BVGraphCodesWriter`] that doesn't write anything
/// but just returns the length of the bytes that would have been written.
#[derive(Clone)]
pub struct DynamicCodesMockWriter {
    len_outdegree: fn(u64) -> usize,
    len_reference_offset: fn(u64) -> usize,
    len_block_count: fn(u64) -> usize,
    len_blocks: fn(u64) -> usize,
    len_interval_count: fn(u64) -> usize,
    len_interval_start: fn(u64) -> usize,
    len_interval_len: fn(u64) -> usize,
    len_first_residual: fn(u64) -> usize,
    len_residual: fn(u64) -> usize,
}

impl DynamicCodesMockWriter {
    /// Selects the length function for the given [`Code`].
    fn select_code(code: &Code) -> fn(u64) -> usize {
        match code {
            Code::Unary => len_unary,
            Code::Gamma => len_gamma,
            Code::Delta => len_delta,
            Code::Zeta { k: 3 } => |x| len_zeta(x, 3),
            code => panic!(
                "Only unary, ɣ, δ, and ζ₃ codes are allowed. Got: {:?}",
                code
            ),
        }
    }

    /// Creates a new [`DynamicCodesMockWriter`] from the given [`CompFlags`].
    pub fn new(cf: &CompFlags) -> Self {
        Self {
            len_outdegree: Self::select_code(&cf.outdegrees),
            len_reference_offset: Self::select_code(&cf.references),
            len_block_count: Self::select_code(&cf.blocks),
            len_blocks: Self::select_code(&cf.blocks),
            len_interval_count: Self::select_code(&cf.intervals),
            len_interval_start: Self::select_code(&cf.intervals),
            len_interval_len: Self::select_code(&cf.intervals),
            len_first_residual: Self::select_code(&cf.residuals),
            len_residual: Self::select_code(&cf.residuals),
        }
    }
}

impl Encoder for DynamicCodesMockWriter {
    type Error = Infallible;

    type MockEncoder = Self;
    fn mock(&self) -> Self::MockEncoder {
        self.clone()
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_outdegree)(value))
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_reference_offset)(value))
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_block_count)(value))
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_blocks)(value))
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_interval_count)(value))
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_interval_start)(value))
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_interval_len)(value))
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_first_residual)(value))
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_residual)(value))
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
