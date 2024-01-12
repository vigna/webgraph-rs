/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use core::convert::Infallible;

use super::*;
use anyhow::bail;
use dsi_bitstream::prelude::*;

/// An implementation of [`BVGraphCodesReader`] with the most commonly used codes
#[derive(Clone)]
pub struct DynamicCodesReader<E: Endianness, CR: CodeRead<E>> {
    pub(crate) code_reader: CR,
    pub(crate) read_outdegree: fn(&mut CR) -> u64,
    pub(crate) read_reference_offset: fn(&mut CR) -> u64,
    pub(crate) read_block_count: fn(&mut CR) -> u64,
    pub(crate) read_blocks: fn(&mut CR) -> u64,
    pub(crate) read_interval_count: fn(&mut CR) -> u64,
    pub(crate) read_interval_start: fn(&mut CR) -> u64,
    pub(crate) read_interval_len: fn(&mut CR) -> u64,
    pub(crate) read_first_residual: fn(&mut CR) -> u64,
    pub(crate) read_residual: fn(&mut CR) -> u64,
    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CR: CodeRead<E>> DynamicCodesReader<E, CR> {
    const READ_UNARY: fn(&mut CR) -> u64 = |cr| cr.read_unary().unwrap();
    const READ_GAMMA: fn(&mut CR) -> u64 = |cr| cr.read_gamma().unwrap();
    const READ_DELTA: fn(&mut CR) -> u64 = |cr| cr.read_delta().unwrap();
    const READ_ZETA2: fn(&mut CR) -> u64 = |cr| cr.read_zeta(2).unwrap();
    const READ_ZETA3: fn(&mut CR) -> u64 = |cr| cr.read_zeta3().unwrap();
    const READ_ZETA4: fn(&mut CR) -> u64 = |cr| cr.read_zeta(4).unwrap();
    const READ_ZETA5: fn(&mut CR) -> u64 = |cr| cr.read_zeta(5).unwrap();
    const READ_ZETA6: fn(&mut CR) -> u64 = |cr| cr.read_zeta(6).unwrap();
    const READ_ZETA7: fn(&mut CR) -> u64 = |cr| cr.read_zeta(7).unwrap();
    const READ_ZETA1: fn(&mut CR) -> u64 = Self::READ_GAMMA;

    /// Create a new [`DynamicCodesReader`] from a [`CodeRead`] implementation
    /// This will be called by [`DynamicCodesReaderBuilder`] in the [`get_reader`]
    /// method
    pub fn new(code_reader: CR, cf: &CompFlags) -> anyhow::Result<Self> {
        macro_rules! select_code {
            ($code:expr) => {
                match $code {
                    Code::Unary => Self::READ_UNARY,
                    Code::Gamma => Self::READ_GAMMA,
                    Code::Delta => Self::READ_DELTA,
                    Code::Zeta { k: 1 } => Self::READ_ZETA1,
                    Code::Zeta { k: 2 } => Self::READ_ZETA2,
                    Code::Zeta { k: 3 } => Self::READ_ZETA3,
                    Code::Zeta { k: 4 } => Self::READ_ZETA4,
                    Code::Zeta { k: 5 } => Self::READ_ZETA5,
                    Code::Zeta { k: 6 } => Self::READ_ZETA6,
                    Code::Zeta { k: 7 } => Self::READ_ZETA7,
                    code => bail!(
                        "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                        code
                    ),
                }
            };
        }

        Ok(Self {
            code_reader,
            read_outdegree: select_code!(&cf.outdegrees),
            read_reference_offset: select_code!(&cf.references),
            read_block_count: select_code!(&cf.blocks),
            read_blocks: select_code!(&cf.blocks),
            read_interval_count: select_code!(&cf.intervals),
            read_interval_start: select_code!(&cf.intervals),
            read_interval_len: select_code!(&cf.intervals),
            read_first_residual: select_code!(&cf.residuals),
            read_residual: select_code!(&cf.residuals),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, CR: CodeRead<E> + BitSeek> BitSeek for DynamicCodesReader<E, CR> {
    type Error = <CR as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_reader.set_bit_pos(bit_index)
    }

    fn get_bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_reader.get_bit_pos()
    }
}

impl<E: Endianness, CR: CodeRead<E>> BVGraphCodesReader for DynamicCodesReader<E, CR> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        (self.read_outdegree)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        (self.read_reference_offset)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        (self.read_block_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> u64 {
        (self.read_blocks)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        (self.read_interval_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        (self.read_interval_start)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        (self.read_interval_len)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        (self.read_first_residual)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        (self.read_residual)(&mut self.code_reader)
    }
}

/// An implementation of [`BVGraphCodesReader`] with the most commonly used codes
#[derive(Clone)]
pub struct DynamicCodesReaderSkipper<E: Endianness, CR: CodeRead<E>> {
    pub(crate) code_reader: CR,

    pub(crate) read_outdegree: fn(&mut CR) -> u64,
    pub(crate) read_reference_offset: fn(&mut CR) -> u64,
    pub(crate) read_block_count: fn(&mut CR) -> u64,
    pub(crate) read_blocks: fn(&mut CR) -> u64,
    pub(crate) read_interval_count: fn(&mut CR) -> u64,
    pub(crate) read_interval_start: fn(&mut CR) -> u64,
    pub(crate) read_interval_len: fn(&mut CR) -> u64,
    pub(crate) read_first_residual: fn(&mut CR) -> u64,
    pub(crate) read_residual: fn(&mut CR) -> u64,

    pub(crate) skip_outdegrees: fn(&mut CR),
    pub(crate) skip_reference_offsets: fn(&mut CR),
    pub(crate) skip_block_counts: fn(&mut CR),
    pub(crate) skip_blocks: fn(&mut CR),
    pub(crate) skip_interval_counts: fn(&mut CR),
    pub(crate) skip_interval_starts: fn(&mut CR),
    pub(crate) skip_interval_lens: fn(&mut CR),
    pub(crate) skip_first_residuals: fn(&mut CR),
    pub(crate) skip_residuals: fn(&mut CR),

    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CR: CodeRead<E>> DynamicCodesReaderSkipper<E, CR> {
    const READ_UNARY: fn(&mut CR) -> u64 = |cr| cr.read_unary().unwrap();
    const READ_GAMMA: fn(&mut CR) -> u64 = |cr| cr.read_gamma().unwrap();
    const READ_DELTA: fn(&mut CR) -> u64 = |cr| cr.read_delta().unwrap();
    const READ_ZETA2: fn(&mut CR) -> u64 = |cr| cr.read_zeta(2).unwrap();
    const READ_ZETA3: fn(&mut CR) -> u64 = |cr| cr.read_zeta3().unwrap();
    const READ_ZETA4: fn(&mut CR) -> u64 = |cr| cr.read_zeta(4).unwrap();
    const READ_ZETA5: fn(&mut CR) -> u64 = |cr| cr.read_zeta(5).unwrap();
    const READ_ZETA6: fn(&mut CR) -> u64 = |cr| cr.read_zeta(6).unwrap();
    const READ_ZETA7: fn(&mut CR) -> u64 = |cr| cr.read_zeta(7).unwrap();
    const READ_ZETA1: fn(&mut CR) -> u64 = Self::READ_GAMMA;

    const SKIP_UNARY: fn(&mut CR) = |cr| cr.skip_unary().unwrap();
    const SKIP_GAMMA: fn(&mut CR) = |cr| cr.skip_gamma().unwrap();
    const SKIP_DELTA: fn(&mut CR) = |cr| cr.skip_delta().unwrap();
    const SKIP_ZETA2: fn(&mut CR) = |cr| cr.skip_zeta(2).unwrap();
    const SKIP_ZETA3: fn(&mut CR) = |cr| cr.skip_zeta3().unwrap();
    const SKIP_ZETA4: fn(&mut CR) = |cr| cr.skip_zeta(4).unwrap();
    const SKIP_ZETA5: fn(&mut CR) = |cr| cr.skip_zeta(5).unwrap();
    const SKIP_ZETA6: fn(&mut CR) = |cr| cr.skip_zeta(6).unwrap();
    const SKIP_ZETA7: fn(&mut CR) = |cr| cr.skip_zeta(7).unwrap();
    const SKIP_ZETA1: fn(&mut CR) = Self::SKIP_GAMMA;

    /// Create a new [`DynamicCodesReader`] from a [`CodeRead`] implementation
    /// This will be called by [`DynamicCodesReaderSkipperBuilder`] in the [`get_reader`]
    /// method
    pub fn new(code_reader: CR, cf: &CompFlags) -> anyhow::Result<Self> {
        macro_rules! select_code {
            ($code:expr) => {
                match $code {
                    Code::Unary => Self::READ_UNARY,
                    Code::Gamma => Self::READ_GAMMA,
                    Code::Delta => Self::READ_DELTA,
                    Code::Zeta { k: 1 } => Self::READ_ZETA1,
                    Code::Zeta { k: 2 } => Self::READ_ZETA2,
                    Code::Zeta { k: 3 } => Self::READ_ZETA3,
                    Code::Zeta { k: 4 } => Self::READ_ZETA4,
                    Code::Zeta { k: 5 } => Self::READ_ZETA5,
                    Code::Zeta { k: 6 } => Self::READ_ZETA6,
                    Code::Zeta { k: 7 } => Self::READ_ZETA7,
                    code => bail!(
                        "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                        code
                    ),
                }
            };
        }
        macro_rules! select_skip_code {
            ($code:expr) => {
                match $code {
                    Code::Unary => Self::SKIP_UNARY,
                    Code::Gamma => Self::SKIP_GAMMA,
                    Code::Delta => Self::SKIP_DELTA,
                    Code::Zeta { k: 1 } => Self::SKIP_ZETA1,
                    Code::Zeta { k: 2 } => Self::SKIP_ZETA2,
                    Code::Zeta { k: 3 } => Self::SKIP_ZETA3,
                    Code::Zeta { k: 4 } => Self::SKIP_ZETA4,
                    Code::Zeta { k: 5 } => Self::SKIP_ZETA5,
                    Code::Zeta { k: 6 } => Self::SKIP_ZETA6,
                    Code::Zeta { k: 7 } => Self::SKIP_ZETA7,
                    code => bail!(
                        "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                        code
                    ),
                }
            };
        }

        Ok(Self {
            code_reader,
            read_outdegree: select_code!(&cf.outdegrees),
            skip_outdegrees: select_skip_code!(&cf.outdegrees),
            read_reference_offset: select_code!(&cf.references),
            skip_reference_offsets: select_skip_code!(&cf.references),
            read_block_count: select_code!(&cf.blocks),
            skip_block_counts: select_skip_code!(&cf.blocks),
            read_blocks: select_code!(&cf.blocks),
            skip_blocks: select_skip_code!(&cf.blocks),
            read_interval_count: select_code!(&cf.intervals),
            skip_interval_counts: select_skip_code!(&cf.intervals),
            read_interval_start: select_code!(&cf.intervals),
            skip_interval_starts: select_skip_code!(&cf.intervals),
            read_interval_len: select_code!(&cf.intervals),
            skip_interval_lens: select_skip_code!(&cf.intervals),
            read_first_residual: select_code!(&cf.residuals),
            skip_first_residuals: select_skip_code!(&cf.residuals),
            read_residual: select_code!(&cf.residuals),
            skip_residuals: select_skip_code!(&cf.residuals),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, CR: CodeRead<E> + BitSeek> BitSeek for DynamicCodesReaderSkipper<E, CR> {
    type Error = <CR as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_reader.set_bit_pos(bit_index)
    }

    fn get_bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_reader.get_bit_pos()
    }
}

impl<E: Endianness, CR: CodeRead<E>> BVGraphCodesReader for DynamicCodesReaderSkipper<E, CR> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        (self.read_outdegree)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        (self.read_reference_offset)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        (self.read_block_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> u64 {
        (self.read_blocks)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        (self.read_interval_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        (self.read_interval_start)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        (self.read_interval_len)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        (self.read_first_residual)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        (self.read_residual)(&mut self.code_reader)
    }
}

impl<E: Endianness, CR: CodeRead<E>> BVGraphCodesSkipper for DynamicCodesReaderSkipper<E, CR> {
    #[inline(always)]
    fn skip_outdegree(&mut self) {
        (self.skip_outdegrees)(&mut self.code_reader)
    }

    #[inline(always)]
    fn skip_reference_offset(&mut self) {
        (self.skip_reference_offsets)(&mut self.code_reader)
    }

    #[inline(always)]
    fn skip_block_count(&mut self) {
        (self.skip_block_counts)(&mut self.code_reader)
    }
    #[inline(always)]
    fn skip_block(&mut self) {
        (self.skip_blocks)(&mut self.code_reader)
    }

    #[inline(always)]
    fn skip_interval_count(&mut self) {
        (self.skip_interval_counts)(&mut self.code_reader)
    }
    #[inline(always)]
    fn skip_interval_start(&mut self) {
        (self.skip_interval_starts)(&mut self.code_reader)
    }
    #[inline(always)]
    fn skip_interval_len(&mut self) {
        (self.skip_interval_lens)(&mut self.code_reader)
    }

    #[inline(always)]
    fn skip_first_residual(&mut self) {
        (self.skip_first_residuals)(&mut self.code_reader)
    }
    #[inline(always)]
    fn skip_residual(&mut self) {
        (self.skip_residuals)(&mut self.code_reader)
    }
}

/// An implementation of [`BVGraphCodesWriter`] with the most commonly used codes
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

impl<E: Endianness, CW: CodeWrite<E>> BVGraphCodesWriter for DynamicCodesWriter<E, CW> {
    type Error = <CW as BitWrite<E>>::Error;
    type MockWriter = DynamicCodesMockWriter;

    fn mock(&self) -> Self::MockWriter {
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

    fn flush(self) -> Result<(), Self::Error> {
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

impl BVGraphCodesWriter for DynamicCodesMockWriter {
    type Error = Infallible;

    type MockWriter = Self;
    fn mock(&self) -> Self::MockWriter {
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

    fn flush(self) -> Result<(), Self::Error> {
        Ok(())
    }
}
