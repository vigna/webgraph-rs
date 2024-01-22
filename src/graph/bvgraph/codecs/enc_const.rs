/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::bail;
use dsi_bitstream::prelude::*;
use epserde::deser::MemCase;
use std::{convert::Infallible, marker::PhantomData};
use sux::traits::IndexedDict;

use crate::prelude::{CodeReaderFactory, CompFlags, EmptyDict};

use super::{
    code_to_const, const_codes, CodeRead, CodeWrite, ConstCodesDecoder, Encoder,
    RandomAccessDecoderFactory, SequentialDecoderFactory,
};

#[repr(transparent)]
/// An implementation of [`BVGraphCodesWriter`] with compile time defined codes
#[derive(Clone)]
pub struct ConstCodesWriter<
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
    > BitSeek for ConstCodesWriter<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
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
    > ConstCodesWriter<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
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
    > Encoder for ConstCodesWriter<E, CW, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Error = <CW as BitWrite<E>>::Error;

    type MockEncoder =
        ConstCodesMockWriter<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>;
    fn mock(&self) -> Self::MockEncoder {
        ConstCodesMockWriter::new()
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

#[repr(transparent)]
/// An implementation of [`BVGraphCodesWriter`] that doesn't write but just
/// returns the number of bits that would be written.
#[derive(Clone, Default)]
pub struct ConstCodesMockWriter<
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
    > ConstCodesMockWriter<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    /// Creates a new [`ConstCodesMockWriter`]
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
    > Encoder for ConstCodesMockWriter<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type Error = Infallible;

    type MockEncoder = Self;
    fn mock(&self) -> Self::MockEncoder {
        ConstCodesMockWriter::new()
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

pub struct ConstCodesDecoderFactory<
    E: Endianness,
    F: CodeReaderFactory<E>,
    OFF: IndexedDict<Input = usize, Output = usize>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
> {
    /// The owned data
    factory: F,
    /// The offsets into the data.
    offsets: MemCase<OFF>,
    /// Tell the compiler that's Ok that we don't store `E` but we need it
    /// for typing.
    _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        F: CodeReaderFactory<E>,
        OFF: IndexedDict<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > ConstCodesDecoderFactory<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    /// Create a new builder from the given data and compression flags.
    pub fn new(factory: F, offsets: MemCase<OFF>, comp_flags: CompFlags) -> anyhow::Result<Self> {
        if code_to_const(comp_flags.outdegrees)? != OUTDEGREES {
            bail!("Code for outdegrees does not match");
        }
        if code_to_const(comp_flags.references)? != REFERENCES {
            bail!("Cod for references does not match");
        }
        if code_to_const(comp_flags.blocks)? != BLOCKS {
            bail!("Code for blocks does not match");
        }
        if code_to_const(comp_flags.intervals)? != INTERVALS {
            bail!("Code for intervals does not match");
        }
        if code_to_const(comp_flags.residuals)? != RESIDUALS {
            bail!("Code for residuals does not match");
        }
        Ok(Self {
            factory,
            offsets,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<
        E: Endianness,
        F: CodeReaderFactory<E>,
        OFF: IndexedDict<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > RandomAccessDecoderFactory
    for ConstCodesDecoderFactory<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    type Decoder<'a> =
        ConstCodesDecoder<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    fn new_decoder(&self, offset: usize) -> anyhow::Result<Self::Decoder<'_>> {
        let mut code_reader = self.factory.new_reader();
        code_reader.set_bit_pos(self.offsets.get(offset) as u64)?;

        Ok(ConstCodesDecoder {
            code_reader,
            _marker: PhantomData,
        })
    }
}

impl<
        E: Endianness,
        F: CodeReaderFactory<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > SequentialDecoderFactory
    for ConstCodesDecoderFactory<
        E,
        F,
        EmptyDict<usize, usize>,
        OUTDEGREES,
        REFERENCES,
        BLOCKS,
        INTERVALS,
        RESIDUALS,
        K,
    >
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    type Decoder<'a> =
        ConstCodesDecoder<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>> {
        let code_reader = self.factory.new_reader();

        Ok(ConstCodesDecoder {
            code_reader,
            _marker: PhantomData,
        })
    }
}
