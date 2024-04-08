/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::marker::PhantomData;

use super::super::*;
use anyhow::bail;
use dsi_bitstream::prelude::*;
use epserde::deser::MemCase;
use sux::traits::IndexedDict;

#[derive(Debug, Clone)]
pub struct DynCodesDecoder<E: Endianness, CR: CodeRead<E>> {
    pub(crate) code_reader: CR,
    pub(crate) read_outdegree: fn(&mut CR) -> u64,
    pub(crate) read_reference_offset: fn(&mut CR) -> u64,
    pub(crate) read_block_count: fn(&mut CR) -> u64,
    pub(crate) read_block: fn(&mut CR) -> u64,
    pub(crate) read_interval_count: fn(&mut CR) -> u64,
    pub(crate) read_interval_start: fn(&mut CR) -> u64,
    pub(crate) read_interval_len: fn(&mut CR) -> u64,
    pub(crate) read_first_residual: fn(&mut CR) -> u64,
    pub(crate) read_residual: fn(&mut CR) -> u64,
    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CR: CodeRead<E>> DynCodesDecoder<E, CR> {
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
            read_block: select_code!(&cf.blocks),
            read_interval_count: select_code!(&cf.intervals),
            read_interval_start: select_code!(&cf.intervals),
            read_interval_len: select_code!(&cf.intervals),
            read_first_residual: select_code!(&cf.residuals),
            read_residual: select_code!(&cf.residuals),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, CR: CodeRead<E> + BitSeek> BitSeek for DynCodesDecoder<E, CR> {
    type Error = <CR as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_reader.set_bit_pos(bit_index)
    }

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_reader.bit_pos()
    }
}

impl<E: Endianness, CR: CodeRead<E>> Decode for DynCodesDecoder<E, CR> {
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
    fn read_block(&mut self) -> u64 {
        (self.read_block)(&mut self.code_reader)
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

pub struct DynCodesDecoderFactory<
    E: Endianness,
    F: BitReaderFactory<E>,
    OFF: IndexedDict<Input = usize, Output = usize>,
> {
    /// The owned data we will read as a bitstream.
    factory: F,
    /// The offsets into the data.
    offsets: MemCase<OFF>,
    /// The compression flags.
    compression_flags: CompFlags,
    // The cached functions to read the codes.
    read_outdegree: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_reference_offset: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_block_count: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_blocks: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_interval_count: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_interval_start: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_interval_len: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_first_residual: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    read_residual: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64,
    /// Tell the compiler that's Ok that we don't store `E` but we need it
    /// for typing.
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, F: BitReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    DynCodesDecoderFactory<E, F, OFF>
where
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodeRead<E>,
{
    // Const cached functions we use to decode the data. These could be general
    // functions, but this way we have better visibility and we ensure that
    // they are compiled once!
    const READ_UNARY: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_unary().unwrap();
    const READ_GAMMA: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_gamma().unwrap();
    const READ_DELTA: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_delta().unwrap();
    const READ_ZETA2: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_zeta(2).unwrap();
    const READ_ZETA3: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_zeta3().unwrap();
    const READ_ZETA4: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_zeta(4).unwrap();
    const READ_ZETA5: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_zeta(5).unwrap();
    const READ_ZETA6: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_zeta(6).unwrap();
    const READ_ZETA7: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        |cr| cr.read_zeta(7).unwrap();
    const READ_ZETA1: for<'a> fn(&mut <F as BitReaderFactory<E>>::BitReader<'a>) -> u64 =
        Self::READ_GAMMA;

    #[inline(always)]
    /// Return a clone of the compression flags.
    pub fn get_compression_flags(&self) -> CompFlags {
        self.compression_flags
    }

    /// Create a new builder from the data and the compression flags.
    pub fn new(factory: F, offsets: MemCase<OFF>, cf: CompFlags) -> anyhow::Result<Self> {
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
            factory,
            offsets,
            read_outdegree: select_code!(cf.outdegrees),
            read_reference_offset: select_code!(cf.references),
            read_block_count: select_code!(cf.blocks),
            read_blocks: select_code!(cf.blocks),
            read_interval_count: select_code!(cf.intervals),
            read_interval_start: select_code!(cf.intervals),
            read_interval_len: select_code!(cf.intervals),
            read_first_residual: select_code!(cf.residuals),
            read_residual: select_code!(cf.residuals),
            compression_flags: cf,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, F: BitReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    RandomAccessDecoderFactory for DynCodesDecoderFactory<E, F, OFF>
where
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodeRead<E> + BitSeek,
{
    type Decoder<'a> =
        DynCodesDecoder<E, <F as BitReaderFactory<E>>::BitReader<'a>>
    where
        Self: 'a;

    fn new_decoder(&self, node: usize) -> anyhow::Result<Self::Decoder<'_>> {
        let mut code_reader = self.factory.new_reader();
        code_reader.set_bit_pos(self.offsets.get(node) as u64)?;

        Ok(DynCodesDecoder {
            code_reader,
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_block: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            _marker: PhantomData,
        })
    }
}

impl<E: Endianness, F: BitReaderFactory<E>> SequentialDecoderFactory
    for DynCodesDecoderFactory<E, F, EmptyDict<usize, usize>>
where
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodeRead<E>,
{
    type Decoder<'a> =
        DynCodesDecoder<E, <F as BitReaderFactory<E>>::BitReader<'a>>
    where
        Self: 'a;

    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>> {
        Ok(DynCodesDecoder {
            code_reader: self.factory.new_reader(),
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_block: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            _marker: PhantomData,
        })
    }
}
