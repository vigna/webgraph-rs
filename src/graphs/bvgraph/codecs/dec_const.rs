/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::marker::PhantomData;

use super::super::*;
use anyhow::bail;
use anyhow::Result;
use dsi_bitstream::prelude::*;
use epserde::deser::MemCase;
use sux::traits::IndexedDict;

/// Temporary constants while const enum generics are not stable
pub mod const_codes {
    /// The int associated to UNARY code
    pub const UNARY: usize = 0;
    /// The int associated to GAMMA code
    pub const GAMMA: usize = 1;
    /// The int associated to DELTA code
    pub const DELTA: usize = 2;
    /// The int associated to ZETA code
    pub const ZETA: usize = 3;
}

/// Temporary convertion function while const enum generics are not stable
pub(crate) fn code_to_const(code: Code) -> Result<usize> {
    Ok(match code {
        Code::Unary => const_codes::UNARY,
        Code::Gamma => const_codes::GAMMA,
        Code::Zeta { k: _ } => const_codes::ZETA,
        Code::Delta => const_codes::DELTA,
    })
}

#[repr(transparent)]
/// An implementation of [`BVGraphCodesReader`]  with compile-time defined codes
#[derive(Debug, Clone)]
pub struct ConstCodesDecoder<
    E: Endianness,
    CR: CodeRead<E>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: usize = 3,
> {
    /// The inner codes reader we will dispatch to
    pub(crate) code_reader: CR,
    /// Make the compiler happy with the generics we don't use in the struct
    /// (but we need them to be able to use the trait)
    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        CR: CodeRead<E> + BitSeek,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: usize,
    > BitSeek
    for ConstCodesDecoder<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    type Error = <CR as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_reader.set_bit_pos(bit_index)
    }

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_reader.bit_pos()
    }
}

impl<
        E: Endianness,
        CR: CodeRead<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: usize,
    > ConstCodesDecoder<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    /// Create a new [`ConstCodesReader`] from a [`CodeRead`] implementation
    /// and a [`CompFlags`] struct
    /// # Errors
    /// If the codes in the [`CompFlags`] do not match the compile-time defined codes
    pub fn new(code_reader: CR, comp_flags: &CompFlags) -> Result<Self> {
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
            code_reader,
            _marker: core::marker::PhantomData,
        })
    }
}

macro_rules! select_code_read {
    ($self:ident, $code:expr, $k: expr) => {
        match $code {
            const_codes::UNARY => $self.code_reader.read_unary().unwrap(),
            const_codes::GAMMA => $self.code_reader.read_gamma().unwrap(),
            const_codes::DELTA => $self.code_reader.read_delta().unwrap(),
            const_codes::ZETA if $k == 1 => $self.code_reader.read_gamma().unwrap(),
            const_codes::ZETA if $k == 3 => $self.code_reader.read_zeta3().unwrap(),
            const_codes::ZETA => $self.code_reader.read_zeta(K as u64).unwrap(),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
        }
    };
}

impl<
        E: Endianness,
        CR: CodeRead<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: usize,
    > Decode for ConstCodesDecoder<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        select_code_read!(self, OUTDEGREES, K)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        select_code_read!(self, REFERENCES, K)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        select_code_read!(self, BLOCKS, K)
    }
    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        select_code_read!(self, BLOCKS, K)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        select_code_read!(self, INTERVALS, K)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        select_code_read!(self, INTERVALS, K)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        select_code_read!(self, INTERVALS, K)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        select_code_read!(self, RESIDUALS, K)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        select_code_read!(self, RESIDUALS, K)
    }
}

pub struct ConstCodesDecoderFactory<
    E: Endianness,
    F: BitReaderFactory<E>,
    OFF: IndexedDict<Input = usize, Output = usize>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: usize = 3,
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
        F: BitReaderFactory<E>,
        OFF: IndexedDict<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: usize,
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
        F: BitReaderFactory<E>,
        OFF: IndexedDict<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: usize,
    > RandomAccessDecoderFactory
    for ConstCodesDecoderFactory<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
where
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodeRead<E> + BitSeek,
{
    type Decoder<'a> =
        ConstCodesDecoder<E, <F as BitReaderFactory<E>>::BitReader<'a>>
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
        F: BitReaderFactory<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: usize,
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
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodeRead<E>,
{
    type Decoder<'a> =
        ConstCodesDecoder<E, <F as BitReaderFactory<E>>::BitReader<'a>>
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
