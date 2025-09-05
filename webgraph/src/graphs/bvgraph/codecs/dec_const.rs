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
use dsi_bitstream::dispatch::code_consts;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;

use sux::traits::IndexedSeq;

#[repr(transparent)]
/// An implementation of [`Decode`]  with compile-time defined codes.
#[derive(Debug, Clone)]
pub struct ConstCodesDecoder<
    E: Endianness,
    CR: CodesRead<E>,
    const OUTDEGREES: usize = { code_consts::GAMMA },
    const REFERENCES: usize = { code_consts::UNARY },
    const BLOCKS: usize = { code_consts::GAMMA },
    const INTERVALS: usize = { code_consts::GAMMA },
    const RESIDUALS: usize = { code_consts::ZETA3 },
> {
    /// The inner codes reader we will dispatch to
    pub(crate) code_reader: CR,
    /// Make the compiler happy with the generics we don't use in the struct
    /// (but we need them to be able to use the trait)
    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        CR: CodesRead<E> + BitSeek,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > BitSeek for ConstCodesDecoder<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
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
        CR: CodesRead<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > ConstCodesDecoder<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    /// Creates a new [`ConstCodesEncoder`] from a [`CodesRead`] implementation.
    /// and a [`CompFlags`] struct
    /// # Errors
    /// If the codes in the [`CompFlags`] do not match the compile-time defined codes
    pub fn new(code_reader: CR, comp_flags: &CompFlags) -> Result<Self> {
        if comp_flags.outdegrees.to_code_const()? != OUTDEGREES {
            bail!("Code for outdegrees does not match");
        }
        if comp_flags.references.to_code_const()? != REFERENCES {
            bail!("Cod for references does not match");
        }
        if comp_flags.blocks.to_code_const()? != BLOCKS {
            bail!("Code for blocks does not match");
        }
        if comp_flags.intervals.to_code_const()? != INTERVALS {
            bail!("Code for intervals does not match");
        }
        if comp_flags.residuals.to_code_const()? != RESIDUALS {
            bail!("Code for residuals does not match");
        }
        Ok(Self {
            code_reader,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<
        E: Endianness,
        CR: CodesRead<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > Decode for ConstCodesDecoder<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        ConstCode::<OUTDEGREES>.read(&mut self.code_reader).unwrap()
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        ConstCode::<REFERENCES>.read(&mut self.code_reader).unwrap()
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        ConstCode::<BLOCKS>.read(&mut self.code_reader).unwrap()
    }
    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        ConstCode::<BLOCKS>.read(&mut self.code_reader).unwrap()
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        ConstCode::<INTERVALS>.read(&mut self.code_reader).unwrap()
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        ConstCode::<INTERVALS>.read(&mut self.code_reader).unwrap()
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        ConstCode::<INTERVALS>.read(&mut self.code_reader).unwrap()
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        ConstCode::<RESIDUALS>.read(&mut self.code_reader).unwrap()
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        ConstCode::<RESIDUALS>.read(&mut self.code_reader).unwrap()
    }
}

pub struct ConstCodesDecoderFactory<
    E: Endianness,
    F: CodesReaderFactoryHelper<E>,
    OFF: IndexedSeq<Input = usize, Output = usize>,
    const OUTDEGREES: usize = { code_consts::GAMMA },
    const REFERENCES: usize = { code_consts::UNARY },
    const BLOCKS: usize = { code_consts::GAMMA },
    const INTERVALS: usize = { code_consts::GAMMA },
    const RESIDUALS: usize = { code_consts::ZETA3 },
> {
    /// The owned data
    factory: F,
    /// The offsets into the data.
    offsets: OFF,
    /// Tell the compiler that's Ok that we don't store `E` but we need it
    /// for typing.
    _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        F: CodesReaderFactoryHelper<E>,
        OFF: IndexedSeq<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > ConstCodesDecoderFactory<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
where
    for<'a> &'a OFF: IntoIterator<Item = usize>, // This dependence can soon be removed, as there will be a IndexedSeq::iter method
{
    /// Remaps the offsets in a slice of `usize`.
    ///
    /// This method is mainly useful for benchmarking and testing purposes, as
    /// representing the offsets as a slice increasing significantly the
    /// memory footprint.
    ///
    /// This method is used by [`BvGraph::offsets_to_slice`].
    pub fn offsets_to_slice(
        self,
    ) -> ConstCodesDecoderFactory<
        E,
        F,
        SliceSeq<usize, Box<[usize]>>,
        OUTDEGREES,
        REFERENCES,
        BLOCKS,
        INTERVALS,
        RESIDUALS,
    > {
        ConstCodesDecoderFactory {
            factory: self.factory,
            offsets: <Box<[usize]> as Into<SliceSeq<usize, Box<[usize]>>>>::into(
                (0..self.offsets.len())
                    .map(|i| unsafe { self.offsets.get_unchecked(i) })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            _marker: PhantomData,
        }
    }
}

impl<
        E: Endianness,
        F: CodesReaderFactoryHelper<E>,
        OFF: IndexedSeq<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > ConstCodesDecoderFactory<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    /// Creates a new builder from the given data and compression flags.
    pub fn new(factory: F, offsets: OFF, comp_flags: CompFlags) -> anyhow::Result<Self> {
        if comp_flags.outdegrees.to_code_const()? != OUTDEGREES {
            bail!("Code for outdegrees does not match");
        }
        if comp_flags.references.to_code_const()? != REFERENCES {
            bail!("Cod for references does not match");
        }
        if comp_flags.blocks.to_code_const()? != BLOCKS {
            bail!("Code for blocks does not match");
        }
        if comp_flags.intervals.to_code_const()? != INTERVALS {
            bail!("Code for intervals does not match");
        }
        if comp_flags.residuals.to_code_const()? != RESIDUALS {
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
        F: CodesReaderFactoryHelper<E>,
        OFF: IndexedSeq<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > RandomAccessDecoderFactory
    for ConstCodesDecoderFactory<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
where
    for<'a> <F as CodesReaderFactory<E>>::CodesReader<'a>: BitSeek,
{
    type Decoder<'a>
        = ConstCodesDecoder<E, <F as CodesReaderFactory<E>>::CodesReader<'a>>
    where
        Self: 'a;

    fn new_decoder(&self, offset: usize) -> anyhow::Result<Self::Decoder<'_>> {
        let mut code_reader = self.factory.new_reader();
        code_reader.set_bit_pos(unsafe { self.offsets.get_unchecked(offset) } as u64)?;

        Ok(ConstCodesDecoder {
            code_reader,
            _marker: PhantomData,
        })
    }
}

impl<
        E: Endianness,
        F: CodesReaderFactoryHelper<E>,
        OFF: IndexedSeq<Input = usize, Output = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > SequentialDecoderFactory
    for ConstCodesDecoderFactory<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
    type Decoder<'a>
        = ConstCodesDecoder<E, <F as CodesReaderFactory<E>>::CodesReader<'a>>
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
