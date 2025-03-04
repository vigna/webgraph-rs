/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::marker::PhantomData;

use super::super::*;
use dsi_bitstream::prelude::*;
use epserde::deser::MemCase;
use sux::traits::IndexedSeq;

#[derive(Debug)]
pub struct DynCodesDecoder<E: Endianness, CR: CodesRead<E>> {
    pub(crate) code_reader: CR,
    pub(crate) read_outdegree: FuncCodeReader<E, CR>,
    pub(crate) read_reference_offset: FuncCodeReader<E, CR>,
    pub(crate) read_block_count: FuncCodeReader<E, CR>,
    pub(crate) read_block: FuncCodeReader<E, CR>,
    pub(crate) read_interval_count: FuncCodeReader<E, CR>,
    pub(crate) read_interval_start: FuncCodeReader<E, CR>,
    pub(crate) read_interval_len: FuncCodeReader<E, CR>,
    pub(crate) read_first_residual: FuncCodeReader<E, CR>,
    pub(crate) read_residual: FuncCodeReader<E, CR>,
    pub(crate) _marker: core::marker::PhantomData<E>,
}

/// manual implementation to avoid the `E: Clone` bound
impl<E: Endianness, CR: CodesRead<E> + Clone> Clone for DynCodesDecoder<E, CR> {
    fn clone(&self) -> Self {
        Self {
            code_reader: self.code_reader.clone(),
            read_outdegree: self.read_outdegree.clone(),
            read_reference_offset: self.read_reference_offset.clone(),
            read_block_count: self.read_block_count.clone(),
            read_block: self.read_block.clone(),
            read_interval_count: self.read_interval_count.clone(),
            read_interval_start: self.read_interval_start.clone(),
            read_interval_len: self.read_interval_len.clone(),
            read_first_residual: self.read_first_residual.clone(),
            read_residual: self.read_residual.clone(),
            _marker: PhantomData,
        }
    }
}

impl<E: Endianness, CR: CodesRead<E>> DynCodesDecoder<E, CR> {
    pub fn new(code_reader: CR, cf: &CompFlags) -> anyhow::Result<Self> {
        Ok(Self {
            code_reader,
            read_outdegree: FuncCodeReader::new(cf.outdegrees)?,
            read_reference_offset: FuncCodeReader::new(cf.references)?,
            read_block_count: FuncCodeReader::new(cf.blocks)?,
            read_block: FuncCodeReader::new(cf.blocks)?,
            read_interval_count: FuncCodeReader::new(cf.intervals)?,
            read_interval_start: FuncCodeReader::new(cf.intervals)?,
            read_interval_len: FuncCodeReader::new(cf.intervals)?,
            read_first_residual: FuncCodeReader::new(cf.residuals)?,
            read_residual: FuncCodeReader::new(cf.residuals)?,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, CR: CodesRead<E> + BitSeek> BitSeek for DynCodesDecoder<E, CR> {
    type Error = <CR as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_reader.set_bit_pos(bit_index)
    }

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_reader.bit_pos()
    }
}

impl<E: Endianness, CR: CodesRead<E>> Decode for DynCodesDecoder<E, CR> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        self.read_outdegree.read(&mut self.code_reader).unwrap()
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        self.read_reference_offset
            .read(&mut self.code_reader)
            .unwrap()
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        self.read_block_count.read(&mut self.code_reader).unwrap()
    }
    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        self.read_block.read(&mut self.code_reader).unwrap()
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        self.read_interval_count
            .read(&mut self.code_reader)
            .unwrap()
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        self.read_interval_start
            .read(&mut self.code_reader)
            .unwrap()
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        self.read_interval_len.read(&mut self.code_reader).unwrap()
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        self.read_first_residual
            .read(&mut self.code_reader)
            .unwrap()
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        self.read_residual.read(&mut self.code_reader).unwrap()
    }
}

#[derive(Debug)]
pub struct DynCodesDecoderFactory<
    E: Endianness,
    F: BitReaderFactory<E>,
    OFF: IndexedSeq<Input = usize, Output = usize>,
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

impl<E: Endianness, F: BitReaderFactory<E>, OFF: IndexedSeq<Input = usize, Output = usize>>
    DynCodesDecoderFactory<E, F, OFF>
where
    // TODO!: This dependence can soon be removed, as there will be a IndexedSeq::iter method
    for<'a> &'a OFF: IntoIterator<Item = usize>,
{
    /// Remaps the offsets in a slice of `usize`.
    ///
    /// This method is mainly useful for benchmarking and testing purposes, as
    /// representing the offsets as a slice increasing significantly the
    /// memory footprint.
    ///
    /// This method is used by [`BvGraph::offsets_to_slice`].
    pub fn offsets_to_slice(self) -> DynCodesDecoderFactory<E, F, SliceSeq<usize, Box<[usize]>>> {
        DynCodesDecoderFactory {
            factory: self.factory,
            offsets: <Box<[usize]> as Into<SliceSeq<usize, Box<[usize]>>>>::into(
                self.offsets
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
            .into(),
            compression_flags: self.compression_flags,
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_blocks: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            _marker: PhantomData,
        }
    }
}

impl<E: Endianness, F: BitReaderFactory<E>, OFF: IndexedSeq<Input = usize, Output = usize>>
    DynCodesDecoderFactory<E, F, OFF>
where
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodesRead<E>,
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
                    Codes::Unary => Self::READ_UNARY,
                    Codes::Gamma => Self::READ_GAMMA,
                    Codes::Delta => Self::READ_DELTA,
                    Codes::Zeta { k: 1 } => Self::READ_ZETA1,
                    Codes::Zeta { k: 2 } => Self::READ_ZETA2,
                    Codes::Zeta { k: 3 } => Self::READ_ZETA3,
                    Codes::Zeta { k: 4 } => Self::READ_ZETA4,
                    Codes::Zeta { k: 5 } => Self::READ_ZETA5,
                    Codes::Zeta { k: 6 } => Self::READ_ZETA6,
                    Codes::Zeta { k: 7 } => Self::READ_ZETA7,
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

impl<E: Endianness, F: BitReaderFactory<E>, OFF: IndexedSeq<Input = usize, Output = usize>>
    RandomAccessDecoderFactory for DynCodesDecoderFactory<E, F, OFF>
where
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodesRead<E> + BitSeek,
{
    type Decoder<'a>
        = DynCodesDecoder<E, <F as BitReaderFactory<E>>::BitReader<'a>>
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

impl<E: Endianness, F: BitReaderFactory<E>, OFF: IndexedSeq<Input = usize, Output = usize>>
    SequentialDecoderFactory for DynCodesDecoderFactory<E, F, OFF>
where
    for<'a> <F as BitReaderFactory<E>>::BitReader<'a>: CodesRead<E>,
{
    type Decoder<'a>
        = DynCodesDecoder<E, <F as BitReaderFactory<E>>::BitReader<'a>>
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
