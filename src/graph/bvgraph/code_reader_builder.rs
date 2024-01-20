/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use core::marker::PhantomData;
use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use crate::utils::MmapBackend;

use super::*;
use anyhow::{bail, ensure};
use bitflags::bitflags;
use common_traits::UnsignedInt;
use dsi_bitstream::prelude::*;
use epserde::deser::MemCase;
use mmap_rs::{self, Mmap};
use std::io::Read;
use sux::traits::IndexedDict;

pub trait CodeReaderFactory<E: Endianness> {
    type CodeReader<'a>: CodeRead<E>
    where
        Self: 'a;
    fn new_reader(&self) -> Self::CodeReader<'_>;
}

#[derive(Clone)]
pub struct FileFactory<E: Endianness> {
    path: Box<Path>,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness> FileFactory<E> {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path: Box<Path> = path.as_ref().into();
        let metadata = std::fs::metadata(&path)?;
        ensure!(metadata.is_file(), "File {:?} is not a file", &path);

        Ok(Self {
            path,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness> CodeReaderFactory<E> for FileFactory<E>
where
    for<'a> BufBitReader<E, WordAdapter<u32, BufReader<File>>>: CodeRead<E>,
{
    type CodeReader<'a> = BufBitReader<E, WordAdapter<u32, BufReader<File>>>
    where
        Self: 'a;

    fn new_reader(&self) -> Self::CodeReader<'_> {
        BufBitReader::<E, _>::new(WordAdapter::<u32, _>::new(BufReader::new(
            File::open(&self.path).unwrap(),
        )))
    }
}

bitflags! {
    /// Flags for [`map`] and [`load_mmap`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Flags: u32 {
        /// Suggest to map a region using transparent huge pages. This flag
        /// is only a suggestion, and it is ignored if the kernel does not
        /// support transparent huge pages. It is mainly useful to support
        /// `madvise()`-based huge pages on Linux. Note that at the time
        /// of this writing Linux does not support transparent huge pages
        /// in file-based memory mappings.
        const SEQUENTIAL = 1 << 0;
        const RANDOM_ACCESS = 1 << 1;
        const TRANSPARENT_HUGE_PAGES = 1 << 2;
    }
}

/// Empty flags.
impl core::default::Default for Flags {
    fn default() -> Self {
        Flags::empty()
    }
}

impl Flags {
    /// Translates internal flags to `mmap_rs` flags.
    pub(crate) fn mmap_flags(&self) -> mmap_rs::MmapFlags {
        let mut flags: mmap_rs::MmapFlags = mmap_rs::MmapFlags::empty();
        if self.contains(Self::SEQUENTIAL) {
            flags |= mmap_rs::MmapFlags::SEQUENTIAL;
        }
        if self.contains(Self::RANDOM_ACCESS) {
            flags |= mmap_rs::MmapFlags::RANDOM_ACCESS;
        }
        if self.contains(Self::TRANSPARENT_HUGE_PAGES) {
            flags |= mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES;
        }

        flags
    }
}

#[derive(Clone)]
pub struct MemoryFactory<E: Endianness, M: AsRef<[u32]>> {
    data: Arc<M>,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness> MemoryFactory<E, Box<[u32]>> {
    pub fn new_mem(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file_len = path.as_ref().metadata()?.len() as usize;
        let mut file = std::fs::File::open(path)?;
        let capacity = file_len.align_to(16);

        // SAFETY: the entire vector will be filled with data read from the file,
        // or with zeroes if the file is shorter than the vector.
        let mut bytes = unsafe {
            Vec::from_raw_parts(
                std::alloc::alloc(std::alloc::Layout::from_size_align(capacity, 16)?),
                capacity,
                capacity,
            )
        };

        file.read_exact(&mut bytes[..file_len])?;
        // Fixes the last few bytes to guarantee zero-extension semantics
        // for bit vectors and full-vector initialization.
        bytes[file_len..].fill(0);
        Ok(Self {
            // Safety: the length is a multiple of 16.
            data: Arc::new(unsafe { std::mem::transmute(bytes.into_boxed_slice()) }),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness> MemoryFactory<E, MmapBackend<u32>> {
    pub fn new_mmap(path: impl AsRef<Path>, flags: Flags) -> anyhow::Result<Self> {
        let file_len = path.as_ref().metadata()?.len() as usize;
        let mut file = std::fs::File::open(path)?;
        let capacity = file_len.align_to(16);

        let mut mmap = mmap_rs::MmapOptions::new(capacity)?
            .with_flags(flags.mmap_flags())
            .map_mut()?;
        file.read_exact(&mut mmap[..file_len])?;
        // Fixes the last few bytes to guarantee zero-extension semantics
        // for bit vectors.
        mmap[file_len..].fill(0);

        Ok(Self {
            // Safety: the length is a multiple of 16.
            data: Arc::new(MmapBackend::from(mmap.make_read_only().map_err(|(_, err)| err)?)),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, M: AsRef<[u32]>> CodeReaderFactory<E> for MemoryFactory<E, M>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a[u32]>>: CodeRead<E>,
{
    type CodeReader<'a> = BufBitReader<E, MemWordReader<u32, &'a[u32]>>
    where
        Self: 'a;

    fn new_reader(&self) -> Self::CodeReader<'_> {
        BufBitReader::<E, _>::new(MemWordReader::new(self.data.as_ref().as_ref()))
    }
}

pub struct EmptyDict<I, O> {
    _marker: core::marker::PhantomData<(I, O)>,
}

impl<I, O> IndexedDict for EmptyDict<I, O> {
    type Input = usize;
    type Output = usize;

    fn get(&self, _key: Self::Input) -> Self::Output {
        panic!();
    }

    unsafe fn get_unchecked(&self, _index: usize) -> Self::Output {
        panic!();
    }

    fn len(&self) -> usize {
        0
    }
}

impl<I, O> Default for EmptyDict<I, O> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// A builder for the [`DynamicCodesReader`] that stores the data and gives
/// references to the [`DynamicCodesReader`]. This does single-static-dispatching
/// to optimize the reader building time.
pub struct DynamicCodesReaderBuilder<
    E: Endianness,
    F: CodeReaderFactory<E>,
    OFF: IndexedDict<Input = usize, Output = usize>,
> {
    /// The owned data we will read as a bitstream.
    factory: F,
    /// The offsets into the data.
    offsets: MemCase<OFF>,
    /// The compression flags.
    compression_flags: CompFlags,
    // The cached functions to read the codes.
    read_outdegree: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_reference_offset: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_block_count: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_blocks: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_interval_count: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_interval_start: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_interval_len: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_first_residual: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_residual: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    /// Tell the compiler that's Ok that we don't store `E` but we need it
    /// for typing.
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    DynamicCodesReaderBuilder<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    // Const cached functions we use to decode the data. These could be general
    // functions, but this way we have better visibility and we ensure that
    // they are compiled once!
    const READ_UNARY: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_unary().unwrap();
    const READ_GAMMA: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_gamma().unwrap();
    const READ_DELTA: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_delta().unwrap();
    const READ_ZETA2: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(2).unwrap();
    const READ_ZETA3: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta3().unwrap();
    const READ_ZETA4: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(4).unwrap();
    const READ_ZETA5: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(5).unwrap();
    const READ_ZETA6: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(6).unwrap();
    const READ_ZETA7: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(7).unwrap();
    const READ_ZETA1: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        Self::READ_GAMMA;

    #[inline(always)]
    /// Return a clone of the compression flags.
    pub fn get_compression_flags(&self) -> CompFlags {
        self.compression_flags
    }

    pub fn to_skipper(self) -> DynamicCodesReaderSkipperBuilder<E, F, OFF> {
        DynamicCodesReaderSkipperBuilder::new(self.factory, self.offsets, self.compression_flags)
            .unwrap()
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

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    BVGraphCodesReaderBuilder for DynamicCodesReaderBuilder<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    type Reader<'a> =
        DynamicCodesReader<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    fn get_reader(&self, node: usize) -> Result<Self::Reader<'_>, Box<dyn std::error::Error>> {
        let mut code_reader = self.factory.new_reader();
        code_reader.set_bit_pos(self.offsets.get(node) as u64)?;

        Ok(DynamicCodesReader {
            code_reader,
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
        })
    }
}

impl<E: Endianness, F: CodeReaderFactory<E>> BVGraphSeqCodesReaderBuilder
    for DynamicCodesReaderBuilder<E, F, EmptyDict<usize, usize>>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    type Reader<'a> =
        DynamicCodesReader<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    fn get_reader(&self) -> Result<Self::Reader<'_>, Box<dyn std::error::Error>> {
        Ok(DynamicCodesReader {
            code_reader: self.factory.new_reader(),
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
        })
    }
}

/// A builder for [`DynamicCodesReaderSkipper`]. It is similar to
/// [`DynamicCodesReaderBuilder`] but also supports skipping codes.
///
/// This is a different struct because we need to store the skipper functions
/// which basically double the size of the readers. So during random access
/// we won't need them, so we can slightly speedup the random accesses at the
/// cost of more code.
pub struct DynamicCodesReaderSkipperBuilder<
    E: Endianness,
    F: CodeReaderFactory<E>,
    OFF: IndexedDict<Input = usize, Output = usize>,
> {
    /// The owned data we will read as a bitstream.
    factory: F,
    /// The offsets into the data.
    offsets: MemCase<OFF>,
    /// The compression flags.
    compression_flags: CompFlags,

    // The cached functions to read the codes.
    read_outdegree: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_reference_offset: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_block_count: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_blocks: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_interval_count: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_interval_start: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_interval_len: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_first_residual: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,
    read_residual: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64,

    // The cached functions to skip the codes.
    skip_outdegrees: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_reference_offsets: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_block_counts: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_blocks: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_interval_counts: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_interval_starts: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_interval_lens: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_first_residuals: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),
    skip_residuals: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>),

    /// Tell the compiler that's Ok that we don't store `E` but we need it
    /// for typing.
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    DynamicCodesReaderSkipperBuilder<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    // Const cached functions we use to decode the data. These could be general
    // functions, but this way we have better visibility and we ensure that
    // they are compiled once!
    const READ_UNARY: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_unary().unwrap();
    const READ_GAMMA: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_gamma().unwrap();
    const READ_DELTA: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_delta().unwrap();
    const READ_ZETA2: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(2).unwrap();
    const READ_ZETA3: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta3().unwrap();
    const READ_ZETA4: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(4).unwrap();
    const READ_ZETA5: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(5).unwrap();
    const READ_ZETA6: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(6).unwrap();
    const READ_ZETA7: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        |cr| cr.read_zeta(7).unwrap();
    const READ_ZETA1: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) -> u64 =
        Self::READ_GAMMA;

    // Const cached functions we use to skip the data. These could be general
    // functions, but this way we have better visibility and we ensure that
    // they are compiled once!
    const SKIP_UNARY: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_unary().unwrap();
    const SKIP_GAMMA: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_gamma().unwrap();
    const SKIP_DELTA: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_delta().unwrap();
    const SKIP_ZETA2: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_zeta(2).unwrap();
    const SKIP_ZETA3: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_zeta3().unwrap();
    const SKIP_ZETA4: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_zeta(4).unwrap();
    const SKIP_ZETA5: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_zeta(5).unwrap();
    const SKIP_ZETA6: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_zeta(6).unwrap();
    const SKIP_ZETA7: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        |cr| cr.skip_zeta(7).unwrap();
    const SKIP_ZETA1: for<'a> fn(&mut <F as CodeReaderFactory<E>>::CodeReader<'a>) =
        Self::SKIP_GAMMA;

    #[inline(always)]
    /// Return a copy of the compression flags used to build this reader.
    pub fn get_compression_flags(&self) -> CompFlags {
        self.compression_flags
    }

    /// Build a new `DynamicCodesReaderSkipper` from the given data and
    /// compression flags.
    pub fn new(factory: F, offsets: MemCase<OFF>, cf: CompFlags) -> anyhow::Result<Self> {
        // macro used to dispatch the right function to read the data
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

        // macro used to dispatch the right function to skip the data
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

            skip_outdegrees: select_skip_code!(cf.outdegrees),
            skip_reference_offsets: select_skip_code!(cf.references),
            skip_block_counts: select_skip_code!(cf.blocks),
            skip_blocks: select_skip_code!(cf.blocks),
            skip_interval_counts: select_skip_code!(cf.intervals),
            skip_interval_starts: select_skip_code!(cf.intervals),
            skip_interval_lens: select_skip_code!(cf.intervals),
            skip_first_residuals: select_skip_code!(cf.residuals),
            skip_residuals: select_skip_code!(cf.residuals),

            compression_flags: cf,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    BVGraphCodesReaderBuilder for DynamicCodesReaderSkipperBuilder<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    type Reader<'a> =
        DynamicCodesReaderSkipper<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn get_reader(&self, node: usize) -> Result<Self::Reader<'_>, Box<dyn std::error::Error>> {
        let mut code_reader = self.factory.new_reader();
        code_reader.set_bit_pos(self.offsets.get(node) as u64)?;
        Ok(DynamicCodesReaderSkipper {
            code_reader,
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_blocks: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            skip_outdegrees: self.skip_outdegrees,
            skip_reference_offsets: self.skip_reference_offsets,
            skip_block_counts: self.skip_block_counts,
            skip_blocks: self.skip_blocks,
            skip_interval_counts: self.skip_interval_counts,
            skip_interval_starts: self.skip_interval_starts,
            skip_interval_lens: self.skip_interval_lens,
            skip_first_residuals: self.skip_first_residuals,
            skip_residuals: self.skip_residuals,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    BVGraphSeqCodesReaderBuilder for DynamicCodesReaderSkipperBuilder<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    type Reader<'a> =
        DynamicCodesReaderSkipper<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn get_reader(&self) -> Result<Self::Reader<'_>, Box<dyn std::error::Error>> {
        let code_reader = self.factory.new_reader();
        Ok(DynamicCodesReaderSkipper {
            code_reader,
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_blocks: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            skip_outdegrees: self.skip_outdegrees,
            skip_reference_offsets: self.skip_reference_offsets,
            skip_block_counts: self.skip_block_counts,
            skip_blocks: self.skip_blocks,
            skip_interval_counts: self.skip_interval_counts,
            skip_interval_starts: self.skip_interval_starts,
            skip_interval_lens: self.skip_interval_lens,
            skip_first_residuals: self.skip_first_residuals,
            skip_residuals: self.skip_residuals,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    From<DynamicCodesReaderBuilder<E, F, OFF>> for DynamicCodesReaderSkipperBuilder<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    #[inline(always)]
    fn from(value: DynamicCodesReaderBuilder<E, F, OFF>) -> Self {
        Self::new(value.factory, value.offsets, value.compression_flags).unwrap()
    }
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedDict<Input = usize, Output = usize>>
    From<DynamicCodesReaderSkipperBuilder<E, F, OFF>> for DynamicCodesReaderBuilder<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    #[inline(always)]
    fn from(value: DynamicCodesReaderSkipperBuilder<E, F, OFF>) -> Self {
        Self::new(value.factory, value.offsets, value.compression_flags).unwrap()
    }
}

/// A compile type dispatched codes reader builder.
/// This will create slighlty faster readers than the dynamic one as it avoids
/// the indirection layer which can results in more / better inlining.
pub struct ConstCodesReaderBuilder<
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
    > ConstCodesReaderBuilder<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
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
    > BVGraphCodesReaderBuilder
    for ConstCodesReaderBuilder<E, F, OFF, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodeRead<E> + BitSeek,
{
    type Reader<'a> =
        ConstCodesReader<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    fn get_reader(&self, offset: usize) -> Result<Self::Reader<'_>, Box<dyn std::error::Error>> {
        let mut code_reader = self.factory.new_reader();
        code_reader.set_bit_pos(self.offsets.get(offset) as u64)?;

        Ok(ConstCodesReader {
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
    > BVGraphSeqCodesReaderBuilder
    for ConstCodesReaderBuilder<
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
    type Reader<'a> =
        ConstCodesReader<E, <F as CodeReaderFactory<E>>::CodeReader<'a>>
    where
        Self: 'a;

    fn get_reader(&self) -> Result<Self::Reader<'_>, Box<dyn std::error::Error>> {
        let code_reader = self.factory.new_reader();

        Ok(ConstCodesReader {
            code_reader,
            _marker: PhantomData,
        })
    }
}
