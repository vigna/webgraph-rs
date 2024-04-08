/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Factories for bit readers.

Implementations of the [`BitReaderFactory`] trait can be used to create
bit readers accessing a graph data using different techniques.
- [`FileFactory`] uses a [std::fs::File] to create a bit reader.
- [`MemoryFactory`] creates bit readers from a slice of memory,
either [allocated](MemoryFactory::new_mem) or [mapped](MemoryFactory::new_mmap).
- [`MmapHelper`] can be used to create a bit reader from a memory-mapped file.

Any factory can be plugged either into a
[`SequentialDecoderFactory`](super::SequentialDecoderFactory)
or a [`RandomAccessDecoderFactory`](`super::RandomAccessDecoderFactory`),
decoupling the choice of encoder from the underlying support.

*/
use anyhow::{ensure, Context};
use bitflags::bitflags;
use common_traits::UnsignedInt;
use dsi_bitstream::{
    impls::{BufBitReader, MemWordReader, WordAdapter},
    traits::Endianness,
};
use std::{
    fs::File,
    io::{BufReader, Read},
    marker::PhantomData,
    path::Path,
};
use sux::traits::IndexedDict;

use crate::utils::MmapHelper;

pub trait BitReaderFactory<E: Endianness> {
    type BitReader<'a>
    where
        Self: 'a;
    fn new_reader(&self) -> Self::BitReader<'_>;
}

#[derive(Debug, Clone)]
pub struct FileFactory<E: Endianness> {
    path: Box<Path>,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness> FileFactory<E> {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path: Box<Path> = path.as_ref().into();
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("Could not stat {}", path.display()))?;
        ensure!(metadata.is_file(), "File {} is not a file", path.display());

        Ok(Self {
            path,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness> BitReaderFactory<E> for FileFactory<E> {
    type BitReader<'a> = BufBitReader<E, WordAdapter<u32, BufReader<File>>>
    where
        Self: 'a;

    fn new_reader(&self) -> Self::BitReader<'_> {
        BufBitReader::<E, _>::new(WordAdapter::<u32, _>::new(BufReader::new(
            File::open(&self.path).unwrap(),
        )))
    }
}

bitflags! {
    /// Flags for [`MemoryFactory`] and [`MmapHelper`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct MemoryFlags: u32 {
        /// Suggest to map a region using transparent huge pages.
        ///
        /// This flag is only a suggestion, and it is ignored if the kernel does not
        /// support transparent huge pages. It is mainly useful to support
        /// `madvise()`-based huge pages on Linux. Note that at the time
        /// of this writing Linux does not support transparent huge pages
        /// in file-based memory mappings.
        const TRANSPARENT_HUGE_PAGES = 1 << 0;
        /// Suggest that the mapped region will be accessed sequentially.
        ///
        /// This flag is only a suggestion, and it is ignored if the kernel does
        /// not support it. It is mainly useful to support `madvise()` on Linux.
        const SEQUENTIAL = 1 << 1;
        /// Suggest that the mapped region will be accessed randomly.
        ///
        /// This flag is only a suggestion, and it is ignored if the kernel does
        /// not support it. It is mainly useful to support `madvise()` on Linux.
        const RANDOM_ACCESS = 1 << 2;
    }
}

/// Empty flags.
impl core::default::Default for MemoryFlags {
    fn default() -> Self {
        MemoryFlags::empty()
    }
}

impl From<MemoryFlags> for mmap_rs::MmapFlags {
    fn from(flags: MemoryFlags) -> Self {
        let mut mmap_flags = mmap_rs::MmapFlags::empty();
        if flags.contains(MemoryFlags::SEQUENTIAL) {
            mmap_flags |= mmap_rs::MmapFlags::SEQUENTIAL;
        }
        if flags.contains(MemoryFlags::RANDOM_ACCESS) {
            mmap_flags |= mmap_rs::MmapFlags::RANDOM_ACCESS;
        }
        if flags.contains(MemoryFlags::TRANSPARENT_HUGE_PAGES) {
            mmap_flags |= mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES;
        }

        mmap_flags
    }
}

impl From<MemoryFlags> for epserde::deser::Flags {
    fn from(flags: MemoryFlags) -> Self {
        let mut deser_flags = epserde::deser::Flags::empty();
        if flags.contains(MemoryFlags::SEQUENTIAL) {
            deser_flags |= epserde::deser::Flags::SEQUENTIAL;
        }
        if flags.contains(MemoryFlags::RANDOM_ACCESS) {
            deser_flags |= epserde::deser::Flags::RANDOM_ACCESS;
        }
        if flags.contains(MemoryFlags::TRANSPARENT_HUGE_PAGES) {
            deser_flags |= epserde::deser::Flags::TRANSPARENT_HUGE_PAGES;
        }

        deser_flags
    }
}

#[derive(Debug, Clone)]
pub struct MemoryFactory<E: Endianness, M: AsRef<[u32]>> {
    data: M,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, T: AsRef<[u32]>> MemoryFactory<E, T> {
    pub fn from_data(data: T) -> Self {
        Self {
            data,
            _marker: core::marker::PhantomData,
        }
    }
}

impl<E: Endianness> MemoryFactory<E, Box<[u32]>> {
    pub fn new_mem(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let file_len = path
            .metadata()
            .with_context(|| format!("Could not stat {}", path.display()))?
            .len() as usize;
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("Could not open {}", path.display()))?;
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

        file.read_exact(&mut bytes[..file_len])
            .with_context(|| format!("Could not read {}", path.display()))?;
        // Fixes the last few bytes to guarantee zero-extension semantics
        // for bit vectors and full-vector initialization.
        bytes[file_len..].fill(0);
        Ok(Self {
            // Safety: the length is a multiple of 16.
            data: unsafe { std::mem::transmute(bytes.into_boxed_slice()) },
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness> MemoryFactory<E, MmapHelper<u32>> {
    pub fn new_mmap(path: impl AsRef<Path>, flags: MemoryFlags) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let file_len = path
            .metadata()
            .with_context(|| format!("Could not stat {}", path.display()))?
            .len() as usize;
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("Could not open {}", path.display()))?;
        let capacity = file_len.align_to(16);

        let mut mmap = mmap_rs::MmapOptions::new(capacity)?
            .with_flags(flags.into())
            .map_mut()
            .context("Could not create anonymous mmap")?;
        file.read_exact(&mut mmap[..file_len])
            .with_context(|| format!("Could not read {}", path.display()))?;
        // Fixes the last few bytes to guarantee zero-extension semantics
        // for bit vectors.
        mmap[file_len..].fill(0);

        Ok(Self {
            // Safety: the length is a multiple of 16.
            data: MmapHelper::try_from(
                mmap.make_read_only()
                    .map_err(|(_, err)| err)
                    .context("Could not make memory read-only")?,
            )
            .context("Could not create mmap backend")?,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, M: AsRef<[u32]>> BitReaderFactory<E> for MemoryFactory<E, M> {
    type BitReader<'a> = BufBitReader<E, MemWordReader<u32, &'a[u32]>>
    where
        Self: 'a;

    fn new_reader(&self) -> Self::BitReader<'_> {
        BufBitReader::<E, _>::new(MemWordReader::new(self.data.as_ref()))
    }
}

#[derive(Debug, Clone)]
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

impl<E: Endianness> BitReaderFactory<E> for MmapHelper<u32> {
    type BitReader<'a> = BufBitReader<E, MemWordReader<u32, &'a [u32]>>;

    fn new_reader(&self) -> Self::BitReader<'_> {
        BufBitReader::<E, _>::new(MemWordReader::new(self.as_ref()))
    }
}
