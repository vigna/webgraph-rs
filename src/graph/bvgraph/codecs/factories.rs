/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::ensure;
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

use crate::utils::MmapBackend;

pub trait CodeReaderFactory<E: Endianness> {
    type CodeReader<'a>
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

impl<E: Endianness> CodeReaderFactory<E> for FileFactory<E> {
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
impl core::default::Default for Flags {
    fn default() -> Self {
        Flags::empty()
    }
}

impl From<Flags> for mmap_rs::MmapFlags {
    fn from(flags: Flags) -> Self {
        let mut mmap_flags = mmap_rs::MmapFlags::empty();
        if flags.contains(Flags::SEQUENTIAL) {
            mmap_flags |= mmap_rs::MmapFlags::SEQUENTIAL;
        }
        if flags.contains(Flags::RANDOM_ACCESS) {
            mmap_flags |= mmap_rs::MmapFlags::RANDOM_ACCESS;
        }
        if flags.contains(Flags::TRANSPARENT_HUGE_PAGES) {
            mmap_flags |= mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES;
        }

        mmap_flags
    }
}

impl From<Flags> for epserde::deser::Flags {
    fn from(flags: Flags) -> Self {
        let mut deser_flags = epserde::deser::Flags::empty();
        if flags.contains(Flags::SEQUENTIAL) {
            deser_flags |= epserde::deser::Flags::SEQUENTIAL;
        }
        if flags.contains(Flags::RANDOM_ACCESS) {
            deser_flags |= epserde::deser::Flags::RANDOM_ACCESS;
        }
        if flags.contains(Flags::TRANSPARENT_HUGE_PAGES) {
            deser_flags |= epserde::deser::Flags::TRANSPARENT_HUGE_PAGES;
        }

        deser_flags
    }
}

#[derive(Clone)]
pub struct MemoryFactory<E: Endianness, M: AsRef<[u32]>> {
    data: M,
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
            data: unsafe { std::mem::transmute(bytes.into_boxed_slice()) },
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
            .with_flags(flags.into())
            .map_mut()?;
        file.read_exact(&mut mmap[..file_len])?;
        // Fixes the last few bytes to guarantee zero-extension semantics
        // for bit vectors.
        mmap[file_len..].fill(0);

        Ok(Self {
            // Safety: the length is a multiple of 16.
            data: MmapBackend::from(mmap.make_read_only().map_err(|(_, err)| err)?),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, M: AsRef<[u32]>> CodeReaderFactory<E> for MemoryFactory<E, M> {
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
