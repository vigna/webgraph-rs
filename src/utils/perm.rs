use crate::utils::MmapBackend;
use anyhow::Result;
use epserde::prelude::*;
use mmap_rs::*;
use std::path::Path;
use sux::traits::prelude::*;

/// Wrapper for the permutation in the Java format.
///
/// To allow interoperability of the Java end the epserde formats, functions
/// should be implemented over a generic type that implements [`VSlice`] as
/// both [`JavaPermutation<Mmap>`], [`JavaPermutation<MmapMut>`], and the deserialized
/// values from [`epserde`] implement it.
///
/// The java format is an array of big endian u64s.
pub struct JavaPermutation<M> {
    pub perm: MmapBackend<u64>,
}

impl JavaPermutation<MmapMut> {
    /// Create a new  mutable Memory mapped permutation
    pub fn new(path: impl AsRef<Path>, flags: Flags) -> Result<Self> {
        let file_len = std::fs::metadata(path.as_ref())?.len() as usize;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path.as_ref())?;
        let perm = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(flags.mmap_flags())
                .with_file(file, 0)
                .map_mut()?
        };
        Ok(Self { perm })
    }

    /// Memory map a mutable permutation from disk
    pub fn load_mut(path: impl AsRef<Path>, flags: Flags) -> Result<Self> {
        let file_len = std::fs::metadata(path.as_ref())?.len() as usize;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(path.as_ref())?;
        let perm = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(flags.mmap_flags())
                .with_file(file, 0)
                .map_mut()?
        };
        Ok(Self { perm })
    }
}

impl JavaPermutation<Mmap> {
    /// Memory map a permutation from disk reading
    pub fn load(path: impl AsRef<Path>, flags: Flags) -> Result<Self> {
        let file_len = std::fs::metadata(path.as_ref())?.len() as usize;
        let file = std::fs::File::open(path.as_ref())?;
        let perm = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(flags.mmap_flags())
                .with_file(file, 0)
                .map()?
        };
        Ok(Self { perm })
    }
}

impl VSliceCore for JavaPermutation<Mmap> {
    fn bit_width(&self) -> usize {
        64
    }

    fn len(&self) -> usize {
        self.perm.len() / 8
    }
}

impl VSliceCore for JavaPermutation<MmapMut> {
    fn bit_width(&self) -> usize {
        64
    }

    fn len(&self) -> usize {
        self.perm.len() / 8
    }
}

impl VSlice for JavaPermutation<Mmap> {
    #[inline(always)]
    unsafe fn get_unchecked(&self, index: usize) -> usize {
        usize::from_be_bytes(
            self.perm
                .as_ref()
                .get_unchecked(index * 8..(index + 1) * 8)
                .try_into()
                .unwrap_unchecked(),
        )
    }
}

impl VSlice for JavaPermutation<MmapMut> {
    #[inline(always)]
    unsafe fn get_unchecked(&self, index: usize) -> usize {
        usize::from_be_bytes(
            self.perm
                .as_ref()
                .get_unchecked(index * 8..(index + 1) * 8)
                .try_into()
                .unwrap_unchecked(),
        )
    }
}

impl VSliceMut for JavaPermutation<MmapMut> {
    #[inline(always)]
    unsafe fn set_unchecked(&mut self, index: usize, value: usize) {
        self.perm
            .as_mut()
            .get_unchecked_mut(index * 8..(index + 1) * 8)
            .copy_from_slice(&value.to_be_bytes());
    }
}
