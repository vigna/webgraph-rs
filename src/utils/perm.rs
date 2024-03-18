/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::utils::MmapHelper;
use anyhow::Result;
use mmap_rs::{Mmap, MmapFlags, MmapMut};
use std::path::Path;
use sux::traits::*;

/// Maps into memory a file of big-endian 64-bit values, making it accessible as
/// a [`BitFieldSlice<usize>`].
///
/// The purpose of this helper class make interoperability with the big version
/// of the Java implementation of WebGraph easier. It is a thin wrapper
/// around [`MmapHelper`], and its methods are named accordingly.
///
/// Note that this class is only available on 64-bit platforms.
#[cfg(target_pointer_width = "64")]
pub struct JavaPermutation<M = Mmap> {
    pub perm: MmapHelper<u64, M>,
}

#[cfg(target_pointer_width = "64")]
impl JavaPermutation<MmapMut> {
    /// Creates and maps a permutation into memory (read/write), overwriting it if it exists.
    ///
    /// # Arguments
    /// - `path` - The path to the permutation.
    /// - `flags` - The flags to use for the memory mapping.
    /// - `len` - The length of the permutation (number of 64-bit unsigned values).
    pub fn new(path: impl AsRef<Path>, flags: MmapFlags, len: usize) -> Result<Self> {
        Ok(Self {
            perm: MmapHelper::new(path, flags, len)?,
        })
    }

    /// Maps a permutation into memory (read/write).
    ///
    /// # Arguments
    /// - `path` - The path to the permutation.
    /// - `flags` - The flags to use for the memory mapping.
    pub fn mmap_mut(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        Ok(Self {
            perm: MmapHelper::mmap_mut(path, flags)?,
        })
    }
}

impl JavaPermutation {
    /// Maps a permutation into memory (read-only).
    ///
    /// # Arguments
    /// - `path` - The path to the permutation.
    /// - `flags` - The flags to use for the memory mapping.
    pub fn mmap(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        Ok(Self {
            perm: MmapHelper::mmap(path, flags)?,
        })
    }
}

impl BitFieldSliceCore<usize> for JavaPermutation {
    fn bit_width(&self) -> usize {
        64
    }

    fn len(&self) -> usize {
        self.perm.as_ref().len()
    }
}

impl BitFieldSliceCore<usize> for JavaPermutation<MmapMut> {
    fn bit_width(&self) -> usize {
        64
    }

    fn len(&self) -> usize {
        self.perm.as_ref().len()
    }
}

impl BitFieldSlice<usize> for JavaPermutation {
    #[inline(always)]
    unsafe fn get_unchecked(&self, index: usize) -> usize {
        u64::from_be_bytes(self.perm.as_ref().get_unchecked(index).to_ne_bytes()) as usize
    }
}

impl BitFieldSlice<usize> for JavaPermutation<MmapMut> {
    #[inline(always)]
    unsafe fn get_unchecked(&self, index: usize) -> usize {
        u64::from_be_bytes(self.perm.as_ref().get_unchecked(index).to_ne_bytes()) as usize
    }
}

impl BitFieldSliceMut<usize> for JavaPermutation<MmapMut> {
    #[inline(always)]
    unsafe fn set_unchecked(&mut self, index: usize, value: usize) {
        *self.perm.as_mut().get_unchecked_mut(index) = value as u64;
    }

    #[inline(always)]
    fn reset(&mut self) {
        self.perm.as_mut().reset();
    }
}
