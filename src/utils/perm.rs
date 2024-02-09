/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::utils::MmapBackend;
use anyhow::Result;
use mmap_rs::{Mmap, MmapFlags, MmapMut};
use std::path::Path;
use sux::traits::bit_field_slice::BitFieldSlice;
use sux::traits::bit_field_slice::BitFieldSliceMut;
use sux::traits::*;

/// Wrapper for the permutation in the Java format.
///
/// To allow interoperability of the Java end the epserde formats, functions
/// should be implemented over a generic type that implements [`BitFieldSlice`] as
/// both [`JavaPermutation<Mmap>`], [`JavaPermutation<MmapMut>`], and the deserialized
/// values from [`epserde`] implement it.
///
/// The java format is an array of big endian u64s.
pub struct JavaPermutation<M = Mmap> {
    pub perm: MmapBackend<u64, M>,
}

impl JavaPermutation<MmapMut> {
    /// Create a new  mutable Memory mapped permutation
    ///
    /// # Arguments
    /// - `path` - The path to the file to memory map
    /// - `flags` - The flags to use for the memory mapping
    /// - `len` - The length of the permutation (in number of nodes)
    pub fn new(path: impl AsRef<Path>, flags: MmapFlags, len: usize) -> Result<Self> {
        Ok(Self {
            perm: MmapBackend::new(path, flags, len)?,
        })
    }

    /// Memory map a mutable permutation from disk
    ///
    /// # Arguments
    /// - `path` - The path to the file to memory map
    /// - `flags` - The flags to use for the memory mapping
    pub fn load_mut(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        Ok(Self {
            perm: MmapBackend::load_mut(path, flags)?,
        })
    }
}

impl JavaPermutation {
    /// Memory map a permutation from disk reading
    ///
    /// # Arguments
    /// - `path` - The path to the file to memory map
    /// - `flags` - The flags to use for the memory mapping
    pub fn load(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        Ok(Self {
            perm: MmapBackend::load(path, flags)?,
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
