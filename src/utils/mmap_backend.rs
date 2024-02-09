/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Context, Result};
use core::fmt::Debug;
use mmap_rs::*;
use std::{mem::size_of, path::Path, sync::Arc};

/// Helper struct providing convenience methods and
/// type-based [`AsRef`] access to an [`Mmap`] or [`MmapMut`].
///
/// The parameter `W` defines the type used to access the [`Mmap`] or [`MmapMut`]
/// instance. Usually, this will be a unsigned type such as `usize`, but per se `W`
/// has no trait bounds.
#[derive(Clone)]
pub struct MmapBackend<W, M = Mmap> {
    /// The underlying [`Mmap`].
    mmap: M,
    /// The length of the mapping in `W`'s.
    len: usize,
    _marker: core::marker::PhantomData<W>,
}

impl<W: Debug> Debug for MmapBackend<W> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MmapBackend")
            .field("mmap", &self.mmap.as_ptr())
            .field("len", &self.len)
            .finish()
    }
}

impl<W: Debug> Debug for MmapBackend<W, MmapMut> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MmapBackend")
            .field("mmap", &self.mmap.as_ptr())
            .field("len", &self.len)
            .finish()
    }
}

impl<W> From<Mmap> for MmapBackend<W> {
    fn from(mmap: Mmap) -> Self {
        Self {
            len: mmap.len(),
            mmap,
            _marker: core::marker::PhantomData,
        }
    }
}

impl<W> MmapBackend<W> {
    /// Return the size of the memory mapping in `W`'s.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Create a new MmapBackend
    pub fn load(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        let file_len = path
            .as_ref()
            .metadata()
            .with_context(|| format!("Cannot stat {}", path.as_ref().display()))?
            .len() as usize;
        let file = std::fs::File::open(path.as_ref())
            .with_context(|| "Cannot open file for MmapBackend")?;
        // Align to multiple of size_of::<W>. Moreover, it must be > 0, or we get a panic.
        let mmap_len = (file_len / size_of::<W>() + 1) * size_of::<W>();

        let mmap = unsafe {
            mmap_rs::MmapOptions::new(mmap_len)
                .with_context(|| format!("Cannot initialize mmap of size {}", mmap_len))?
                .with_flags(flags)
                .with_file(&file, 0)
                .map()
                .with_context(|| {
                    format!(
                        "Cannot mmap {} (size {})",
                        path.as_ref().display(),
                        mmap_len
                    )
                })?
        };

        Ok(Self {
            len: mmap.len() / core::mem::size_of::<W>(),
            mmap,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<W> MmapBackend<W, MmapMut> {
    /// Create a new mutable MmapBackend
    pub fn load_mut(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        let file_len = path
            .as_ref()
            .metadata()
            .with_context(|| format!("Cannot stat {}", path.as_ref().display()))?
            .len();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .with_context(|| {
                format!(
                    "Cannot open {} for mutable MmapBackend",
                    path.as_ref().display()
                )
            })?;

        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .with_context(|| format!("Cannot initialize mmap of size {}", file_len))?
                .with_flags(flags)
                .with_file(&file, 0)
                .map_mut()
                .with_context(|| {
                    format!(
                        "Cannot mutably mmap {} (size {})",
                        path.as_ref().display(),
                        file_len
                    )
                })?
        };

        Ok(Self {
            len: mmap.len() / core::mem::size_of::<W>(),
            mmap,
            _marker: core::marker::PhantomData,
        })
    }

    /// Create a new mutable MmapBackend
    pub fn new(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        let file_len = path
            .as_ref()
            .metadata()
            .with_context(|| format!("Cannot stat {}", path.as_ref().display()))?
            .len();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())
            .with_context(|| {
                format!(
                    "Cannot create {} for mutable MmapBackend",
                    path.as_ref().display()
                )
            })?;

        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .with_context(|| format!("Cannot initialize mmap of size {}", file_len))?
                .with_flags(flags)
                .with_file(&file, 0)
                .map_mut()
                .with_context(|| format!("Cannot mutably mmap {}", path.as_ref().display()))?
        };

        Ok(Self {
            len: mmap.len() / core::mem::size_of::<W>(),
            mmap,
            _marker: core::marker::PhantomData,
        })
    }
}

/// A clonable version of [`MmapBackend`].
///
/// This newtype contains an [`MmapBackend`] wrapped in an [`Arc`], making it possible
/// to clone the backend.
#[derive(Clone)]
pub struct ArcMmapBackend<W>(pub Arc<MmapBackend<W>>);

impl<W> AsRef<[W]> for MmapBackend<W> {
    fn as_ref(&self) -> &[W] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const W, self.len) }
    }
}

impl<W> AsRef<[W]> for ArcMmapBackend<W> {
    fn as_ref(&self) -> &[W] {
        unsafe { std::slice::from_raw_parts(self.0.mmap.as_ptr() as *const W, self.0.len) }
    }
}

impl<W> AsRef<[W]> for MmapBackend<W, MmapMut> {
    fn as_ref(&self) -> &[W] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const W, self.len) }
    }
}

impl<W> AsMut<[W]> for MmapBackend<W, MmapMut> {
    fn as_mut(&mut self) -> &mut [W] {
        unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr() as *mut W, self.len) }
    }
}
