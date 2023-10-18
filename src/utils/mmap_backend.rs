/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Context, Result};
use core::fmt::Debug;
use mmap_rs::*;
use std::sync::Arc;

/// Adapt an [`Mmap`] that implements [`AsRef<[u8]>`] into a [`AsRef<[W]>`].
///
/// This is implemented for two different instances of `M`:
/// - [`Arc<Mmap>`], an immutable case where we put [`Mmap`] inside an [`Arc`](`std::sync::Arc`) so
/// it's [Clonable](`core::clone::Clone`).
/// - [`MmapMut`], for mutable cases.
///
/// While this could not depend on [`Mmap`] but just on [`AsRef<[u8]>`],
/// we only need it on [`Mmap`], so we can provide ergonomic methods to create
/// and load the mmap.
///
/// The main usecases are to be able to easily mmap slices to disk, and to be able
/// to read a bitstream form mmap.
#[derive(Clone)]
pub struct MmapBackend<W, M = Arc<Mmap>> {
    mmap: M,
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

impl<W> MmapBackend<W> {
    /// Create a new MmapBackend
    pub fn load<P: AsRef<std::path::Path>>(path: P, flags: MmapFlags) -> Result<Self> {
        let file_len = path
            .as_ref()
            .metadata()
            .with_context(|| format!("Cannot stat {}", path.as_ref().display()))?
            .len() as usize;
        let file = std::fs::File::open(path.as_ref())
            .with_context(|| "Cannot open file for MmapBackend")?;
        let capacity = (file_len + 7) / 8;
        let mmap = unsafe {
            mmap_rs::MmapOptions::new(capacity * 8)
                .with_context(|| format!("Cannot initialize mmap of size {}", capacity * 8))?
                .with_flags(flags)
                .with_file(file, 0)
                .map()
                .with_context(|| {
                    format!(
                        "Cannot mmap {} (size {})",
                        path.as_ref().display(),
                        capacity * 8
                    )
                })?
        };

        Ok(Self {
            len: mmap.len() / core::mem::size_of::<W>(),
            mmap: Arc::new(mmap),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<W> MmapBackend<W, MmapMut> {
    /// Create a new mutable MmapBackend
    pub fn load_mut<P: AsRef<std::path::Path>>(path: P, flags: MmapFlags) -> Result<Self> {
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
                .with_file(file, 0)
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
    pub fn new<P: AsRef<std::path::Path>>(path: P, flags: MmapFlags) -> Result<Self> {
        let file_len = path
            .as_ref()
            .metadata()
            .with_context(|| format!("Cannot stat {}", path.as_ref().display()))?
            .len();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
                .with_file(file, 0)
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

impl<W> AsRef<[W]> for MmapBackend<W> {
    fn as_ref(&self) -> &[W] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const W, self.len) }
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
