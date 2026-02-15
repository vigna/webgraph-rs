/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Context, Result, ensure};
use common_traits::UnsignedInt;
use core::fmt::Debug;
use mmap_rs::*;
use std::{mem::size_of, path::Path, sync::Arc};

/// Helper struct providing convenience methods and type-based [`AsRef`] access
/// to an [`Mmap`] or [`MmapMut`] instance.
///
/// The parameter `W` defines the type of the slice used to access the [`Mmap`]
/// or [`MmapMut`] instance. Usually, this will be a unsigned type such as
/// `usize`, but per se `W` has no trait bounds.
///
/// If the length of the file is not a multiple of the size of `W`, the behavior
/// of [`mmap`](MmapHelper::mmap) is platform-dependent:
/// - on Linux, files will be silently zero-extended to the smallest length that
///   is a multiple of  the size of `W`;
/// - on Windows, an error will be returned; you will have to pad manually the
///   file using the `pad` command of the `webgraph` CLI.
///
/// On the contrary, [`mmap_mut`](MmapHelper::mmap_mut) will always refuse to
/// map a file whose length is not a multiple of the size of `W`.
///
/// If you need clonable version of this structure, consider using
/// [`ArcMmapHelper`].
#[derive(Clone)]
pub struct MmapHelper<W, M = Mmap> {
    /// The underlying memory mapping, [`Mmap`] or [`MmapMut`].
    mmap: M,
    /// The length of the mapping in `W`'s.
    len: usize,
    _marker: core::marker::PhantomData<W>,
}

impl<W: Debug> Debug for MmapHelper<W, Mmap> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MmapHelper")
            .field("mmap", &self.mmap.as_ptr())
            .field("len", &self.len)
            .finish()
    }
}

impl<W: Debug> Debug for MmapHelper<W, MmapMut> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MmapHelper")
            .field("mmap", &self.mmap.as_ptr())
            .field("len", &self.len)
            .finish()
    }
}

impl<W> TryFrom<Mmap> for MmapHelper<W> {
    type Error = anyhow::Error;

    fn try_from(value: Mmap) -> std::result::Result<Self, Self::Error> {
        #[cfg(windows)]
        {
            /// Only on windows is required, on linux we can handle these cases
            /// with the implicit zero padding that mmap guarantees.
            ensure!(
                value.len() % size_of::<W>() == 0,
                "The size of the mmap is not a multiple of the size of W"
            );
        }
        let len = value.len().div_ceil(size_of::<W>());
        Ok(Self {
            len,
            mmap: value,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<W> MmapHelper<W> {
    /// Returns the size of the memory mapping in `W`'s.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the memory mapping is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        // make clippy happy
        self.len == 0
    }

    /// Maps a file into memory (read-only).
    ///
    /// # Arguments
    /// - `path`: The path to the file to be memory mapped.
    /// - `flags`: The flags to be used for the mmap.
    pub fn mmap(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        let file_len: usize = path
            .as_ref()
            .metadata()
            .with_context(|| format!("Cannot stat {}", path.as_ref().display()))?
            .len()
            .try_into()
            .with_context(|| "Cannot convert file length to usize")?;
        // Align to multiple of size_of::<W>
        let mmap_len = file_len.align_to(size_of::<W>());
        #[cfg(windows)]
        {
            ensure!(
                mmap_len == file_len,
                "File has insufficient padding for word size {}. Use \"webgraph run pad BASENAME u{}\" to ensure sufficient padding.",
                size_of::<W>() * 8,
                size_of::<W>() * 8
            );
        }
        let file = std::fs::File::open(path.as_ref())
            .with_context(|| "Cannot open file for MmapHelper")?;

        let mmap = unsafe {
            // Length must be > 0, or we get a panic.
            mmap_rs::MmapOptions::new(mmap_len.max(size_of::<W>()))
                .with_context(|| format!("Cannot initialize mmap of size {mmap_len}"))?
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
            len: mmap_len / core::mem::size_of::<W>(),
            mmap,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<W> MmapHelper<W, MmapMut> {
    /// Maps a file into memory (read/write).
    ///
    /// # Arguments
    /// - `path`: The path to the file to be mapped.
    /// - `flags`: The flags to be used for the mmap.
    pub fn mmap_mut(path: impl AsRef<Path>, flags: MmapFlags) -> Result<Self> {
        let file_len: usize = path
            .as_ref()
            .metadata()
            .with_context(|| format!("Cannot stat {}", path.as_ref().display()))?
            .len()
            .try_into()
            .with_context(|| "Cannot convert file length to usize")?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .with_context(|| {
                format!(
                    "Cannot open {} for mutable MmapHelper",
                    path.as_ref().display()
                )
            })?;

        // Align to multiple of size_of::<W>
        let mmap_len = file_len.align_to(size_of::<W>());

        ensure!(
            mmap_len == file_len,
            "File has insufficient padding for word size {}. Use \"webgraph run pad BASENAME u{}\" to ensure sufficient padding.",
            size_of::<W>() * 8,
            size_of::<W>() * 8
        );

        let mmap = unsafe {
            mmap_rs::MmapOptions::new(mmap_len.max(1))
                .with_context(|| format!("Cannot initialize mmap of size {file_len}"))?
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

    /// Creates and map a file into memory (read/write), overwriting it if it exists.
    ///
    /// # Arguments
    /// - `path`: The path to the file to be created.
    /// - `flags`: The flags to be used for the mmap.
    /// - `len`: The length of the file in `W`'s.
    pub fn new(path: impl AsRef<Path>, flags: MmapFlags, len: usize) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())
            .with_context(|| format!("Cannot create {} new MmapHelper", path.as_ref().display()))?;
        let file_len = len * size_of::<W>();
        #[cfg(windows)]
        {
            // Zero fill the file as CreateFileMappingW does not initialize everything to 0
            file.set_len(
                file_len
                    .try_into()
                    .with_context(|| "Cannot convert usize to u64")?,
            )
            .with_context(|| "Cannot modify file size")?;
        }
        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .with_context(|| format!("Cannot initialize mmap of size {file_len}"))?
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

impl<W> AsRef<[W]> for MmapHelper<W> {
    fn as_ref(&self) -> &[W] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const W, self.len) }
    }
}

impl<W> AsRef<[W]> for MmapHelper<W, MmapMut> {
    fn as_ref(&self) -> &[W] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const W, self.len) }
    }
}

impl<W> AsMut<[W]> for MmapHelper<W, MmapMut> {
    fn as_mut(&mut self) -> &mut [W] {
        unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr() as *mut W, self.len) }
    }
}

/// A clonable version of a read-only [`MmapHelper`].
///
/// This newtype contains a read-only [`MmapHelper`] wrapped in an [`Arc`],
/// making it possible to clone it.
#[derive(Clone)]
pub struct ArcMmapHelper<W>(pub Arc<MmapHelper<W>>);

impl<W: Debug> Debug for ArcMmapHelper<W> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ArcMmapHelper")
            .field("mmap", &self.0.mmap.as_ptr())
            .field("len", &self.0.len)
            .finish()
    }
}

impl<W> AsRef<[W]> for ArcMmapHelper<W> {
    fn as_ref(&self) -> &[W] {
        unsafe { std::slice::from_raw_parts(self.0.mmap.as_ptr() as *const W, self.0.len) }
    }
}
