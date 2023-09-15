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
        let file_len = path.as_ref().metadata()?.len();
        let file = std::fs::File::open(path.as_ref())
            .with_context(|| "Cannot open file for MmapBackend")?;

        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(flags)
                .with_file(file, 0)
                .map()?
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
        let file_len = path.as_ref().metadata()?.len();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .with_context(|| "Cannot open file for mutable MmapBackend")?;

        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(flags)
                .with_file(file, 0)
                .map_mut()?
        };

        Ok(Self {
            len: mmap.len() / core::mem::size_of::<W>(),
            mmap,
            _marker: core::marker::PhantomData,
        })
    }

    /// Create a new mutable MmapBackend
    pub fn new<P: AsRef<std::path::Path>>(path: P, flags: MmapFlags) -> Result<Self> {
        let file_len = path.as_ref().metadata()?.len();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .with_context(|| "Cannot create file for mutable MmapBackend")?;

        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(flags)
                .with_file(file, 0)
                .map_mut()?
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
