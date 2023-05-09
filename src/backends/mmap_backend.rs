use crate::traits::*;
use mmap_rs::*;

/// Treat an mmap as a slice.
/// Mmap only implements [`AsRef<[u8]>`] but we need also other types
/// to be able to read bigger words.
/// This wrapper struct just implement this behaviour.
pub struct MmapBackend<W: Word> {
    mmap: Mmap,
    len: usize,
    _marker: core::marker::PhantomData<W>,
}

impl<W: Word> MmapBackend<W> {
    /// Create a new FileBackend
    pub fn new(mmap: Mmap) -> Self {
        Self {
            len: (mmap.len() + core::mem::size_of::<W>() - 1) / core::mem::size_of::<W>(),
            mmap,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<W: Word> AsRef<[W]> for MmapBackend<W> {
    fn as_ref(&self) -> &[W] {
        unsafe {
            core::slice::from_raw_parts(
                self.mmap.as_ptr() as *const W, 
                self.len,
            )
        }
    }
}