use anyhow::{Context, Result};
use log::info;
use mmap_rs::{Mmap, MmapMut};
use std::path::Path;
use sux::prelude::*;

/// A struct that stores a permutation and is used for the mapping
/// of initial order -> graph order as a slice of BIG ENDIAN u64.
pub struct Order<B> {
    data: B,
}

impl<B: VSliceCore> VSliceCore for Order<B> {
    #[inline(always)]
    fn bit_width(&self) -> usize {
        64
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.data.len() / core::mem::size_of::<u64>()
    }
}

impl<B: VSlice> VSlice for Order<B> {
    #[inline(always)]
    unsafe fn get_unchecked(&self, node_id: usize) -> u64 {
        u64::from_be(self.data.get_unchecked(node_id))
    }
}

impl<B: VSliceMut> VSliceMut for Order<B> {
    #[inline(always)]
    unsafe fn set_unchecked(&mut self, node_id: usize, value: u64) {
        self.data.set_unchecked(node_id, value.to_be())
    }
}

impl Order<Mmap> {
    /// Load a `.order` file
    pub fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path.metadata()?.len();
        let file = std::fs::File::open(path)?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                .with_file(file, 0)
                .map()?
        };
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(data.as_ptr() as *mut _, data.len(), libc::MADV_RANDOM)
        };
        Ok(Self { data })
    }
}

impl Order<Vec<u64>> {
    /// Create a new in-memory `.order` struct
    pub fn new(num_nodes: u64) -> Result<Self> {
        Ok(Self {
            data: vec![0; num_nodes as usize],
        })
    }
}

impl Order<MmapMut> {
    /// Create a new `.order` file
    pub fn new_file<P: AsRef<Path>>(path: P, num_nodes: u64) -> Result<Self> {
        let path = path.as_ref();
        // compute the size of the file we are creating in bytes
        let file_len = num_nodes * core::mem::size_of::<u64>() as u64;
        info!(
            "The file {} will be {} bytes long.",
            path.to_string_lossy(),
            file_len
        );

        // create the file
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .with_context(|| {
                format!("While creating the .order file: {}", path.to_string_lossy())
            })?;

        // fallocate the file with zeros so we can fill it without ever resizing it
        file.set_len(file_len)
            .with_context(|| "While fallocating the file with zeros")?;

        // create a mutable mmap to the file so we can directly write it in place
        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_file(file, 0)
                .map_mut()
                .with_context(|| "While mmapping the file")?
        };

        Ok(Self { data: mmap })
    }

    /// Load a mutable `.order` file
    pub fn load_mut<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path.metadata()?.len();
        let file = std::fs::File::open(path)?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                .with_file(file, 0)
                .map_mut()?
        };
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(data.as_ptr() as *mut _, data.len(), libc::MADV_RANDOM)
        };
        Ok(Self { data })
    }
}
