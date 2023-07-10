use anyhow::Result;
use mmap_rs::Mmap;

/// A struct that stores a permutation and is used for the mapping
/// of initial order -> graph order.
pub struct Order<B: VSlice> {
    data: B,
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

impl<B: VSlice> Order<B> {
    /// Convert an initial order to a graph order
    pub fn get(&self, node_id: usize) -> Option<usize> {
        let word_size = core::mem::size_of::<u64>();
        let offset = node_id * word_size;
        let bytes = self.data.get(offset..offset + word_size)?;
        // this unwrap is always safe because we checked the size
        let value = u64::from_be_bytes(bytes.try_into().unwrap());
        Some(value as _)
    }
}
