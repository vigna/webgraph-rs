//! Collection of common functions we use throughout the codebase
use dsi_bitstream::prelude::*;
use mmap_rs::*;

/// Bijective mapping from isize to u64 as defined in <https://github.com/vigna/dsiutils/blob/master/src/it/unimi/dsi/bits/Fast.java>
pub const fn int2nat(x: i64) -> u64 {
    (x << 1 ^ (x >> 63)) as u64
}

/// Bijective mapping from u64 to i64 as defined in <https://github.com/vigna/dsiutils/blob/master/src/it/unimi/dsi/bits/Fast.java>
///
/// ```
/// # use webgraph::utils::*;
///
/// assert_eq!(nat2int(0), 0);
/// assert_eq!(nat2int(1), -1);
/// assert_eq!(nat2int(2), 1);
/// assert_eq!(nat2int(3), -2);
/// assert_eq!(nat2int(4), 2);
/// ```
pub const fn nat2int(x: u64) -> i64 {
    ((x >> 1) ^ !((x & 1).wrapping_sub(1))) as i64
}

mod coo_to_graph;
pub use coo_to_graph::*;

mod coo_to_labelled_graph;
pub use coo_to_labelled_graph::*;

mod circular_buffer;
pub(crate) use circular_buffer::*;

//mod sorted_graph;
//pub use sorted_graph::*;

mod kary_heap;
pub use kary_heap::*;

mod sort_pairs;
pub use sort_pairs::*;

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
            _marker: core::marker::PhantomData,
        }
    }
}

impl<W: Word> AsRef<[W]> for MmapBackend<W> {
    fn as_ref(&self) -> &[W] {
        unsafe { core::slice::from_raw_parts(self.mmap.as_ptr() as *const W, self.len) }
    }
}
