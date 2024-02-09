/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::path::{Path, PathBuf};

use rand::Rng;

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

/// Create a new random dir inside the given folder
pub fn temp_dir<P: AsRef<std::path::Path>>(base: P) -> String {
    let mut base = base.as_ref().to_owned();
    const ALPHABET: &[u8] = b"0123456789abcdef";
    let mut rnd = rand::thread_rng();
    let mut random_str = String::new();
    loop {
        random_str.clear();
        for _ in 0..16 {
            let idx = rnd.gen_range(0..ALPHABET.len());
            random_str.push(ALPHABET[idx] as char);
        }
        base.push(&random_str);

        if !base.exists() {
            std::fs::create_dir(&base).unwrap();
            return base.to_string_lossy().to_string();
        }
        base.pop();
    }
}

/// Appends a string to a path
///
/// ```
/// # use std::path::{Path, PathBuf};
/// # use webgraph::utils::suffix_path;
///
/// assert_eq!(
///     suffix_path(Path::new("/tmp/graph"), "-transposed"),
///     Path::new("/tmp/graph-transposed").to_owned()
/// );
/// ```
#[inline(always)]
pub fn suffix_path<P: AsRef<Path>, S: AsRef<std::ffi::OsStr>>(path: P, suffix: S) -> PathBuf {
    let mut path = path.as_ref().as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

mod circular_buffer;
pub(crate) use circular_buffer::*;

mod mmap_backend;
pub use mmap_backend::*;

mod perm;
pub use perm::*;

pub mod sort_pairs;
pub use sort_pairs::SortPairs;
