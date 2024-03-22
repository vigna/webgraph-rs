/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Miscellaneous utilities.

use rand::Rng;
use std::path::PathBuf;

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

/// Creates a new random dir inside the given folder
pub fn temp_dir<P: AsRef<std::path::Path>>(base: P) -> anyhow::Result<PathBuf> {
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
            std::fs::create_dir(&base)?;
            return Ok(base);
        }
        base.pop();
    }
}

mod circular_buffer;
pub(crate) use circular_buffer::*;

mod mmap_helper;
pub use mmap_helper::*;

mod java_perm;
pub use java_perm::*;

pub mod sort_pairs;
pub use sort_pairs::SortPairs;


pub enum Threads {
    Default,
    Num(usize),
    Pool(rayon::ThreadPool),
}

impl Threads {
    pub fn num_threads(&self) -> usize {
        match self {
            Self::Default => rayon::current_num_threads(),
            Self::Num(num_threads) => *num_threads,
            Self::Pool(thread_pool) => thread_pool.current_num_threads(),
        }
    }
}

impl AsMut<rayon::ThreadPool> for Threads {
    fn as_mut(&mut self) -> &mut rayon::ThreadPool {
        match self {
            Self::Default => {
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .build()
                    .expect("Failed to create thread pool");
                *self = Self::Pool(thread_pool);
                self.as_mut()
            },
            Self::Num(num_threads) => {
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(*num_threads)
                    .build()
                    .expect("Failed to create thread pool");
                *self = Self::Pool(thread_pool);
                self.as_mut()
            }
            Self::Pool(thread_pool) => thread_pool,
        }
    }
}