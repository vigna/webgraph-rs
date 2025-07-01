/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Miscellaneous utilities.

use rand::Rng;
use std::path::PathBuf;

/// Creates a new random dir inside the given folder
pub fn temp_dir<P: AsRef<std::path::Path>>(base: P) -> anyhow::Result<PathBuf> {
    let mut base = base.as_ref().to_owned();
    const ALPHABET: &[u8] = b"0123456789abcdef";
    let mut rnd = rand::rng();
    let mut random_str = String::new();
    loop {
        random_str.clear();
        for _ in 0..16 {
            let idx = rnd.random_range(0..ALPHABET.len());
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

mod granularity;
pub use granularity::*;

pub mod sort_pairs;
pub use sort_pairs::SortPairs;

use crate::graphs::bvgraph::{Decode, Encode};

/// A decoder that encodes the read values using the given encoder.
/// This is commonly used to change the codes of a graph without decoding and
/// re-encoding it but by changing the codes.
pub struct Converter<D: Decode, E: Encode> {
    pub decoder: D,
    pub encoder: E,
    pub offset: usize,
}

impl<D: Decode, E: Encode> Decode for Converter<D, E> {
    // TODO: implement correctly start_node/end_node
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        let res = self.decoder.read_outdegree();
        self.offset += self.encoder.write_outdegree(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        let res = self.decoder.read_reference_offset();
        self.offset += self.encoder.write_reference_offset(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        let res = self.decoder.read_block_count();
        self.offset += self.encoder.write_block_count(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        let res = self.decoder.read_block();
        self.offset += self.encoder.write_block(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        let res = self.decoder.read_interval_count();
        self.offset += self.encoder.write_interval_count(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        let res = self.decoder.read_interval_start();
        self.offset += self.encoder.write_interval_start(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        let res = self.decoder.read_interval_len();
        self.offset += self.encoder.write_interval_len(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        let res = self.decoder.read_first_residual();
        self.offset += self.encoder.write_first_residual(res).unwrap();
        res
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        let res = self.decoder.read_residual();
        self.offset += self.encoder.write_residual(res).unwrap();
        res
    }
}

/// Utility macro to create [`thread_pools`](`rayon::ThreadPool`).
///
/// There are two forms of this macro:
/// * Create a [`ThreadPool`](rayon::ThreadPool) with the default settings:
/// ```
/// # use webgraph_algo::thread_pool;
/// let t: rayon::ThreadPool = thread_pool![];
/// ```
/// * Create a [`ThreadPool`](rayon::ThreadPool) with a given number of threads:
/// ```
/// # use webgraph_algo::thread_pool;
/// let t: rayon::ThreadPool = thread_pool![7];
/// assert_eq!(t.current_num_threads(), 7);
/// ```
#[macro_export]
macro_rules! thread_pool {
    () => {
        rayon::ThreadPoolBuilder::new()
            .build()
            .expect("Cannot build a ThreadPool with default parameters")
    };
    ($num_threads:expr) => {
        rayon::ThreadPoolBuilder::new()
            .num_threads($num_threads)
            .build()
            .unwrap_or_else(|_| {
                panic!(
                    "Cannot build a ThreadPool with default parameters and {} threads",
                    $num_threads,
                )
            })
    };
}
