/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Utilities.

mod argmax;
mod argmin;

/// Module containing mathematical utilities.
pub mod math {
    pub use super::argmax::*;
    pub use super::argmin::*;
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
