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

pub mod par_sort_pairs;
pub use par_sort_pairs::ParSortPairs;

pub mod par_sort_graph;
pub use par_sort_graph::ParSortIters;

use crate::graphs::{
    arc_list_graph::Iter,
    bvgraph::{Decode, Encode},
};

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
/// # use webgraph::thread_pool;
/// let t: rayon::ThreadPool = thread_pool![];
/// ```
/// * Create a [`ThreadPool`](rayon::ThreadPool) with a given number of threads:
/// ```
/// # use webgraph::thread_pool;
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

/// An enum expressing the memory requirements for batched algorithms
/// such as [`SortPairs`] and [`ParSortPairs`].
///
/// This type implements [`Mul`](core::ops::Mul) and [`Div`](core::ops::Div) to
/// scale the memory usage requirements by a given factor, independently of the
/// variant.
#[derive(Clone, Copy, Debug)]
pub enum MemoryUsage {
    /// The target overall memory usage in bytes.
    ///
    /// This is the more user-friendly option. The actual number of elements
    /// can be computed using [`batch_size`](MemoryUsage::batch_size).
    MemorySize(usize),
    /// The number of elements used in all batches.
    ///
    /// This is a more low-level option that gives more control to the user, but
    /// the actual memory usage will depend on the size of labels (if any).
    BatchSize(usize),
}

/// Default implementation, returning half of the physical RAM.
impl Default for MemoryUsage {
    fn default() -> Self {
        Self::from_perc(50.0)
    }
}

impl core::fmt::Display for MemoryUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryUsage::MemorySize(size) => write!(f, "{} bytes", size),
            MemoryUsage::BatchSize(size) => write!(f, "{} elements", size),
        }
    }
}

impl core::ops::Mul<usize> for MemoryUsage {
    type Output = MemoryUsage;

    fn mul(self, rhs: usize) -> Self::Output {
        match self {
            MemoryUsage::MemorySize(size) => MemoryUsage::MemorySize(size * rhs),
            MemoryUsage::BatchSize(size) => MemoryUsage::BatchSize(size * rhs),
        }
    }
}

impl core::ops::Div<usize> for MemoryUsage {
    type Output = MemoryUsage;

    fn div(self, rhs: usize) -> Self::Output {
        match self {
            MemoryUsage::MemorySize(size) => MemoryUsage::MemorySize(size / rhs),
            MemoryUsage::BatchSize(size) => MemoryUsage::BatchSize(size / rhs),
        }
    }
}

impl MemoryUsage {
    /// Creates a new memory usage expressed as a percentage of the
    /// physical RAM.
    pub fn from_perc(perc: f64) -> Self {
        let system = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::nothing()
                .with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()),
        );
        MemoryUsage::MemorySize(
            usize::try_from((system.total_memory() as f64 * perc / 100.0) as u64)
                .expect("System memory overflows usize"),
        )
    }

    /// Returns the batch size for elements of type `T`.
    ///
    /// If the [memory usage is expressed as a number of
    /// bytes](MemoryUsage::MemorySize), this method divides the number of bytes
    /// by the size of `T` to obtain the number of elements. Otherwise, [it just
    /// returns specified batch size](MemoryUsage::BatchSize).
    pub fn batch_size<T>(&self) -> usize {
        match &self {
            MemoryUsage::MemorySize(memory_size) => {
                let elem_size = std::mem::size_of::<T>();
                assert!(elem_size > 0, "Element size cannot be zero");
                memory_size / elem_size
            }
            MemoryUsage::BatchSize(batch_size) => *batch_size,
        }
    }
}

/// A structure holding partition iterators and their boundaries.
///
/// This type holds a list of iterators and a list of boundaries, one more
/// than the number of iterators. The implied semantics is that each iterator
/// returns (labelled) pairs of nodes, and that the first element of
/// each pair sits between the boundaries associated with the iterator.
///
/// This structures is returned by [`ParSortPairs`] and [`ParSortIters`] and can
/// easily be converted into lenders for use with
/// [`BvComp::parallel_iter`](crate::graphs::bvgraph::BvComp::parallel_iter)
/// using a convenient implementation of the [`From`] trait.
pub struct SplitIters<I> {
    pub boundaries: Box<[usize]>,
    pub iters: Box<[I]>,
}

impl<I> SplitIters<I> {
    pub fn new(boundaries: Box<[usize]>, iters: Box<[I]>) -> Self {
        Self { boundaries, iters }
    }
}

impl<I> From<(Box<[usize]>, Box<[I]>)> for SplitIters<I> {
    fn from((boundaries, iters): (Box<[usize]>, Box<[I]>)) -> Self {
        Self::new(boundaries, iters)
    }
}

/// Conversion of a [`SplitIters`] of iterators on unlabeled pairs into a
/// sequence of pairs of starting points and associated lenders.
///
/// This is useful for converting the output of sorting utilities like
/// [`ParSortPairs`] or [`ParSortIters`] into a form suitable for
/// [`BvComp::parallel_iter`](crate::graphs::bvgraph::BvComp::parallel_iter)
/// when working with unlabeled graphs.
///
/// The pairs `(src, dst)` are automatically converted to labeled form with unit
/// labels, and the resulting lenders are wrapped with
/// [`LeftIterator`](crate::labels::proj::LeftIterator) to project out just the
/// successor nodes.
impl<
        I: Iterator<Item = (usize, usize)> + Send + Sync,
        IT: IntoIterator<Item = (usize, usize), IntoIter = I>,
    > From<SplitIters<IT>>
    for Vec<(
        usize,
        crate::labels::proj::LeftIterator<
            Iter<(), std::iter::Map<I, fn((usize, usize)) -> (usize, usize, ())>>,
        >,
    )>
{
    fn from(split: SplitIters<IT>) -> Self {
        split
            .iters
            .into_vec()
            .into_iter()
            .enumerate()
            .map(|(i, iter)| {
                let start_node = split.boundaries[i];
                let end_node = split.boundaries[i + 1];
                let num_partition_nodes = end_node - start_node;
                // Map pairs to triples with unit labels
                let map_fn: fn((usize, usize)) -> (usize, usize, ()) = |(src, dst)| (src, dst, ());
                let labeled_iter = iter.into_iter().map(map_fn);
                let lender = Iter::try_new_from(num_partition_nodes, labeled_iter, start_node)
                    .expect("Iterator should start from the expected first node");
                // Wrap with LeftIterator to project out just the successor
                (start_node, crate::labels::proj::LeftIterator(lender))
            })
            .collect()
    }
}

/// Conversion of a [`SplitIters`] of iterators on labelled pairs into a
/// sequences of pairs of starting points and associated lenders.
///
/// This is useful for converting the output of sorting utilities like
/// [`ParSortPairs`] or [`ParSortIters`] into a form suitable for
/// [`BvComp::parallel_iter`](crate::graphs::bvgraph::BvComp::parallel_iter).
impl<
        L: Clone + Copy + 'static,
        I: Iterator<Item = (usize, usize, L)> + Send + Sync,
        IT: IntoIterator<Item = (usize, usize, L), IntoIter = I>,
    > From<SplitIters<IT>> for Vec<(usize, Iter<L, I>)>
{
    fn from(split: SplitIters<IT>) -> Self {
        split
            .iters
            .into_vec()
            .into_iter()
            .enumerate()
            .map(|(i, iter)| {
                let start_node = split.boundaries[i];
                let end_node = split.boundaries[i + 1];
                let num_partition_nodes = end_node - start_node;
                let lender = Iter::try_new_from(num_partition_nodes, iter.into_iter(), start_node)
                    .expect("Iterator should start from the expected first node");
                (start_node, lender)
            })
            .collect()
    }
}
