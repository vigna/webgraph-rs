/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Miscellaneous utilities.

use rand::RngExt;
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

mod batch_codec;
pub use batch_codec::*;

mod circular_buffer;
pub(crate) use circular_buffer::*;

mod ragged_array;
pub use ragged_array::RaggedArray;

mod mmap_helper;
pub use mmap_helper::*;

#[cfg(target_pointer_width = "64")]
mod java_perm;
#[cfg(target_pointer_width = "64")]
pub use java_perm::*;

mod granularity;
pub use granularity::*;

pub mod matrix;
pub use matrix::Matrix;

pub mod sort_pairs;

pub mod par_sort_pairs;
pub use par_sort_pairs::ParSortPairs;

pub mod par_sort_iters;
pub use par_sort_iters::ParSortIters;

use crate::graphs::{
    bvgraph::{Decode, Encode},
    sorted_graph::{SortedGraph, SortedLabeledGraph},
};

/// A decoder that encodes the read values using the given encoder.
///
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

/// An enum expressing the memory requirements for batched algorithms
/// such as [`ParSortPairs`] and [`ParSortIters`].
///
/// The [`Default`] implementation uses a non-linear formula: roughly 50% of
/// RAM on machines with up to 64 GiB, then sub-linear (square-root) growth,
/// capped at 1 TiB on 64-bit platforms and 256 MiB on 32-bit platforms.
/// See the [`Default`] impl for the full table.
///
/// This type implements [`Mul`] and [`Div`] to scale the memory usage
/// requirements by a given factor, independently of the variant.
///
/// [`Mul`]: core::ops::Mul
/// [`Div`]: core::ops::Div
#[derive(Clone, Copy, Debug)]
pub enum MemoryUsage {
    /// The target overall memory usage in bytes.
    ///
    /// This is the more user-friendly option. The actual number of elements
    /// can be computed using [`batch_size`].
    ///
    /// [`batch_size`]: MemoryUsage::batch_size
    MemorySize(usize),
    /// The number of elements used in all batches.
    ///
    /// This is a more low-level option that gives more control to the user, but
    /// the actual memory usage will depend on the size of labels (if any).
    BatchSize(usize),
}

/// Default implementation using a non-linear formula that behaves like 50% of
/// RAM on small machines but grows sub-linearly on large ones, capped at 1 TiB.
///
/// Concretely, the default is `min(total / 2, C · √total, cap)` where
/// `C = 4 √GiB` (so the crossover from `total / 2` to the square-root regime
/// happens at 64 GiB) and the cap is 1 TiB on 64-bit platforms and 256 MiB on
/// 32-bit platforms.
///
/// | Total RAM | Default usage | Fraction |
/// |-----------|---------------|----------|
/// | 8 GiB     | 4 GiB         | 50%      |
/// | 16 GiB    | 8 GiB         | 50%      |
/// | 64 GiB    | 32 GiB        | 50%      |
/// | 128 GiB   | 45 GiB        | 35%      |
/// | 256 GiB   | 64 GiB        | 25%      |
/// | 1 TiB     | 128 GiB       | 12.5%    |
/// | 4 TiB     | 256 GiB       | 6.25%    |
///
/// The rationale is that for batch/external-sort workloads the marginal
/// benefit of more memory is logarithmic (one fewer merge pass), while on
/// large machines the OS page cache and co-resident processes benefit from
/// the headroom.  On 32-bit platforms the cap is much tighter because the
/// address space is scarce and CI runners are typically memory-constrained.
impl Default for MemoryUsage {
    fn default() -> Self {
        let system = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::nothing()
                .with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()),
        );
        let total = system.total_memory(); // bytes

        // C = 4 · √(1 GiB) = 4 · 2¹⁵ = 131072.  In bytes:
        //   usage = C · √total = 131072 · √total
        const C: f64 = 131_072.0; // 4 * (1 GiB as f64).sqrt()
        let sqrt_usage = (C * (total as f64).sqrt()) as u64;

        let half = total / 2;

        #[cfg(target_pointer_width = "64")]
        const CAP: u64 = 1u64 << 40; // 1 TiB
        #[cfg(not(target_pointer_width = "64"))]
        const CAP: u64 = 256 * 1024 * 1024; // 256 MiB

        let usage = half.min(sqrt_usage).min(CAP);
        MemoryUsage::MemorySize(usage as usize)
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
        // On 32-bit platforms, sysinfo may report the host's memory which
        // far exceeds the addressable space. We cap at isize::MAX, which is
        // the largest allocation Rust's allocator supports.
        let cap = isize::MAX as u64;
        MemoryUsage::MemorySize(
            ((system.total_memory() as f64 * perc / 100.0) as u64).min(cap) as usize,
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

/// Writes a human-readable representation of a large number using SI prefixes units.
pub fn humanize(value: f64) -> String {
    const UNITS: &[&str] = &["", "K", "M", "G", "T", "P", "E"];
    let mut v = value;
    let mut unit: usize = 0;
    while v >= 1000.0 && unit + 1 < UNITS.len() {
        v /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{:.0}{}", v, UNITS[unit])
    } else {
        format!("{:.3}{}", v, UNITS[unit])
    }
}

/// A structure holding partition iterators and their boundaries.
///
/// This type holds a list of iterators and a list of boundaries, one more
/// than the number of iterators. The implied semantics is that each iterator
/// returns (labeled) pairs of nodes, and that the first element of
/// each pair sits between the boundaries associated with the iterator.
///
/// This structure is returned by [`ParSortPairs`] and [`ParSortIters`].
/// For graph compression, convert the result into a [`SortedGraph`] (or
/// [`SortedLabeledGraph`]) using the [`From`] implementations provided
/// below (i.e., by calling [`.into()`](Into::into)).
pub struct SplitIters<I> {
    pub boundaries: Box<[usize]>,
    pub iters: Box<[I]>,
}

impl<I> SplitIters<I> {
    pub const fn new(boundaries: Box<[usize]>, iters: Box<[I]>) -> Self {
        Self { boundaries, iters }
    }
}

/// Iterator type obtained by merging sorted batches of unlabeled pairs.
///
/// This is the concrete iterator type inside
/// [`SplitIters`]`<SortedPairIter>`, as returned by
/// [`ParSortIters::sort`] and [`ParSortPairs::sort`].
///
/// Note that `SortedPairIter` strips the `()` label from the underlying
/// [`KMergeIters`](sort_pairs::KMergeIters) via [`Map`](std::iter::Map);
/// the transform functions (e.g., [`transpose_split`]) use
/// [`KMergeIters`](sort_pairs::KMergeIters) directly and return a
/// [`SortedGraph`] instead.
///
/// [`ParSortIters::sort`]: par_sort_iters::ParSortIters::sort
/// [`ParSortPairs::sort`]: par_sort_pairs::ParSortPairs::sort
/// [`transpose_split`]: crate::transform::transpose_split
pub type SortedPairIter<const DEDUP: bool = false> = std::iter::Map<
    sort_pairs::KMergeIters<CodecIter<DefaultBatchCodec<DEDUP>>, (), DEDUP>,
    fn(((usize, usize), ())) -> (usize, usize),
>;

impl<I> From<(Box<[usize]>, Box<[I]>)> for SplitIters<I> {
    fn from((boundaries, iters): (Box<[usize]>, Box<[I]>)) -> Self {
        Self::new(boundaries, iters)
    }
}

/// Converts a [`SplitIters`] of unlabeled pair iterators into a
/// [`SortedGraph`].
impl<I: Iterator<Item = (usize, usize)>> From<SplitIters<I>>
    for SortedGraph<std::iter::Map<I, fn((usize, usize)) -> ((usize, usize), ())>>
{
    fn from(split: SplitIters<I>) -> Self {
        SortedGraph::from_parts(split.boundaries, split.iters)
    }
}

/// Converts a [`SplitIters`] of labeled pair iterators into a
/// [`SortedLabeledGraph`].
impl<L, I: Iterator<Item = ((usize, usize), L)>> From<SplitIters<I>> for SortedLabeledGraph<L, I> {
    fn from(split: SplitIters<I>) -> Self {
        SortedLabeledGraph::from_parts(split.boundaries, split.iters)
    }
}
