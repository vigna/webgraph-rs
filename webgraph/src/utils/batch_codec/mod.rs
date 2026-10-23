/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Traits and implementations to encode and decode batches of sorted triples
//! to/from disk.
//!
//! The traits and implementations in this module are used to customize the
//! encoding of batches of sorted triples to/from disk. They are used by
//! [`ParSortPairs`], [`ParSortIters`], and the transform functions.
//!
//! They usually do not need to be accessed or modified by the end users, albeit
//! in some specific cases where performance or on-disk occupation is critical
//! they can be customized.
//!
//! [`ParSortPairs`]: crate::utils::par_sort_pairs::ParSortPairs
//! [`ParSortIters`]: crate::utils::par_sort_iters::ParSortIters

use anyhow::{Context, Result};

use super::{ArcMmapHelper, MergePolicy, MmapHelper, NoMerge};
use core::fmt::Display;
use dsi_bitstream::prelude::*;
use mmap_rs::MmapFlags;
use rdst::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

pub mod gaps;
pub mod grouped_gaps;

/// The recommended default batch codec for unlabeled batches.
///
/// With the [`Dedup`] [merge policy], duplicates are eliminated during batch
/// serialization, reducing I/O and disk usage.
///
/// [`Dedup`]: crate::utils::Dedup
/// [merge policy]: crate::utils::merge_policy
pub type DefaultBatchCodec<P = NoMerge> = grouped_gaps::GroupedGapsCodec<
    NE,
    (),
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::DELTA },
    P,
>;

pub type BitWriter<E> = BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>;
pub type BitReader<E> = BufBitReader<E, MemWordReader<u32, ArcMmapHelper<u32>>>;

/// Statistics about a batch encoded by a [`BatchCodec`].
///
/// Implementations also implement [`Display`] so they can be logged.
pub trait BatchStats: Display {
    /// The number of triples actually written to disk.
    ///
    /// When the codec is deduplicating, this is the number of unique triples
    /// that were encoded; it may be smaller than the number of arcs originally
    /// pushed into the batch.
    fn total_triples(&self) -> usize;
}

/// A trait for encoding and decoding batches of sorted triples.
///
/// A codec has a [merge policy], which it uses to collapse the repeated pairs
/// of each batch, and which it passes, through [`merge_policy`], to the
/// [`KMergeIters`] merging its batches. With merging, the labels written to
/// batches (states) may have a type different from that of the labels of the
/// triples to encode.
///
/// [merge policy]: crate::utils::merge_policy
/// [`merge_policy`]: Self::merge_policy
/// [`KMergeIters`]: crate::utils::kmerge_iters::KMergeIters
pub trait BatchCodec: Send + Sync {
    /// The label type of the triples to encode.
    ///
    /// The bounds are needed to sort batches in memory.
    type Label: Copy + Send + Sync + 'static;

    /// The label type written to and decoded from batches.
    ///
    /// It is equal to [`Label`] unless labels are merged, in which case it
    /// is the type of the states of the merger.
    ///
    /// [`Label`]: Self::Label
    type State: Send + Sync + 'static;

    /// The [merge policy] of the codec.
    ///
    /// [merge policy]: crate::utils::merge_policy
    type Policy: MergePolicy<Self::State>;
    /// The type returned by `decode_batch`, the iterator of which yields the
    /// decoded triples in sorted order.
    ///
    /// The type `IntoIter` has to be `Send + Sync + Clone` because they are
    /// used in [`ParSortedGraph`]/[`SortedLabeledGraph`] which require them.
    ///
    /// [`ParSortedGraph`]: crate::graphs::par_sorted_graph::ParSortedGraph
    /// [`SortedLabeledGraph`]: crate::graphs::par_sorted_graph::ParSortedLabeledGraph
    type DecodedBatch: IntoIterator<Item = ((usize, usize), Self::State), IntoIter: Send + Sync + Clone>;

    /// A type representing statistics about the encoded batch.
    ///
    /// It has to implement [`Display`] so that we can log it, and
    /// [`BatchStats`] so that we can query the number of triples actually
    /// encoded (which may differ from the number of arcs pushed when
    /// repeated pairs are collapsed).
    type EncodedBatchStats: BatchStats;

    /// Returns the merge policy, which must be used to merge the decoded
    /// batches.
    fn merge_policy(&self) -> Self::Policy;

    /// Given a batch of sorted triples, encodes them to disk and returns the
    /// number of bits written.
    ///
    /// Note that the input batch must be already sorted. Use
    /// [`encode_batch`] otherwise.
    ///
    /// [`encode_batch`]: Self::encode_batch
    fn encode_sorted_batch(
        &self,
        path: impl AsRef<Path>,
        batch: &[((usize, usize), Self::Label)],
    ) -> Result<(usize, Self::EncodedBatchStats)>;

    /// Given a batch of triples, sorts them, encodes them to disk, and returns
    /// the number of bits written.
    fn encode_batch(
        &self,
        path: impl AsRef<Path>,
        batch: &mut [((usize, usize), Self::Label)],
    ) -> Result<(usize, Self::EncodedBatchStats)> {
        let start = std::time::Instant::now();
        Triple::cast_batch_mut(batch).radix_sort_unstable();
        log::debug!("Sorted {} arcs in {:?}", batch.len(), start.elapsed());
        self.encode_sorted_batch(path, batch)
    }

    /// Decodes a batch of triples from disk.
    ///
    /// The returned type's iterator yields the serialized triples in sorted order.
    fn decode_batch(&self, path: impl AsRef<Path>) -> Result<Self::DecodedBatch>;
}

/// Convenience alias to extract the iterator type of the decoded batch from a
/// [`BatchCodec`].
pub type CodecIter<C> = <<C as BatchCodec>::DecodedBatch as IntoIterator>::IntoIter;

/// Returns the number of distinct pairs in a sorted batch.
fn num_distinct_pairs<L>(batch: &[((usize, usize), L)]) -> usize {
    batch.len() - batch.windows(2).filter(|w| w[0].0 == w[1].0).count()
}

/// Creates a batch file, writing its length as a prefix.
///
/// The length is the number of triples in `batch`, or the number of distinct
/// pairs in `batch` if `collapse` is true.
fn create_batch<E: Endianness, L>(
    path: &Path,
    batch: &[((usize, usize), L)],
    collapse: bool,
) -> Result<(BitWriter<E>, usize)>
where
    BitWriter<E>: CodesWrite<E>,
{
    let mut stream = buf_bit_writer::from_path::<E, usize>(path).with_context(|| {
        format!(
            "Could not create BatchIterator temporary file {}",
            path.display()
        )
    })?;
    let len = if collapse {
        num_distinct_pairs(batch)
    } else {
        batch.len()
    };
    // We use a delta code since it'll be a big number most of the time
    stream
        .write_delta(len as u64)
        .context("Could not write length")?;
    Ok((stream, len))
}

/// Opens a batch file, returning a reader positioned after the length prefix
/// and the length of the batch.
fn open_batch<E: Endianness>(path: &Path) -> Result<(BitReader<E>, usize)>
where
    BitReader<E>: CodesRead<E>,
{
    let mut stream = <BufBitReader<E, _>>::new(MemWordReader::new(ArcMmapHelper(Arc::new(
        MmapHelper::mmap(
            path,
            MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SEQUENTIAL,
        )
        .with_context(|| format!("Could not mmap {}", path.display()))?,
    ))));
    let len = stream.read_delta().context("Could not read length")? as usize;
    Ok((stream, len))
}

/// Implements [`SortedIterator`], [`ExactSizeIterator`], and
/// [`FusedIterator`] for an iterator over a batch with fields `len` and
/// `current`, and with the parameters and the bounds of [`GapsIter`].
///
/// [`SortedIterator`]: crate::traits::SortedIterator
/// [`FusedIterator`]: core::iter::FusedIterator
/// [`GapsIter`]: gaps::GapsIter
macro_rules! impl_batch_iter {
    ($ty:ident<$($code:ident),*>) => {
        // SAFETY: batches are decoded in non-decreasing (src, dst) order.
        unsafe impl<E: Endianness, D: BitDeserializer<E, BitReader<E>>, $(const $code: usize),*>
            $crate::traits::SortedIterator for $ty<E, D, $($code),*>
        where
            BitReader<E>: BitRead<E> + CodesRead<E>,
            BitWriter<E>: BitWrite<E> + CodesWrite<E>,
        {
        }

        impl<E: Endianness, D: BitDeserializer<E, BitReader<E>>, $(const $code: usize),*>
            ExactSizeIterator for $ty<E, D, $($code),*>
        where
            BitReader<E>: BitRead<E> + CodesRead<E>,
            BitWriter<E>: BitWrite<E> + CodesWrite<E>,
        {
            fn len(&self) -> usize {
                self.len - self.current
            }
        }

        impl<E: Endianness, D: BitDeserializer<E, BitReader<E>>, $(const $code: usize),*>
            core::iter::FusedIterator for $ty<E, D, $($code),*>
        where
            BitReader<E>: BitRead<E> + CodesRead<E>,
            BitWriter<E>: BitWrite<E> + CodesWrite<E>,
        {
        }
    };
}

use impl_batch_iter;

/// An arc expressed as a pair of nodes and the associated label.
///
/// Equality and order are defined only (lexicographically) on the pair of
/// nodes.
///
/// Since we use this to sort a batch of `(usize, usize, L)` triples, in order
/// to safely transmute between the two types, Triple has to be
/// `repr(transparent)` of the same tuple type.
///
/// We use this to implement `RadixKey` for sorting batches of triples
/// using the [`rdst`] crate.
///
/// [`rdst`]: https://crates.io/crates/rdst
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Triple<L>(((usize, usize), L));

impl<L> Triple<L> {
    /// slice of `Triple<L>`.
    ///
    /// The conversion is safe because `Triple` is `repr(transparent)` of the
    /// same tuple type.
    pub const fn cast_batch(batch: &[((usize, usize), L)]) -> &[Triple<L>] {
        // SAFETY: `Triple` is `repr(transparent)` of the same tuple type.
        unsafe { std::mem::transmute(batch) }
    }

    /// Converts a mutable reference to a slice of `((usize, usize), L)` triples
    /// into a mutable reference to a slice of `Triple<L>`.
    ///
    /// The conversion is safe because `Triple` is `repr(transparent)` of the
    /// same tuple type.
    pub const fn cast_batch_mut(batch: &mut [((usize, usize), L)]) -> &mut [Triple<L>] {
        // SAFETY: `Triple` is `repr(transparent)` of the same tuple type.
        unsafe { std::mem::transmute(batch) }
    }
}

const USIZE_BYTES: usize = usize::BITS as usize / 8;

impl<L> RadixKey for Triple<L> {
    const LEVELS: usize = USIZE_BYTES * 2;

    fn get_level(&self, level: usize) -> u8 {
        (if level < USIZE_BYTES {
            self.0.0.1 >> ((level % USIZE_BYTES) * 8)
        } else {
            self.0.0.0 >> ((level % USIZE_BYTES) * 8)
        }) as u8
    }
}

impl<L> PartialEq for Triple<L> {
    fn eq(&self, other: &Self) -> bool {
        self.0.0 == other.0.0
    }
}

impl<L> Eq for Triple<L> {}

impl<L> PartialOrd for Triple<L> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<L> Ord for Triple<L> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.0.cmp(&other.0.0)
    }
}
