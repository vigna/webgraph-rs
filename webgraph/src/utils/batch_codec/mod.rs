/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits and implementations to encode and decode batches of sorted triples
//! to/from disk.
//!
//! The traits and implementations in this module are used to customize the
//! encoding of batches of sorted triples to/from disk. They are used by
//! [`ParSortPairs`], [`ParSortIters`], and the transform functions.
//!
//! [`ParSortPairs`]: crate::utils::par_sort_pairs::ParSortPairs
//! [`ParSortIters`]: crate::utils::par_sort_iters::ParSortIters
//!
//! They usually do not need to be accessed or modified by the end users, albeit
//! in some specific cases where performance or on-disk occupation is critical
//! they can be customized.

use anyhow::Result;

use super::ArcMmapHelper;
use core::fmt::Display;
use dsi_bitstream::prelude::*;
use rdst::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

pub mod gaps;
pub mod grouped_gaps;

/// The recommended default batch codec for unlabeled batches.
///
/// When `DEDUP` is `true`, duplicates are eliminated during batch
/// serialization, reducing I/O and disk usage.
pub type DefaultBatchCodec<const DEDUP: bool = false> = grouped_gaps::GroupedGapsCodec<
    NE,
    (),
    (),
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::GAMMA },
    { dsi_bitstream::dispatch::code_consts::DELTA },
    DEDUP,
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
pub trait BatchCodec: Send + Sync {
    /// The label type of the triples to encode and decode.
    /// While the bounds are not really necessary, in all the practical cases
    /// we need them.
    type Label: Copy + Send + Sync + 'static;
    /// The type returned by `decode_batch`, the iterator of which yields the
    /// decoded triples in sorted order.
    ///
    /// The type `IntoIter` has to be `Send + Sync + Clone` because they are
    /// used in [`SortedGraph`]/[`SortedLabeledGraph`] which require them.
    ///
    /// [`SortedGraph`]: crate::graphs::sorted_graph::SortedGraph
    /// [`SortedLabeledGraph`]: crate::graphs::sorted_graph::SortedLabeledGraph
    type DecodedBatch: IntoIterator<Item = ((usize, usize), Self::Label), IntoIter: Send + Sync + Clone>;

    /// A type representing statistics about the encoded batch.
    ///
    /// It has to implement [`Display`] so that we can log it, and
    /// [`BatchStats`] so that we can query the number of triples actually
    /// encoded (which may differ from the number of arcs pushed when
    /// deduplication is enabled).
    type EncodedBatchStats: BatchStats;

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
    ) -> Result<(usize, Self::EncodedBatchStats)>;

    /// Decodes a batch of triples from disk.
    ///
    /// The returned type's iterator yields the serialized triples in sorted order.
    fn decode_batch(&self, path: impl AsRef<Path>) -> Result<Self::DecodedBatch>;
}

/// Convenience alias to extract the iterator type of the decoded batch from a
/// [`BatchCodec`].
pub type CodecIter<C> = <<C as BatchCodec>::DecodedBatch as IntoIterator>::IntoIter;

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
