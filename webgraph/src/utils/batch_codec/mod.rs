/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;

use super::ArcMmapHelper;
use dsi_bitstream::prelude::*;
use rdst::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

pub mod gaps;
pub mod grouped_gaps;

/// The recommended default batch codec for unlabelled batches.
pub type DefaultBatchCodec = grouped_gaps::GroupedGapsCodec;

pub type BitWriter<E> = BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>;
pub type BitReader<E> = BufBitReader<E, MemWordReader<u32, ArcMmapHelper<u32>>>;

/// A trait for encoding and decoding batches of sorted triples.
pub trait BatchCodec: Send + Sync {
    /// The label type of the triples to encode and decode.
    /// While the bounds are not really necessary, in all the practical cases
    /// we need them.
    type Label: Copy + Send + Sync + 'static;
    //// The type returned by `decode_batch`, the iterator of which yields the
    //// decoded triples in sorted order.
    ///
    /// The type `IntoIter` has to be `Send + Sync + Clone` because most often we want
    /// to use them in [`SortPairs`](crate::utils::sort_pairs::SortPairs) and
    /// then in [`ArcListGraph`](crate::graphs::arc_list_graph::ArcListGraph)
    /// which require them.
    type DecodedBatch: IntoIterator<
        Item = ((usize, usize), Self::Label),
        IntoIter: Send + Sync + Clone,
    >;

    /// Given a batch of sorted triples, encodes them to disk and returns the number of bits written.
    fn encode_sorted_batch(
        &self,
        path: impl AsRef<Path>,
        batch: &[((usize, usize), Self::Label)],
    ) -> Result<usize>;

    /// Given a batch of triples, encodes them to disk and returns the number of bits written.
    /// The batch needs a mutable reference to allow the coded to sort-in-place if needed.
    fn encode_batch(
        &self,
        path: impl AsRef<Path>,
        batch: &mut [((usize, usize), Self::Label)],
    ) -> Result<usize>;

    /// Decodes a batch of triples from disk.
    /// The returned type's iterator yields the serialized triples in sorted order.
    fn decode_batch(&self, path: impl AsRef<Path>) -> Result<Self::DecodedBatch>;
}

/// Convenience alias to extract the iterator type of the decoded batch from a [`BatchCodec`].
pub type CodecIter<C> = <<C as BatchCodec>::DecodedBatch as IntoIterator>::IntoIter;

/// An arc expressed as a pair of nodes and the associated label.
///
/// Equality and order are defined only (lexicographically) on the pair of
/// nodes.
///
/// Since we use this to sort a batch of `(usize, usize, L)` triples, in order to
/// safely transmute between the two types, Triple HAS TO be `repr(transparent)`
/// of the same tuple type.
///
/// We use this to implement `RadixKey` for sorting batches of triples
/// using `rdst`.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Triple<L>(((usize, usize), L));

impl<L> Triple<L> {
    /// Converts a mutable batch of `((usize, usize), L)` triples into a mutable slice of `Triple<L>`.
    ///
    /// This is safe because `Triple` is `repr(transparent)` of the same tuple type.
    pub fn cast_batch_mut(batch: &mut [((usize, usize), L)]) -> &mut [Triple<L>] {
        unsafe { std::mem::transmute(batch) }
    }
    /// Converts a batch of `((usize, usize), L)` triples into a slice of `Triple<L>`.
    ///
    /// This is safe because `Triple` is `repr(transparent)` of the same tuple type.
    pub fn cast_batch(batch: &[((usize, usize), L)]) -> &[Triple<L>] {
        unsafe { std::mem::transmute(batch) }
    }
}

impl<L> RadixKey for Triple<L> {
    const LEVELS: usize = 16;

    fn get_level(&self, level: usize) -> u8 {
        (if level < 8 {
            self.0 .0 .1 >> ((level % 8) * 8)
        } else {
            self.0 .0 .0 >> ((level % 8) * 8)
        }) as u8
    }
}

impl<L> PartialEq for Triple<L> {
    fn eq(&self, other: &Self) -> bool {
        self.0 .0 == other.0 .0
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
        self.0 .0.cmp(&other.0 .0)
    }
}
