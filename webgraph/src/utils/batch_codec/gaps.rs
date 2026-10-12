/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use super::{BitReader, BitWriter, create_batch, impl_batch_iter, open_batch};
use crate::utils::Triple;
use crate::{
    traits::{BitDeserializer, BitSerializer},
    utils::{BatchCodec, humanize},
};

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;

/// A codec for encoding and decoding batches of triples using gap compression.
///
/// This codec encodes triples of the form `(src, dst, label)` by encoding the
/// gaps between consecutive sources and destinations using a specified code.
///
/// # Type Parameters
///
/// * `S` - Serializer for the labels, implementing [`BitSerializer`] for the label type.
/// * `D` - Deserializer for the labels, implementing [`BitDeserializer`] for the label type.
/// * `SRC_CODE` - Code used for encoding source gaps (default: gamma).
/// * `DST_CODE` - Code used for encoding destination gaps (default: gamma).
///
/// # Encoding Format
///
/// 1. The batch length is written using delta coding.
/// 2. For each group of triples with the same source:
///     - The gap from the previous source is encoded.
///     - The gap from the previous destination is encoded.
///     - The label is serialized.
///
/// The bit deserializer must be [`Clone`] because we need one for each
/// [`GapsIter`], and there are possible scenarios in which the
/// deserializer might be stateful.
#[derive(Clone, Debug)]
pub struct GapsCodec<
    E: Endianness = NE,
    S: BitSerializer<E, BitWriter<E>> = (),
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Clone = (),
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::DELTA },
    const DEDUP: bool = false,
> where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    /// Serializer for the labels
    pub serializer: S,
    /// Deserializer for the labels
    pub deserializer: D,
    /// Marker for the endianness
    _marker: std::marker::PhantomData<E>,
}

impl<E, S, D, const SRC_CODE: usize, const DST_CODE: usize, const DEDUP: bool>
    GapsCodec<E, S, D, SRC_CODE, DST_CODE, DEDUP>
where
    E: Endianness,
    S: BitSerializer<E, BitWriter<E>> + Send + Sync,
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Send + Sync + Clone,
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    /// Creates a new `GapsCodec` with the given serializer and deserializer.
    pub const fn new(serializer: S, deserializer: D) -> Self {
        Self {
            serializer,
            deserializer,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E, S: Default, D: Default, const SRC_CODE: usize, const DST_CODE: usize, const DEDUP: bool>
    core::default::Default for GapsCodec<E, S, D, SRC_CODE, DST_CODE, DEDUP>
where
    E: Endianness,
    S: BitSerializer<E, BitWriter<E>> + Send + Sync,
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Send + Sync + Clone,
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    fn default() -> Self {
        Self::new(Default::default(), Default::default())
    }
}

/// Statistics about the encoding performed by [`GapsCodec`].
#[derive(Debug, Clone, Copy)]
pub struct GapsStats {
    /// Total number of triples encoded
    pub total_triples: usize,
    /// Number of bits used for source gaps
    pub src_bits: usize,
    /// Number of bits used for destination gaps
    pub dst_bits: usize,
    /// Number of bits used for labels
    pub labels_bits: usize,
}

impl core::fmt::Display for GapsStats {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let total_bits = self.src_bits + self.dst_bits + self.labels_bits;
        write!(
            f,
            "src: {}B ({:.3} bits / triple), dst: {}B ({:.3} bits / triple), labels: {}B ({:.3} bits / triple), total: {}B ({:.3} bits / triple)",
            humanize(self.src_bits as f64 / 8.0),
            self.src_bits as f64 / self.total_triples as f64,
            humanize(self.dst_bits as f64 / 8.0),
            self.dst_bits as f64 / self.total_triples as f64,
            humanize(self.labels_bits as f64 / 8.0),
            self.labels_bits as f64 / self.total_triples as f64,
            humanize(total_bits as f64 / 8.0),
            total_bits as f64 / self.total_triples as f64,
        )
    }
}

impl crate::utils::BatchStats for GapsStats {
    #[inline]
    fn total_triples(&self) -> usize {
        self.total_triples
    }
}

impl<E, S, D, const SRC_CODE: usize, const DST_CODE: usize, const DEDUP: bool> BatchCodec
    for GapsCodec<E, S, D, SRC_CODE, DST_CODE, DEDUP>
where
    E: Endianness,
    S: BitSerializer<E, BitWriter<E>> + Send + Sync,
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Send + Sync + Clone,
    S::SerType: Send + Sync + Copy + 'static, // needed by radix sort
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    type Label = S::SerType;
    type DecodedBatch = GapsIter<E, D, SRC_CODE, DST_CODE>;
    type EncodedBatchStats = GapsStats;

    fn encode_sorted_batch(
        &self,
        path: impl AsRef<std::path::Path>,
        batch: &[((usize, usize), Self::Label)],
    ) -> Result<(usize, Self::EncodedBatchStats)> {
        debug_assert!(Triple::cast_batch(batch).is_sorted());
        let (mut stream, batch_len) = create_batch::<E, _>(path.as_ref(), batch, DEDUP)?;

        let mut stats = GapsStats {
            total_triples: batch_len,
            src_bits: 0,
            dst_bits: 0,
            labels_bits: 0,
        };
        // Dump the triples to the bitstream
        let (mut prev_src, mut prev_dst) = (0, 0);
        let mut prev_pair: Option<(usize, usize)> = None;
        for ((src, dst), label) in batch.iter() {
            if DEDUP {
                if prev_pair == Some((*src, *dst)) {
                    continue;
                }
                prev_pair = Some((*src, *dst));
            }
            // write the source gap as gamma
            stats.src_bits += ConstCode::<SRC_CODE>
                .write(&mut stream, (src - prev_src) as u64)
                .with_context(|| format!("Could not write {src} after {prev_src}"))?;
            if *src != prev_src {
                // Reset prev_y
                prev_dst = 0;
            }
            // write the destination gap as gamma
            stats.dst_bits += ConstCode::<DST_CODE>
                .write(&mut stream, (dst - prev_dst) as u64)
                .with_context(|| format!("Could not write {dst} after {prev_dst}"))?;
            // write the label
            stats.labels_bits += self
                .serializer
                .serialize(label, &mut stream)
                .context("Could not serialize label")?;
            (prev_src, prev_dst) = (*src, *dst);
        }
        // flush the stream and reset the buffer
        stream.flush().context("Could not flush stream")?;

        let total_bits = stats.src_bits + stats.dst_bits + stats.labels_bits;
        Ok((total_bits, stats))
    }

    fn decode_batch(&self, path: impl AsRef<std::path::Path>) -> Result<Self::DecodedBatch> {
        let (stream, len) = open_batch(path.as_ref())?;

        // create the iterator
        Ok(GapsIter {
            deserializer: self.deserializer.clone(),
            stream,
            len,
            current: 0,
            prev_src: 0,
            prev_dst: 0,
        })
    }
}

/// An iterator over triples encoded with gaps, this is returned by [`GapsCodec`].
#[derive(Clone, Debug)]
pub struct GapsIter<
    E: Endianness = NE,
    D: BitDeserializer<E, BitReader<E>> = (),
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::DELTA },
> where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    /// Deserializer for the labels
    deserializer: D,
    /// Bitstream to read from
    stream: BitReader<E>,
    /// Length of the iterator (number of triples)
    len: usize,
    /// Current position in the iterator
    current: usize,
    /// Previous source node
    prev_src: usize,
    /// Previous destination node
    prev_dst: usize,
}

impl<
    E: Endianness,
    D: BitDeserializer<E, BitReader<E>>,
    const SRC_CODE: usize,
    const DST_CODE: usize,
> Iterator for GapsIter<E, D, SRC_CODE, DST_CODE>
where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    type Item = ((usize, usize), D::DeserType);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.len {
            return None;
        }
        let src_gap = ConstCode::<SRC_CODE>.read(&mut self.stream).ok()?;
        let dst_gap = ConstCode::<DST_CODE>.read(&mut self.stream).ok()?;
        let label = self.deserializer.deserialize(&mut self.stream).ok()?;
        self.prev_src += src_gap as usize;
        if src_gap != 0 {
            self.prev_dst = 0;
        }
        self.prev_dst += dst_gap as usize;
        self.current += 1;
        Some(((self.prev_src, self.prev_dst), label))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }

    fn count(self) -> usize {
        self.len()
    }
}

impl_batch_iter!(GapsIter<SRC_CODE, DST_CODE>);
