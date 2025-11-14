/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{BitReader, BitWriter};
use crate::traits::SortedIterator;
use crate::utils::{ArcMmapHelper, MmapHelper, Triple};
use crate::{
    traits::{BitDeserializer, BitSerializer},
    utils::{humanize, BatchCodec},
};

use std::sync::Arc;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use mmap_rs::MmapFlags;
use rdst::*;

#[derive(Clone, Debug)]
/// A codec for encoding and decoding batches of triples using grouped gap compression.
///
/// This codec encodes triples of the form `(src, dst, label)` by grouping edges
/// with the same source node, and encoding the gaps between consecutive sources
/// and destinations using a specified code (default: gamma). The outdegree
/// (number of edges for each source) is also encoded using the specified code.
///
/// # Type Parameters
///
/// - `S`: Serializer for the labels, implementing [`BitSerializer`] for the label type.
/// - `D`: Deserializer for the labels, implementing [`BitDeserializer`] for the label type.
/// - `OUTDEGREE_CODE`: Code used for encoding outdegrees (default: [ɣ](dsi_bitstream::codes::gamma)).
/// - `SRC_CODE`: Code used for encoding source gaps (default: [ɣ](dsi_bitstream::codes::gamma)).
/// - `DST_CODE`: Code used for encoding destination gaps (default: [ɣ](dsi_bitstream::codes::gamma)).
///
/// # Encoding Format
///
/// 1. The batch length is written using delta coding.
/// 2. For each group of triples with the same source:
///     - The gap from the previous source is encoded.
///     - The outdegree (number of edges for this source) is encoded.
///     - For each destination:
///         - The gap from the previous destination is encoded.
///         - The label is serialized.
///
/// The bit deserializer must be [`Clone`] because we need one for each
/// [`GroupedGapsIterator`], and there are possible scenarios in which the
/// deserializer might be stateful.
pub struct GroupedGapsCodec<
    E: Endianness = NE,
    S: BitSerializer<E, BitWriter<E>> = (),
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Clone = (),
    const OUTDEGREE_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::DELTA },
> where
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    /// Serializer for the labels.
    pub serializer: S,
    /// Deserializer for the labels.
    pub deserializer: D,

    pub _marker: core::marker::PhantomData<E>,
}

impl<E, S, D, const OUTDEGREE_CODE: usize, const SRC_CODE: usize, const DST_CODE: usize>
    GroupedGapsCodec<E, S, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
where
    E: Endianness,
    S: BitSerializer<E, BitWriter<E>> + Send + Sync,
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Send + Sync + Clone,
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    /// Creates a new `GroupedGapsCodec` with the given serializer and deserializer.
    pub fn new(serializer: S, deserializer: D) -> Self {
        Self {
            serializer,
            deserializer,
            _marker: core::marker::PhantomData,
        }
    }
}

impl<
        E: Endianness,
        S: BitSerializer<E, BitWriter<E>> + Default,
        D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Clone + Default,
        const OUTDEGREE_CODE: usize,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > Default for GroupedGapsCodec<E, S, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
where
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    fn default() -> Self {
        Self {
            serializer: S::default(),
            deserializer: D::default(),
            _marker: core::marker::PhantomData,
        }
    }
}

#[derive(Debug, Clone, Copy)]
/// Statistics about the encoding performed by
/// [`GapsCodec`](crate::utils::gaps::GapsCodec).
pub struct GroupedGapsStats {
    /// Total number of triples encoded
    pub total_triples: usize,
    /// Number of bits used for outdegrees
    pub outdegree_bits: usize,
    /// Number of bits used for source gaps
    pub src_bits: usize,
    //// Number of bits used for destination gaps
    pub dst_bits: usize,
    /// Number of bits used for labels
    pub labels_bits: usize,
}

impl core::fmt::Display for GroupedGapsStats {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "outdegree: {}B ({:.3} bits / arc), src: {}B ({:.3} bits / arc), dst: {}B ({:.3} bits / arc), labels: {}B ({:.3} bits / arc)",
            humanize(self.outdegree_bits as f64 / 8.0),
            self.outdegree_bits as f64 / self.total_triples as f64,
            humanize(self.src_bits as f64 / 8.0),
            self.src_bits as f64 / self.total_triples as f64,
            humanize(self.dst_bits as f64 / 8.0),
            self.dst_bits as f64 / self.total_triples as f64,
            humanize(self.labels_bits as f64 / 8.0),
            self.labels_bits as f64 / self.total_triples as f64,
        )
    }
}

impl<E, S, D, const OUTDEGREE_CODE: usize, const SRC_CODE: usize, const DST_CODE: usize> BatchCodec
    for GroupedGapsCodec<E, S, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
where
    E: Endianness,
    S: BitSerializer<E, BitWriter<E>> + Send + Sync,
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Send + Sync + Clone,
    S::SerType: Send + Sync + Copy + 'static, // needed by radix sort
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    type Label = S::SerType;
    type DecodedBatch = GroupedGapsIterator<E, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>;
    type EncodedBatchStats = GroupedGapsStats;

    fn encode_batch(
        &self,
        path: impl AsRef<std::path::Path>,
        batch: &mut [((usize, usize), Self::Label)],
    ) -> Result<(usize, Self::EncodedBatchStats)> {
        let start = std::time::Instant::now();
        Triple::cast_batch_mut(batch).radix_sort_unstable();
        log::debug!("Sorted {} arcs in {:?}", batch.len(), start.elapsed());
        self.encode_sorted_batch(path, batch)
    }

    fn encode_sorted_batch(
        &self,
        path: impl AsRef<std::path::Path>,
        batch: &[((usize, usize), Self::Label)],
    ) -> Result<(usize, Self::EncodedBatchStats)> {
        debug_assert!(Triple::cast_batch(batch).is_sorted(), "Batch is not sorted");
        // create a batch file where to dump
        let file_path = path.as_ref();
        let file = std::io::BufWriter::with_capacity(
            1 << 16,
            std::fs::File::create(file_path).with_context(|| {
                format!(
                    "Could not create BatchIterator temporary file {}",
                    file_path.display()
                )
            })?,
        );
        // create a bitstream to write to the file
        let mut stream = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(file));

        // prefix the stream with the length of the batch
        // we use a delta code since it'll be a big number most of the time
        stream
            .write_delta(batch.len() as u64)
            .context("Could not write length")?;

        let mut stats = GroupedGapsStats {
            total_triples: batch.len(),
            outdegree_bits: 0,
            src_bits: 0,
            dst_bits: 0,
            labels_bits: 0,
        };
        // dump the triples to the bitstream
        let mut prev_src = 0;
        let mut i = 0;
        while i < batch.len() {
            let ((src, _), _) = batch[i];
            // write the source gap as gamma
            stats.src_bits += ConstCode::<SRC_CODE>
                .write(&mut stream, (src - prev_src) as _)
                .with_context(|| format!("Could not write {src} after {prev_src}"))?;
            // figure out how many edges have this source
            let outdegree = batch[i..].iter().take_while(|t| t.0 .0 == src).count();
            // write the outdegree
            stats.outdegree_bits += ConstCode::<OUTDEGREE_CODE>
                .write(&mut stream, outdegree as _)
                .with_context(|| format!("Could not write outdegree {outdegree} for {src}"))?;

            // encode the destinations
            let mut prev_dst = 0;
            for _ in 0..outdegree {
                let ((_, dst), label) = &batch[i];
                // write the destination gap as gamma
                stats.dst_bits += ConstCode::<DST_CODE>
                    .write(&mut stream, (dst - prev_dst) as _)
                    .with_context(|| format!("Could not write {dst} after {prev_dst}"))?;
                // write the label
                stats.labels_bits += self
                    .serializer
                    .serialize(label, &mut stream)
                    .context("Could not serialize label")?;
                prev_dst = *dst;
                i += 1;
            }
            prev_src = src;
        }
        // flush the stream and reset the buffer
        stream.flush().context("Could not flush stream")?;

        let total_bits = stats.outdegree_bits + stats.src_bits + stats.dst_bits + stats.labels_bits;
        Ok((total_bits, stats))
    }

    fn decode_batch(&self, path: impl AsRef<std::path::Path>) -> Result<Self::DecodedBatch> {
        // open the file
        let mut stream = <BufBitReader<E, _>>::new(MemWordReader::new(ArcMmapHelper(Arc::new(
            MmapHelper::mmap(
                path.as_ref(),
                MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SEQUENTIAL,
            )
            .with_context(|| format!("Could not mmap {}", path.as_ref().display()))?,
        ))));

        // read the length of the batch (first value in the stream)
        let len = stream.read_delta().context("Could not read length")? as usize;

        // create the iterator
        Ok(GroupedGapsIterator {
            deserializer: self.deserializer.clone(),
            stream,
            len,
            current: 0,
            src: 0,
            dst_left: 0,
            prev_dst: 0,
        })
    }
}

#[derive(Clone, Debug)]
/// An iterator over triples encoded with gaps, this is returned by [`GroupedGapsCodec`].
pub struct GroupedGapsIterator<
    E: Endianness = NE,
    D: BitDeserializer<E, BitReader<E>> = (),
    const OUTDEGREE_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
> where
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    /// Deserializer for the labels
    deserializer: D,
    /// Bitstream to read from
    stream: BitReader<E>,
    /// Length of the iterator (number of triples)
    len: usize,
    /// Current position in the iterator
    current: usize,
    /// Current source node
    src: usize,
    /// Number of destinations left for the current source
    dst_left: usize,
    /// Previous destination node
    prev_dst: usize,
}

unsafe impl<
        E: Endianness,
        D: BitDeserializer<E, BitReader<E>>,
        const OUTDEGREE_CODE: usize,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > SortedIterator for GroupedGapsIterator<E, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
}

impl<
        E: Endianness,
        D: BitDeserializer<E, BitReader<E>>,
        const OUTDEGREE_CODE: usize,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > Iterator for GroupedGapsIterator<E, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    type Item = ((usize, usize), D::DeserType);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.len {
            return None;
        }
        if self.dst_left == 0 {
            // read a new source
            let src_gap = ConstCode::<SRC_CODE>.read(&mut self.stream).ok()?;
            self.src += src_gap as usize;
            // read the outdegree
            self.dst_left = ConstCode::<OUTDEGREE_CODE>.read(&mut self.stream).ok()? as usize;
            self.prev_dst = 0;
        }

        let dst_gap = ConstCode::<DST_CODE>.read(&mut self.stream).ok()?;
        let label = self.deserializer.deserialize(&mut self.stream).ok()?;
        self.prev_dst += dst_gap as usize;
        self.current += 1;
        self.dst_left -= 1;
        Some(((self.src, self.prev_dst), label))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

impl<
        E: Endianness,
        D: BitDeserializer<E, BitReader<E>>,
        const OUTDEGREE_CODE: usize,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > ExactSizeIterator for GroupedGapsIterator<E, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    fn len(&self) -> usize {
        self.len - self.current
    }
}
