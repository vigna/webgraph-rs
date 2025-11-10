/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
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
/// A codec for encoding and decoding batches of triples using gap compression.
///
/// This codec encodes triples of the form `(src, dst, label)` by encoding the
/// gaps between consecutive sources and destinations using a specified code.
///
/// # Type Parameters
///
/// - `S`: Serializer for the labels, implementing [`BitSerializer`] for the label type.
/// - `D`: Deserializer for the labels, implementing [`BitDeserializer`] for the label type.
/// - `SRC_CODE`: Code used for encoding source gaps (default: gamma).
/// - `DST_CODE`: Code used for encoding destination gaps (default: gamma).
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
/// [`GapsIterator`], and there are possible scenarios in which the
/// deserializer might be stateful.
pub struct GapsCodec<
    E: Endianness = NE,
    S: BitSerializer<E, BitWriter<E>> = (),
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Clone = (),
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::DELTA },
> where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    /// Serializer for the labels
    pub serializer: S,
    /// Deserializer for the labels
    pub deserializer: D,
    /// Marker for the endianness
    pub _marker: std::marker::PhantomData<E>,
}

impl<E, S, D, const SRC_CODE: usize, const DST_CODE: usize> GapsCodec<E, S, D, SRC_CODE, DST_CODE>
where
    E: Endianness,
    S: BitSerializer<E, BitWriter<E>> + Send + Sync,
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Send + Sync + Clone,
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    /// Creates a new `GapsCodec` with the given serializer and deserializer.
    pub fn new(serializer: S, deserializer: D) -> Self {
        Self {
            serializer,
            deserializer,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E, S: Default, D: Default, const SRC_CODE: usize, const DST_CODE: usize> core::default::Default
    for GapsCodec<E, S, D, SRC_CODE, DST_CODE>
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

#[derive(Debug, Clone, Copy)]
/// Statistics about the encoding performed by [`GapsCodec`].
pub struct GapsStats {
    /// Total number of triples encoded
    pub total_triples: usize,
    /// Number of bits used for source gaps
    pub src_bits: usize,
    //// Number of bits used for destination gaps
    pub dst_bits: usize,
    /// Number of bits used for labels
    pub labels_bits: usize,
}

impl core::fmt::Display for GapsStats {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let total_bits = self.src_bits + self.dst_bits + self.labels_bits;
        write!(
            f,
            "src: {}B ({:.3} bits / arc), dst: {}B ({:.3} bits / arc), labels: {}B ({:.3} bits / arc), total: {}B ({:.3} bits / arc)",
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

impl<E, S, D, const SRC_CODE: usize, const DST_CODE: usize> BatchCodec
    for GapsCodec<E, S, D, SRC_CODE, DST_CODE>
where
    E: Endianness,
    S: BitSerializer<E, BitWriter<E>> + Send + Sync,
    D: BitDeserializer<E, BitReader<E>, DeserType = S::SerType> + Send + Sync + Clone,
    S::SerType: Send + Sync + Copy + 'static + core::fmt::Debug, // needed by radix sort
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    type Label = S::SerType;
    type DecodedBatch = GapsIterator<E, D, SRC_CODE, DST_CODE>;
    type EncodedBatchStats = GapsStats;

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
        debug_assert!(Triple::cast_batch(batch).is_sorted());
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

        let mut stats = GapsStats {
            total_triples: batch.len(),
            src_bits: 0,
            dst_bits: 0,
            labels_bits: 0,
        };
        // dump the triples to the bitstrea
        let (mut prev_src, mut prev_dst) = (0, 0);
        for ((src, dst), label) in batch.iter() {
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
        Ok(GapsIterator {
            deserializer: self.deserializer.clone(),
            stream,
            len,
            current: 0,
            prev_src: 0,
            prev_dst: 0,
        })
    }
}

#[derive(Clone, Debug)]
/// An iterator over triples encoded with gaps, this is returned by [`GapsCodec`].
pub struct GapsIterator<
    E: Endianness = NE,
    D: BitDeserializer<E, BitReader<E>> = (),
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
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

unsafe impl<
        E: Endianness,
        D: BitDeserializer<E, BitReader<E>>,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > SortedIterator for GapsIterator<E, D, SRC_CODE, DST_CODE>
where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
}

impl<
        E: Endianness,
        D: BitDeserializer<E, BitReader<E>>,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > Iterator for GapsIterator<E, D, SRC_CODE, DST_CODE>
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
}

impl<
        E: Endianness,
        D: BitDeserializer<E, BitReader<E>>,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > ExactSizeIterator for GapsIterator<E, D, SRC_CODE, DST_CODE>
where
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    fn len(&self) -> usize {
        self.len - self.current
    }
}
