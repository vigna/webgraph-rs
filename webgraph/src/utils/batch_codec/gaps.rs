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
    utils::BatchCodec,
};

use std::sync::Arc;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use mmap_rs::MmapFlags;
use rdst::*;

#[derive(Clone, Debug, Default)]
/// A codec for encoding and decoding batches of triples using gap compression.
///
/// This codec encodes triples of the form `(src, dst, label)` by encoding the
/// gaps between consecutive sources and destinations using a specified code.
///
/// ## Type Parameters
/// - `S`: Serializer for the labels, implementing [`BitSerializer`] for the label type.
/// - `D`: Deserializer for the labels, implementing [`BitDeserializer`] for the label type.
/// - `SRC_CODE`: Code used for encoding source gaps (default: gamma).
/// - `DST_CODE`: Code used for encoding destination gaps (default: gamma).
///
/// ## Fields
/// - `serializer`: The label serializer.
/// - `deserializer`: The label deserializer.
///
/// ## Encoding Format
/// 1. The batch length is written using delta coding.
/// 2. For each group of triples with the same source:
///     - The gap from the previous source is encoded.
///     - The gap from the previous destination is encoded.
///     - The label is serialized.
///
/// The bit deserializer must be [`Clone`] because we need one for each
/// [`GapsIterator`], and there are possible scenarios in which the
/// deserializer might be stateful.
///
/// ## Choosing the codes
///
/// These are the top 10 codes for src and dst gaps when transposing `enwiki-2024`.
/// ```ignore
/// Src codes:
///   Code: Unary        Size: 179553432
///   Code: Golomb(1)    Size: 179553432
///   Code: Rice(0)      Size: 179553432
///   Code: Gamma        Size: 185374984
///   Code: Zeta(1)      Size: 185374984
///   Code: ExpGolomb(0) Size: 185374984
///   Code: Omega        Size: 185439656
///   Code: Delta        Size: 191544794
///   Code: Golomb(2)    Size: 345986198
///   Code: Rice(1)      Size: 345986198
/// Dst codes:
///   Code: Pi(2)   Size: 2063880685
///   Code: Pi(3)   Size: 2074138948
///   Code: Zeta(3) Size: 2122730298
///   Code: Zeta(4) Size: 2123948774
///   Code: Zeta(5) Size: 2169131998
///   Code: Pi(4)   Size: 2176097847
///   Code: Zeta(2) Size: 2226573622
///   Code: Zeta(6) Size: 2237680403
///   Code: Delta   Size: 2272691460
///   Code: Zeta(7) Size: 2305354857
/// ```
///
/// So the best combination is `Unary` for src gaps and `Pi(2)` for dst gaps.
/// But, `Unary` can behave poorly if the distribution of your data changes,
/// therefore the recommended default is `Gamma` for src gaps and `Delta` for
/// dst gaps as they are universal codes.
pub struct GapsCodec<
    S: BitSerializer<NE, BitWriter> = (),
    D: BitDeserializer<NE, BitReader, DeserType = S::SerType> + Clone = (),
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::DELTA },
> {
    /// Serializer for the labels
    pub serializer: S,
    /// Deserializer for the labels
    pub deserializer: D,
}

impl<S, D, const SRC_CODE: usize, const DST_CODE: usize> BatchCodec
    for GapsCodec<S, D, SRC_CODE, DST_CODE>
where
    S: BitSerializer<NE, BitWriter> + Send + Sync,
    D: BitDeserializer<NE, BitReader, DeserType = S::SerType> + Send + Sync + Clone,
    S::SerType: Send + Sync + Copy + 'static + core::fmt::Debug, // needed by radix sort
{
    type Label = S::SerType;
    type DecodedBatch = GapsIterator<D, SRC_CODE, DST_CODE>;

    fn encode_batch(
        &self,
        path: impl AsRef<std::path::Path>,
        batch: &mut [((usize, usize), Self::Label)],
    ) -> Result<usize> {
        let start = std::time::Instant::now();
        Triple::cast_batch_mut(batch).radix_sort_unstable();
        log::debug!("Sorted {} arcs in {:?}", batch.len(), start.elapsed());
        self.encode_sorted_batch(path, batch)
    }

    fn encode_sorted_batch(
        &self,
        path: impl AsRef<std::path::Path>,
        batch: &[((usize, usize), Self::Label)],
    ) -> Result<usize> {
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
        let mut stream = <BufBitWriter<NE, _>>::new(<WordAdapter<usize, _>>::new(file));

        // prefix the stream with the length of the batch
        // we use a delta code since it'll be a big number most of the time
        stream
            .write_delta(batch.len() as u64)
            .context("Could not write length")?;

        // dump the triples to the bitstream
        let (mut prev_src, mut prev_dst) = (0, 0);
        let mut written_bits = 0;
        for ((src, dst), label) in batch.iter() {
            // write the source gap as gamma
            written_bits += ConstCode::<SRC_CODE>
                .write(&mut stream, (src - prev_src) as u64)
                .with_context(|| format!("Could not write {src} after {prev_src}"))?;
            if *src != prev_src {
                // Reset prev_y
                prev_dst = 0;
            }
            // write the destination gap as gamma
            written_bits += ConstCode::<DST_CODE>
                .write(&mut stream, (dst - prev_dst) as u64)
                .with_context(|| format!("Could not write {dst} after {prev_dst}"))?;
            // write the label
            written_bits += self
                .serializer
                .serialize(label, &mut stream)
                .context("Could not serialize label")?;
            (prev_src, prev_dst) = (*src, *dst);
        }
        // flush the stream and reset the buffer
        written_bits += stream.flush().context("Could not flush stream")?;

        Ok(written_bits)
    }

    fn decode_batch(&self, path: impl AsRef<std::path::Path>) -> Result<Self::DecodedBatch> {
        // open the file
        let mut stream = <BufBitReader<NE, _>>::new(MemWordReader::new(ArcMmapHelper(Arc::new(
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
    D: BitDeserializer<NE, BitReader> = (),
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
> {
    /// Deserializer for the labels
    deserializer: D,
    /// Bitstream to read from
    stream: BitReader,
    /// Length of the iterator (number of triples)
    len: usize,
    /// Current position in the iterator
    current: usize,
    /// Previous source node
    prev_src: usize,
    /// Previous destination node
    prev_dst: usize,
}

unsafe impl<D: BitDeserializer<NE, BitReader>, const SRC_CODE: usize, const DST_CODE: usize>
    SortedIterator for GapsIterator<D, SRC_CODE, DST_CODE>
{
}

impl<D: BitDeserializer<NE, BitReader>, const SRC_CODE: usize, const DST_CODE: usize> Iterator
    for GapsIterator<D, SRC_CODE, DST_CODE>
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

impl<D: BitDeserializer<NE, BitReader>, const SRC_CODE: usize, const DST_CODE: usize>
    ExactSizeIterator for GapsIterator<D, SRC_CODE, DST_CODE>
{
    fn len(&self) -> usize {
        self.len - self.current
    }
}
