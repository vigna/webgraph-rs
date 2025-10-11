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
    utils::BatchCodec,
};

use std::sync::Arc;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use mmap_rs::MmapFlags;
use rdst::*;

#[derive(Clone, Debug, Default)]
/// A codec for encoding and decoding batches of triples using grouped gap compression.
///
/// This codec encodes triples of the form `(src, dst, label)` by grouping edges with the same source node,
/// and encoding the gaps between consecutive sources and destinations using a specified code (default: gamma).
/// The outdegree (number of edges for each source) is also encoded using the specified code.
///
/// ## Type Parameters
/// - `S`: Serializer for the labels, implementing [`BitSerializer`] for the label type.
/// - `D`: Deserializer for the labels, implementing [`BitDeserializer`] for the label type.
/// - `OUTDEGREE_CODE`: Code used for encoding outdegrees (default: gamma).
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
///     - The outdegree (number of edges for this source) is encoded.
///     - For each destination:
///         - The gap from the previous destination is encoded.
///         - The label is serialized.
///
/// The bit deserializer must be [`Clone`] because we need one for each
/// [`GroupedGapsIterator`], and there are possible scenarios in which the
/// deserializer might be stateful.
///
/// ## Choosing the codes
///
/// When transposing `enwiki-2024`, these are the top 10 codes for src gaps, outdegree, and dst gaps:
/// ```ignore
/// Outdegree stats
///   Code: ExpGolomb(3) Size: 34004796
///   Code: ExpGolomb(2) Size: 34101784
///   Code: ExpGolomb(4) Size: 36036394
///   Code: Zeta(2)      Size: 36231582
///   Code: ExpGolomb(1) Size: 36369750
///   Code: Zeta(3)      Size: 36893285
///   Code: Pi(2)        Size: 37415701
///   Code: Zeta(4)      Size: 38905267
///   Code: Golomb(20)   Size: 38963840
///   Code: Golomb(19)   Size: 39118201
/// Src stats
///   Code: Golomb(2)    Size: 12929998
///   Code: Rice(1)      Size: 12929998
///   Code: Unary        Size: 13025332
///   Code: Golomb(1)    Size: 13025332
///   Code: Rice(0)      Size: 13025332
///   Code: ExpGolomb(1) Size: 13319930
///   Code: Golomb(4)    Size: 18732384
///   Code: Rice(2)      Size: 18732384
///   Code: Golomb(3)    Size: 18736573
///   Code: ExpGolomb(2) Size: 18746122
/// Dst stats
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
/// The best codes are `Golomb(2)` for src gaps, `ExpGolomb(3)` for outdegree, and `Pi(2)` for dst gaps.
/// However, `Golomb` can perform poorly if the data don't follow the expected distribution,
/// so the recommended defaults are `Gamma` for src gaps, `ExpGolomb3` for outdegree, and `Delta` for dst gaps,
/// as they are universal codes.
pub struct GroupedGapsCodec<
    S: BitSerializer<NE, BitWriter> = (),
    D: BitDeserializer<NE, BitReader, DeserType = S::SerType> + Clone = (),
    const OUTDEGREE_CODE: usize = { dsi_bitstream::dispatch::code_consts::EXP_GOLOMB3 },
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::DELTA },
> {
    /// Serializer for the labels
    pub serializer: S,
    /// Deserializer for the labels
    pub deserializer: D,
}

impl<S, D, const OUTDEGREE_CODE: usize, const SRC_CODE: usize, const DST_CODE: usize> BatchCodec
    for GroupedGapsCodec<S, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
where
    S: BitSerializer<NE, BitWriter> + Send + Sync,
    D: BitDeserializer<NE, BitReader, DeserType = S::SerType> + Send + Sync + Clone,
    S::SerType: Send + Sync + Copy + 'static, // needed by radix sort
{
    type Label = S::SerType;
    type DecodedBatch = GroupedGapsIterator<D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>;

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
        let mut stream = <BufBitWriter<NE, _>>::new(<WordAdapter<usize, _>>::new(file));

        // prefix the stream with the length of the batch
        // we use a delta code since it'll be a big number most of the time
        stream
            .write_delta(batch.len() as u64)
            .context("Could not write length")?;

        // dump the triples to the bitstream
        let mut prev_src = 0;
        let mut written_bits = 0;
        let mut i = 0;
        while i < batch.len() {
            let ((src, _), _) = batch[i];
            // write the source gap as gamma
            written_bits += ConstCode::<SRC_CODE>
                .write(&mut stream, (src - prev_src) as _)
                .with_context(|| format!("Could not write {src} after {prev_src}"))?;
            // figure out how many edges have this source
            let outdegree = batch[i..].iter().take_while(|t| t.0 .0 == src).count();
            // write the outdegree
            written_bits += ConstCode::<OUTDEGREE_CODE>
                .write(&mut stream, outdegree as _)
                .with_context(|| format!("Could not write outdegree {outdegree} for {src}"))?;

            // encode the destinations
            let mut prev_dst = 0;
            for _ in 0..outdegree {
                let ((_, dst), label) = &batch[i];
                // write the destination gap as gamma
                written_bits += ConstCode::<DST_CODE>
                    .write(&mut stream, (dst - prev_dst) as _)
                    .with_context(|| format!("Could not write {dst} after {prev_dst}"))?;
                // write the label
                written_bits += self
                    .serializer
                    .serialize(label, &mut stream)
                    .context("Could not serialize label")?;
                prev_dst = *dst;
                i += 1;
            }
            prev_src = src;
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
    D: BitDeserializer<NE, BitReader> = (),
    const OUTDEGREE_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
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
    /// Current source node
    src: usize,
    /// Number of destinations left for the current source
    dst_left: usize,
    /// Previous destination node
    prev_dst: usize,
}

unsafe impl<
        D: BitDeserializer<NE, BitReader>,
        const OUTDEGREE_CODE: usize,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > SortedIterator for GroupedGapsIterator<D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
{
}

impl<
        D: BitDeserializer<NE, BitReader>,
        const OUTDEGREE_CODE: usize,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > Iterator for GroupedGapsIterator<D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
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
        D: BitDeserializer<NE, BitReader>,
        const OUTDEGREE_CODE: usize,
        const SRC_CODE: usize,
        const DST_CODE: usize,
    > ExactSizeIterator for GroupedGapsIterator<D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
{
    fn len(&self) -> usize {
        self.len - self.current
    }
}
