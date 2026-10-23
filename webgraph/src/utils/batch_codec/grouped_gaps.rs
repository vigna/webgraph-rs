/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use super::{BitReader, BitWriter, create_batch, impl_batch_iter, num_distinct_pairs, open_batch};
use crate::utils::Triple;
use crate::{
    traits::{BitDeserializer, BitSerializer},
    utils::{AddPolicy, BatchCodec, MergePolicy, NoMerge, humanize},
};

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;

/// A codec for encoding and decoding batches of triples using grouped gap compression.
///
/// This codec encodes triples of the form `(src, dst, label)` by grouping edges
/// with the same source node, and encoding the gaps between consecutive sources
/// and destinations using a specified code (default: gamma). The outdegree
/// (number of edges for each source) is also encoded using the specified code.
///
/// # Type Parameters
///
/// * `SD` - A type implementing both [`BitSerializer`] and [`BitDeserializer`]
///   for the label type. Use [`BitSerDeser`] to combine separate serializer and
///   deserializer implementations.
/// * `OUTDEGREE_CODE` - Code used for encoding outdegrees (default: [ɣ]).
/// * `SRC_CODE` - Code used for encoding source gaps (default: [ɣ]).
/// * `DST_CODE` - Code used for encoding destination gaps (default: [δ]).
/// * `P` - The [merge policy] for repeated pairs (default: [`NoMerge`]).
/// * `L` - The label type of the triples to encode (default: the type
///   serialized by `SD`). Unless `P` merges labels, it must be the type
///   serialized by `SD`; otherwise, `SD` serializes the states of the merger.
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
/// If the policy collapses repeated pairs, the length and the outdegrees
/// count distinct pairs, and each pair is written once, with the label
/// given by the policy.
///
/// `SD` must be [`Clone`] because we need one copy for each
/// [`GroupedGapsIter`], and there are possible scenarios in which the
/// deserializer might be stateful.
///
/// [ɣ]: dsi_bitstream::codes::gamma
/// [δ]: dsi_bitstream::codes::delta
/// [`BitSerDeser`]: crate::traits::BitSerDeser
/// [merge policy]: crate::utils::merge_policy
#[derive(Clone, Debug)]
pub struct GroupedGapsCodec<
    E: Endianness = NE,
    SD: BitSerializer<E, BitWriter<E>>
        + BitDeserializer<E, BitReader<E>, DeserType = <SD as BitSerializer<E, BitWriter<E>>>::SerType>
        + Clone = (),
    const OUTDEGREE_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const SRC_CODE: usize = { dsi_bitstream::dispatch::code_consts::GAMMA },
    const DST_CODE: usize = { dsi_bitstream::dispatch::code_consts::DELTA },
    P = NoMerge,
    L = <SD as BitSerializer<E, BitWriter<E>>>::SerType,
> where
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    /// Serializer and deserializer for the labels.
    pub sd: SD,
    /// The merge policy.
    policy: P,
    _marker: core::marker::PhantomData<(E, fn(L))>,
}

impl<E, SD, const OUTDEGREE_CODE: usize, const SRC_CODE: usize, const DST_CODE: usize>
    GroupedGapsCodec<E, SD, OUTDEGREE_CODE, SRC_CODE, DST_CODE, NoMerge, SD::SerType>
where
    E: Endianness,
    SD: BitSerializer<E, BitWriter<E>>
        + BitDeserializer<E, BitReader<E>, DeserType = SD::SerType>
        + Send
        + Sync
        + Clone,
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    /// Creates a new `GroupedGapsCodec` with the given serializer/deserializer
    /// keeping repeated pairs.
    pub const fn new(sd: SD) -> Self {
        Self::with_policy(sd, NoMerge)
    }
}

impl<E, SD, const OUTDEGREE_CODE: usize, const SRC_CODE: usize, const DST_CODE: usize, P, L>
    GroupedGapsCodec<E, SD, OUTDEGREE_CODE, SRC_CODE, DST_CODE, P, L>
where
    E: Endianness,
    SD: BitSerializer<E, BitWriter<E>>
        + BitDeserializer<E, BitReader<E>, DeserType = SD::SerType>
        + Send
        + Sync
        + Clone,
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    /// Creates a new `GroupedGapsCodec` with the given serializer/deserializer
    /// and [merge policy].
    ///
    /// [merge policy]: crate::utils::merge_policy
    pub const fn with_policy(sd: SD, policy: P) -> Self {
        Self {
            sd,
            policy,
            _marker: core::marker::PhantomData,
        }
    }
}

impl<
    E: Endianness,
    SD: BitSerializer<E, BitWriter<E>>
        + BitDeserializer<E, BitReader<E>, DeserType = SD::SerType>
        + Clone
        + Default,
    const OUTDEGREE_CODE: usize,
    const SRC_CODE: usize,
    const DST_CODE: usize,
    P: Default,
    L,
> Default for GroupedGapsCodec<E, SD, OUTDEGREE_CODE, SRC_CODE, DST_CODE, P, L>
where
    BitReader<E>: BitRead<E>,
    BitWriter<E>: BitWrite<E>,
{
    fn default() -> Self {
        Self {
            sd: SD::default(),
            policy: P::default(),
            _marker: core::marker::PhantomData,
        }
    }
}

/// Statistics about the encoding performed by [`GroupedGapsCodec`].
#[derive(Debug, Clone, Copy)]
pub struct GroupedGapsStats {
    /// Total number of triples encoded
    pub total_triples: usize,
    /// Number of bits used for outdegrees
    pub outdegree_bits: usize,
    /// Number of bits used for source gaps
    pub src_bits: usize,
    /// Number of bits used for destination gaps
    pub dst_bits: usize,
    /// Number of bits used for labels
    pub labels_bits: usize,
}

impl core::fmt::Display for GroupedGapsStats {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "outdegree: {}B ({:.3} bits / triple), src: {}B ({:.3} bits / triple), dst: {}B ({:.3} bits / triple), labels: {}B ({:.3} bits / triple)",
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

impl crate::utils::BatchStats for GroupedGapsStats {
    #[inline]
    fn total_triples(&self) -> usize {
        self.total_triples
    }
}

impl<E, SD, const OUTDEGREE_CODE: usize, const SRC_CODE: usize, const DST_CODE: usize, P, L>
    BatchCodec for GroupedGapsCodec<E, SD, OUTDEGREE_CODE, SRC_CODE, DST_CODE, P, L>
where
    E: Endianness,
    SD: BitSerializer<E, BitWriter<E>>
        + BitDeserializer<E, BitReader<E>, DeserType = SD::SerType>
        + Send
        + Sync
        + Clone,
    SD::SerType: Send + Sync + 'static,
    P: AddPolicy<L, State = SD::SerType> + MergePolicy<SD::SerType>,
    L: Copy + Send + Sync + 'static, // needed by radix sort
    BitReader<E>: BitRead<E> + CodesRead<E>,
    BitWriter<E>: BitWrite<E> + CodesWrite<E>,
{
    type Label = L;
    type State = SD::SerType;
    type Policy = P;
    type DecodedBatch = GroupedGapsIter<E, SD, OUTDEGREE_CODE, SRC_CODE, DST_CODE>;
    type EncodedBatchStats = GroupedGapsStats;

    fn merge_policy(&self) -> P {
        self.policy.clone()
    }

    fn encode_sorted_batch(
        &self,
        path: impl AsRef<std::path::Path>,
        batch: &[((usize, usize), Self::Label)],
    ) -> Result<(usize, Self::EncodedBatchStats)> {
        debug_assert!(Triple::cast_batch(batch).is_sorted(), "Batch is not sorted");
        let (mut stream, batch_len) = create_batch::<E, _>(path.as_ref(), batch, P::COLLAPSES)?;
        let mut stats = GroupedGapsStats {
            total_triples: batch_len,
            outdegree_bits: 0,
            src_bits: 0,
            dst_bits: 0,
            labels_bits: 0,
        };
        // a buffer for the states, reused across runs
        let mut buffer = self.policy.buffer();
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
            let group_size = batch[i..].iter().take_while(|t| t.0.0 == src).count();
            // compute outdegree (distinct destinations when collapsing)
            let outdegree = if P::COLLAPSES {
                num_distinct_pairs(&batch[i..i + group_size])
            } else {
                group_size
            };
            // write the outdegree
            stats.outdegree_bits += ConstCode::<OUTDEGREE_CODE>
                .write(&mut stream, outdegree as _)
                .with_context(|| format!("Could not write outdegree {outdegree} for {src}"))?;
            // encode the destinations
            let mut prev_dst = 0;
            let end = i + group_size;
            while i < end {
                let ((_, dst), _) = batch[i];
                // the run of triples with this destination, if collapsing
                let run_end = if P::COLLAPSES {
                    i + batch[i..end].iter().take_while(|t| t.0.1 == dst).count()
                } else {
                    i + 1
                };
                // write the destination gap
                stats.dst_bits += ConstCode::<DST_CODE>
                    .write(&mut stream, (dst - prev_dst) as _)
                    .with_context(|| format!("Could not write {dst} after {prev_dst}"))?;
                // write the label
                stats.labels_bits += self
                    .sd
                    .serialize(
                        self.policy.state(&mut buffer, &batch[i..run_end]),
                        &mut stream,
                    )
                    .context("Could not serialize label")?;
                prev_dst = dst;
                i = run_end;
            }
            prev_src = src;
        }
        // flush the stream and reset the buffer
        stream.flush().context("Could not flush stream")?;

        let total_bits = stats.outdegree_bits + stats.src_bits + stats.dst_bits + stats.labels_bits;
        Ok((total_bits, stats))
    }

    fn decode_batch(&self, path: impl AsRef<std::path::Path>) -> Result<Self::DecodedBatch> {
        let (stream, len) = open_batch(path.as_ref())?;

        // create the iterator
        Ok(GroupedGapsIter {
            deserializer: self.sd.clone(),
            stream,
            len,
            current: 0,
            src: 0,
            dst_left: 0,
            prev_dst: 0,
        })
    }
}

/// An iterator over triples encoded with gaps, this is returned by [`GroupedGapsCodec`].
#[derive(Clone, Debug)]
pub struct GroupedGapsIter<
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

impl<
    E: Endianness,
    D: BitDeserializer<E, BitReader<E>>,
    const OUTDEGREE_CODE: usize,
    const SRC_CODE: usize,
    const DST_CODE: usize,
> Iterator for GroupedGapsIter<E, D, OUTDEGREE_CODE, SRC_CODE, DST_CODE>
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

    fn count(self) -> usize {
        self.len()
    }
}

impl_batch_iter!(GroupedGapsIter<OUTDEGREE_CODE, SRC_CODE, DST_CODE>);
