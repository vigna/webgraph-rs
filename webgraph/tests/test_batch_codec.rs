/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

use anyhow::{Result, ensure};
use dsi_bitstream::dispatch::code_consts::{DELTA, GAMMA};
use dsi_bitstream::prelude::*;
use webgraph::traits::{BitDeserializer, BitSerDeser, BitSerializer, CountRepeats, FixedWidth};
use webgraph::utils::gaps::GapsCodec;
use webgraph::utils::grouped_gaps::GroupedGapsCodec;
use webgraph::utils::{BatchCodec, BatchStats, WithMerge};

/// Encodes a batch with `codec`, both unsorted and sorted, and checks the
/// statistics, the length of the file, and the decoded triples.
fn check_codec<C: BatchCodec<Label = ()>>(codec: &C) -> Result<()>
where
    <C::DecodedBatch as IntoIterator>::IntoIter: ExactSizeIterator,
{
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("batch");
    let mut batch = vec![((2, 3), ()), ((0, 1), ()), ((1, 2), ()), ((0, 2), ())];
    let expected = [(0, 1), (0, 2), (1, 2), (2, 3)];

    let (bits, stats) = codec.encode_batch(&path, &mut batch)?;
    assert_eq!(stats.total_triples(), 4);
    for (encoded_bits, encoded) in [
        (bits, true),
        // The batch has been sorted by encode_batch
        (codec.encode_sorted_batch(&path, &batch)?.0, false),
    ] {
        // The file contains the bits written, padded to a word
        let file_len = std::fs::metadata(&path)?.len() as usize;
        assert_eq!(
            file_len,
            encoded_bits.div_ceil(usize::BITS as usize) * size_of::<usize>(),
            "sorted by encode_batch: {encoded}"
        );
        let iter = codec.decode_batch(&path)?.into_iter();
        assert_eq!(iter.len(), 4);
        assert_eq!(iter.size_hint(), (4, Some(4)));
        assert_eq!(iter.map(|(pair, _)| pair).collect::<Vec<_>>(), expected);
    }
    Ok(())
}

#[test]
fn test_gaps_codec() -> Result<()> {
    check_codec(&GapsCodec::<BE, (), ()>::default())
}

#[test]
fn test_grouped_gaps_codec() -> Result<()> {
    check_codec(&GroupedGapsCodec::<BE, ()>::new(()))
}

/// Encodes with `codec`, which counts repeated pairs, a batch with repeated
/// pairs, and checks the statistics and the decoded triples.
fn check_merging_codec<C: BatchCodec<Label = u32, State = u64>>(codec: &C) -> Result<()>
where
    <C::DecodedBatch as IntoIterator>::IntoIter: ExactSizeIterator,
{
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("batch");
    let mut batch = vec![
        ((2, 3), 9),
        ((0, 1), 9),
        ((2, 3), 9),
        ((0, 1), 9),
        ((0, 2), 9),
        ((0, 1), 9),
    ];
    let (_, stats) = codec.encode_batch(&path, &mut batch)?;
    assert_eq!(stats.total_triples(), 3);
    let iter = codec.decode_batch(&path)?.into_iter();
    assert_eq!(iter.len(), 3);
    assert_eq!(
        iter.collect::<Vec<_>>(),
        [((0, 1), 3), ((0, 2), 1), ((2, 3), 2)]
    );
    Ok(())
}

#[test]
fn test_grouped_gaps_codec_merge() -> Result<()> {
    check_merging_codec(
        &GroupedGapsCodec::<BE, _, GAMMA, GAMMA, DELTA, _, u32>::with_policy(
            FixedWidth::<u64>::new(),
            WithMerge(CountRepeats),
        ),
    )
}

#[test]
fn test_gaps_codec_merge() -> Result<()> {
    check_merging_codec(&GapsCodec::<BE, _, _, GAMMA, DELTA, _, u32>::with_policy(
        FixedWidth::<u64>::new(),
        FixedWidth::<u64>::new(),
        WithMerge(CountRepeats),
    ))
}

/// The label rejected by [`Reject`].
const BAD: u32 = 13;

/// Serializes and deserializes `u32` labels using 32 bits, rejecting the
/// label [`BAD`] with an [`anyhow::Error`].
#[derive(Debug, Clone, Copy)]
struct Reject;

impl<E: Endianness, BW: BitWrite<E>> BitSerializer<E, BW> for Reject {
    type SerType = u32;
    type Error = anyhow::Error;

    fn serialize(&self, value: &u32, bitstream: &mut BW) -> Result<usize> {
        ensure!(*value != BAD, "Label {value} rejected");
        Ok(bitstream.write_bits(*value as u64, 32)?)
    }

    fn name(&self) -> String {
        "Reject".to_string()
    }
}

impl<E: Endianness, BR: BitRead<E>> BitDeserializer<E, BR> for Reject {
    type DeserType = u32;
    type Error = anyhow::Error;

    fn deserialize(&self, bitstream: &mut BR) -> Result<u32> {
        let value = bitstream.read_bits(32)? as u32;
        ensure!(value != BAD, "Label {value} rejected");
        Ok(value)
    }

    fn name(&self) -> String {
        "Reject".to_string()
    }
}

/// Returns a batch in which the arc (2, 3), followed by other arcs once
/// sorted, has label [`BAD`].
fn batch_with_bad_label() -> Vec<((usize, usize), u32)> {
    vec![((2, 3), BAD), ((0, 1), 0), ((2, 4), 0), ((1, 2), 0)]
}

/// Checks that encoding with `codec` a batch containing a label rejected by
/// the serializer fails, reporting the arc and the error of the serializer.
fn check_serialization_error(codec: &impl BatchCodec<Label = u32>) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("batch");
    let Err(err) = codec.encode_batch(&path, &mut batch_with_bad_label()) else {
        panic!("The rejected label has been serialized");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("arc (2, 3): Label 13 rejected"), "{msg}");
    Ok(())
}

#[test]
fn test_gaps_codec_serialization_error() -> Result<()> {
    check_serialization_error(&GapsCodec::<BE, _, _>::new(
        Reject,
        FixedWidth::<u32>::new(),
    ))
}

#[test]
fn test_grouped_gaps_codec_serialization_error() -> Result<()> {
    check_serialization_error(&GroupedGapsCodec::<BE, _>::new(BitSerDeser(
        Reject,
        FixedWidth::<u32>::new(),
    )))
}

/// Decodes with `codec` a batch containing a label rejected by the
/// deserializer.
fn decode_bad_label(codec: &impl BatchCodec<Label = u32>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("batch");
    codec
        .encode_batch(&path, &mut batch_with_bad_label())
        .unwrap();
    codec
        .decode_batch(&path)
        .unwrap()
        .into_iter()
        .for_each(drop);
}

#[test]
#[should_panic(expected = "arc (2, 3): Label 13 rejected")]
fn test_gaps_codec_deserialization_error() {
    decode_bad_label(&GapsCodec::<BE, _, _>::new(
        FixedWidth::<u32>::new(),
        Reject,
    ));
}

#[test]
#[should_panic(expected = "arc (2, 3): Label 13 rejected")]
fn test_grouped_gaps_codec_deserialization_error() {
    decode_bad_label(&GroupedGapsCodec::<BE, _>::new(BitSerDeser(
        FixedWidth::<u32>::new(),
        Reject,
    )));
}
