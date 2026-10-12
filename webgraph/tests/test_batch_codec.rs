/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

use anyhow::Result;
use dsi_bitstream::prelude::*;
use webgraph::utils::gaps::GapsCodec;
use webgraph::utils::grouped_gaps::GroupedGapsCodec;
use webgraph::utils::{BatchCodec, BatchStats};

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
