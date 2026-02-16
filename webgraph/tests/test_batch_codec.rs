/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_bitstream::prelude::*;

#[test]
fn test_gaps_stats_display() {
    let stats = webgraph::utils::gaps::GapsStats {
        total_triples: 100,
        src_bits: 400,
        dst_bits: 600,
        labels_bits: 200,
    };
    let s = format!("{}", stats);
    assert!(s.contains("src:"));
    assert!(s.contains("dst:"));
    assert!(s.contains("labels:"));
    assert!(s.contains("total:"));
    assert!(s.contains("bits / arc"));
}

#[test]
fn test_grouped_gaps_stats_display() {
    let stats = webgraph::utils::grouped_gaps::GroupedGapsStats {
        total_triples: 50,
        outdegree_bits: 100,
        src_bits: 200,
        dst_bits: 300,
        labels_bits: 50,
    };
    let s = format!("{}", stats);
    assert!(s.contains("outdegree:"));
    assert!(s.contains("src:"));
    assert!(s.contains("dst:"));
    assert!(s.contains("labels:"));
    assert!(s.contains("bits / arc"));
}

#[test]
fn test_gaps_codec_encode_batch() -> Result<()> {
    use webgraph::utils::BatchCodec;
    use webgraph::utils::gaps::GapsCodec;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("batch");

    let codec = GapsCodec::<BE, (), ()>::default();
    let mut batch: Vec<((usize, usize), ())> =
        vec![((2, 3), ()), ((0, 1), ()), ((1, 2), ()), ((0, 2), ())];
    let (bits, stats) = codec.encode_batch(&path, &mut batch)?;
    assert!(bits > 0);
    assert_eq!(stats.total_triples, 4);

    // Decode and verify
    let decoded = codec.decode_batch(&path)?;
    let items: Vec<_> = decoded.into_iter().collect();
    assert_eq!(items.len(), 4);
    // Should be sorted after encode_batch
    assert_eq!(items[0].0, (0, 1));
    assert_eq!(items[1].0, (0, 2));
    assert_eq!(items[2].0, (1, 2));
    assert_eq!(items[3].0, (2, 3));
    Ok(())
}

#[test]
fn test_gaps_iter_exact_size() -> Result<()> {
    use webgraph::utils::BatchCodec;
    use webgraph::utils::gaps::GapsCodec;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("batch_sz");

    let codec = GapsCodec::<BE, (), ()>::default();
    let batch: Vec<((usize, usize), ())> = vec![((0, 1), ()), ((1, 2), ()), ((2, 3), ())];
    codec.encode_sorted_batch(&path, &batch)?;

    let decoded = codec.decode_batch(&path)?;
    let iter = decoded.into_iter();
    assert_eq!(iter.len(), 3);
    let (lo, hi) = iter.size_hint();
    assert_eq!(lo, 3);
    assert_eq!(hi, Some(3));
    Ok(())
}

#[test]
fn test_grouped_gaps_codec_new_and_encode() -> Result<()> {
    use webgraph::utils::BatchCodec;
    use webgraph::utils::grouped_gaps::GroupedGapsCodec;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("grouped_batch");

    let codec = GroupedGapsCodec::<BE, (), ()>::new((), ());
    let mut batch: Vec<((usize, usize), ())> =
        vec![((2, 3), ()), ((0, 1), ()), ((0, 2), ()), ((1, 2), ())];
    let (bits, stats) = codec.encode_batch(&path, &mut batch)?;
    assert!(bits > 0);
    assert_eq!(stats.total_triples, 4);

    // Decode and verify
    let decoded = codec.decode_batch(&path)?;
    let iter = decoded.into_iter();
    // Check ExactSizeIterator
    assert_eq!(iter.len(), 4);
    let (lo, hi) = iter.size_hint();
    assert_eq!(lo, 4);
    assert_eq!(hi, Some(4));
    let items: Vec<_> = iter.collect();
    assert_eq!(items[0].0, (0, 1));
    assert_eq!(items[1].0, (0, 2));
    assert_eq!(items[2].0, (1, 2));
    assert_eq!(items[3].0, (2, 3));
    Ok(())
}
