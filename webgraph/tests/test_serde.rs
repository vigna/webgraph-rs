/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use dsi_bitstream::prelude::*;
use webgraph::traits::{BitDeserializer, BitSerializer, FixedWidth};

#[test]
fn test_fixed_size_unsigned() {
    let sd = FixedWidth::<u32>::new();
    let values: Vec<u32> = vec![0, 1, 42, u32::MAX, 0x_DEAD_BEEF];

    let mut buf: Vec<u64> = vec![];
    let mut writer = BufBitWriter::<NE, _>::new(MemWordWriterVec::new(&mut buf));
    for v in &values {
        sd.serialize(v, &mut writer).unwrap();
    }
    drop(writer);

    let mut reader = BufBitReader::<NE, _>::new(MemWordReader::new(&buf));
    for v in &values {
        assert_eq!(sd.deserialize(&mut reader).unwrap(), *v);
    }
}

#[test]
fn test_fixed_size_signed() {
    let sd = FixedWidth::<i16>::new();
    let values: Vec<i16> = vec![0, 1, -1, i16::MIN, i16::MAX];

    let mut buf: Vec<u64> = vec![];
    let mut writer = BufBitWriter::<NE, _>::new(MemWordWriterVec::new(&mut buf));
    for v in &values {
        sd.serialize(v, &mut writer).unwrap();
    }
    drop(writer);

    let mut reader = BufBitReader::<NE, _>::new(MemWordReader::new(&buf));
    for v in &values {
        assert_eq!(sd.deserialize(&mut reader).unwrap(), *v);
    }
}

#[test]
fn test_fixed_width_unsigned() {
    let sd = FixedWidth::<u32>::with_bits(10);
    let values: Vec<u32> = vec![0, 1, 42, 1023];

    let mut buf: Vec<u64> = vec![];
    let mut writer = BufBitWriter::<NE, _>::new(MemWordWriterVec::new(&mut buf));
    for v in &values {
        sd.serialize(v, &mut writer).unwrap();
    }
    drop(writer);

    let mut reader = BufBitReader::<NE, _>::new(MemWordReader::new(&buf));
    for v in &values {
        assert_eq!(sd.deserialize(&mut reader).unwrap(), *v);
    }
}

#[test]
fn test_fixed_width_signed() {
    let sd = FixedWidth::<i16>::with_bits(5);
    // 5 bits signed: range [−16 . . 16)
    let values: Vec<i16> = vec![0, 1, -1, -16, 15];

    let mut buf: Vec<u64> = vec![];
    let mut writer = BufBitWriter::<NE, _>::new(MemWordWriterVec::new(&mut buf));
    for v in &values {
        sd.serialize(v, &mut writer).unwrap();
    }
    drop(writer);

    let mut reader = BufBitReader::<NE, _>::new(MemWordReader::new(&buf));
    for v in &values {
        assert_eq!(sd.deserialize(&mut reader).unwrap(), *v);
    }
}

#[test]
#[should_panic(expected = "at most 64 bits")]
fn test_fixed_width_rejects_wide_types() {
    // Regression: FixedWidth::<i128>::with_bits(5) was accepted but
    // round-tripped negative values through a 64-bit sign extension,
    // deserializing -1 as 2^64 - 1.
    let _ = FixedWidth::<i128>::with_bits(5);
}
