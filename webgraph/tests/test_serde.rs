/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
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
