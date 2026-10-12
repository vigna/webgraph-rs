/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use dsi_bitstream::prelude::*;
use webgraph::traits::{BitDeserializer, BitSerializer, FixedWidth};

/// Serializes `$values` with `$sd` and checks that they deserialize to the
/// same values.
macro_rules! check_roundtrip {
    ($sd:expr, $values:expr) => {{
        let sd = $sd;
        let values = $values;
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
    }};
}

#[test]
fn test_fixed_size() {
    check_roundtrip!(
        FixedWidth::<u32>::new(),
        [0u32, 1, 42, u32::MAX, 0x_DEAD_BEEF]
    );
    check_roundtrip!(FixedWidth::<i16>::new(), [0i16, 1, -1, i16::MIN, i16::MAX]);
}

#[test]
fn test_fixed_width() {
    check_roundtrip!(FixedWidth::<u32>::with_bits(10), [0u32, 1, 42, 1023]);
    // 5 bits signed: range [−16 . . 16)
    check_roundtrip!(FixedWidth::<i16>::with_bits(5), [0i16, 1, -1, -16, 15]);
}

#[test]
#[should_panic(expected = "at most 64 bits")]
fn test_fixed_width_rejects_wide_types() {
    // Regression: FixedWidth::<i128>::with_bits(5) was accepted but
    // round-tripped negative values through a 64-bit sign extension,
    // deserializing -1 as 2^64 - 1.
    let _ = FixedWidth::<i128>::with_bits(5);
}
