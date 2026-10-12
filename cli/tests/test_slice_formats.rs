/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

use value_traits::slices::SliceByValue;
use webgraph_cli::{FloatSliceFormat, IntSlice, IntSliceFormat};

/// Checks that an [`IntSlice`] matches the expected slice via [`SliceByValue`].
fn assert_int_slice_eq(loaded: &IntSlice, expected: &[usize]) {
    assert_eq!(loaded.len(), expected.len());
    for (i, &e) in expected.iter().enumerate() {
        assert_eq!(loaded.index_value(i), e, "mismatch at index {i}");
    }
}

/// The integer formats that do not need a bit width, with a file extension.
#[allow(unused_mut)]
fn int_formats() -> Vec<(IntSliceFormat, &'static str)> {
    let mut formats = vec![
        (IntSliceFormat::Ascii, "txt"),
        (IntSliceFormat::Json, "json"),
        (IntSliceFormat::Epserde, "eps"),
    ];
    #[cfg(target_pointer_width = "64")]
    formats.push((IntSliceFormat::Java, "bin"));
    formats
}

/// The floating-point formats, with a file extension.
const FLOAT_FORMATS: [(FloatSliceFormat, &str); 4] = [
    (FloatSliceFormat::Ascii, "txt"),
    (FloatSliceFormat::Json, "json"),
    (FloatSliceFormat::Java, "bin"),
    (FloatSliceFormat::Epserde, "eps"),
];

#[test]
fn test_int_roundtrip_all_formats() {
    let dir = tempfile::tempdir().unwrap();
    for data in [vec![10, 20, 30, 0, 42], vec![]] {
        for (fmt, ext) in int_formats() {
            let path = dir.path().join(format!("test.{ext}"));
            fmt.store(&path, &data, None).unwrap();
            assert_int_slice_eq(&fmt.load(&path).unwrap(), &data);
        }
    }
}

#[test]
fn test_int_roundtrip_bitfieldvec() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.bfv");
    let data: Vec<usize> = vec![1, 3, 7, 15];
    IntSliceFormat::BitFieldVec
        .store(&path, &data, Some(15))
        .unwrap();
    assert_int_slice_eq(&IntSliceFormat::BitFieldVec.load(&path).unwrap(), &data);
}

#[test]
fn test_int_creates_parent_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a").join("b").join("test.txt");
    IntSliceFormat::Ascii.store(&path, &[1], None).unwrap();
    assert!(path.exists());
}

/// Stores `values` in all floating-point formats and checks that loading
/// returns the same values.
macro_rules! check_float_roundtrip {
    ($values:expr, $T:ty) => {{
        let dir = tempfile::tempdir().unwrap();
        let values: Vec<$T> = $values;
        for (fmt, ext) in FLOAT_FORMATS {
            let path = dir.path().join(format!("test.{ext}"));
            fmt.store(&path, &values, None).unwrap();
            let loaded: Vec<$T> = fmt.load(&path).unwrap();
            assert_eq!(loaded, values, "roundtrip failed for {ext}");
        }
    }};
}

#[test]
fn test_float_roundtrip_all_formats() {
    check_float_roundtrip!(vec![1.5, 2.75, 3.0, 0.0, -1.25], f64);
    check_float_roundtrip!(vec![1.5, 2.75, 3.0, 0.0, -1.25], f32);
    check_float_roundtrip!(vec![], f64);
}

#[test]
fn test_float_json_precision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.json");
    let values: Vec<f64> = vec![1.123456789, 2.987654321];
    FloatSliceFormat::Json
        .store(&path, &values, Some(2))
        .unwrap();
    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "[1.12, 2.99]");
}
