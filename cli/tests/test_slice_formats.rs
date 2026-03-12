use value_traits::slices::SliceByValue;
use webgraph_cli::{FloatSliceFormat, IntSlice, IntSliceFormat};

/// Checks that an [`IntSlice`] matches the expected slice via [`SliceByValue`].
fn assert_int_slice_eq(loaded: &IntSlice, expected: &[usize]) {
    assert_eq!(loaded.len(), expected.len());
    for (i, &e) in expected.iter().enumerate() {
        assert_eq!(loaded.index_value(i), e, "mismatch at index {i}");
    }
}

#[test]
fn test_int_roundtrip_all_formats() {
    let dir = tempfile::tempdir().unwrap();
    let data: Vec<usize> = vec![10, 20, 30, 0, 42];
    let mut formats: Vec<(IntSliceFormat, &str)> = vec![
        (IntSliceFormat::Ascii, "txt"),
        (IntSliceFormat::Json, "json"),
        (IntSliceFormat::Epserde, "eps"),
    ];
    #[cfg(target_pointer_width = "64")]
    formats.push((IntSliceFormat::Java, "bin"));
    for (fmt, ext) in formats {
        let path = dir.path().join(format!("test.{ext}"));
        fmt.store(&path, &data, None).unwrap();
        assert_int_slice_eq(&fmt.load(&path).unwrap(), &data);
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
fn test_int_roundtrip_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut formats: Vec<(IntSliceFormat, &str)> = vec![
        (IntSliceFormat::Ascii, "txt"),
        (IntSliceFormat::Json, "json"),
        (IntSliceFormat::Epserde, "eps"),
    ];
    #[cfg(target_pointer_width = "64")]
    formats.push((IntSliceFormat::Java, "bin"));
    for (fmt, ext) in formats {
        let path = dir.path().join(format!("empty.{ext}"));
        fmt.store(&path, &[], None).unwrap();
        assert_eq!(
            fmt.load(&path).unwrap().len(),
            0,
            "empty roundtrip failed for {ext}"
        );
    }
}

#[test]
fn test_int_creates_parent_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a").join("b").join("test.txt");
    IntSliceFormat::Ascii.store(&path, &[1], None).unwrap();
    assert!(path.exists());
}

#[test]
fn test_float_roundtrip_all_formats() {
    let dir = tempfile::tempdir().unwrap();
    let values: Vec<f64> = vec![1.5, 2.75, 3.0, 0.0, -1.25];
    for (fmt, ext) in [
        (FloatSliceFormat::Ascii, "txt"),
        (FloatSliceFormat::Json, "json"),
        (FloatSliceFormat::Java, "bin"),
        (FloatSliceFormat::Epserde, "eps"),
    ] {
        let path = dir.path().join(format!("test.{ext}"));
        fmt.store(&path, &values, None).unwrap();
        let loaded: Vec<f64> = fmt.load(&path).unwrap();
        assert_eq!(loaded, values, "roundtrip failed for {ext}");
    }
}

#[test]
fn test_float_roundtrip_f32() {
    let dir = tempfile::tempdir().unwrap();
    let values: Vec<f32> = vec![1.5, 2.75, 3.0, 0.0, -1.25];
    for (fmt, ext) in [
        (FloatSliceFormat::Ascii, "txt"),
        (FloatSliceFormat::Json, "json"),
        (FloatSliceFormat::Java, "bin"),
        (FloatSliceFormat::Epserde, "eps"),
    ] {
        let path = dir.path().join(format!("f32.{ext}"));
        fmt.store(&path, &values, None).unwrap();
        let loaded: Vec<f32> = fmt.load(&path).unwrap();
        assert_eq!(loaded, values, "f32 roundtrip failed for {ext}");
    }
}

#[test]
fn test_float_roundtrip_empty() {
    let dir = tempfile::tempdir().unwrap();
    for (fmt, ext) in [
        (FloatSliceFormat::Ascii, "txt"),
        (FloatSliceFormat::Json, "json"),
        (FloatSliceFormat::Java, "bin"),
        (FloatSliceFormat::Epserde, "eps"),
    ] {
        let path = dir.path().join(format!("empty.{ext}"));
        fmt.store::<f64>(&path, &[], None).unwrap();
        let loaded: Vec<f64> = fmt.load(&path).unwrap();
        assert!(loaded.is_empty(), "empty roundtrip failed for {ext}");
    }
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
