use anyhow::Result;
use std::collections::HashSet;
use std::io::BufRead;
use std::io::BufReader;
use webgraph::utils::mph::mph;

#[test]
fn test_mph() -> Result<()> {
    // Read the offsets
    let m = mph::load("tests/data/test.cmph")?;
    let reader = BufReader::new(std::fs::File::open("tests/data/mph.txt")?);
    let mut s = HashSet::new();
    for line in reader.lines() {
        let line = line?;
        let p = m.get(&line);
        assert!(p < m.size());
        assert!(s.insert(p));
    }
    assert_eq!(s.len(), m.size());
    Ok(())
}

#[test]
fn test_mph_file_not_file() {
    assert!(mph::load("tests/data/nofile").is_err());
}
