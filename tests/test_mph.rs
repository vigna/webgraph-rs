use anyhow::Result;
use std::collections::HashSet;
use std::io::BufRead;
use std::io::BufReader;
use std::fs::File;
use webgraph::utils::mph;

#[test]
fn test_mph() -> Result<()> {
    // Read the offsets
    let m = mph::mph::load("tests/data/test.cmph")?;
    let m2 = webgraph::mph::GOVMPH::load(File::open("tests/data/test.cmph")?)?;
    let reader = BufReader::new(File::open("tests/data/mph.txt")?);
    let mut s = HashSet::new();
    for line in reader.lines() {
        let line = line?;
        let p = m.get(&line);
        let p2 = m2.get_byte_array(line.as_bytes());
        assert!(p < m.size());
        assert!(s.insert(p));
        assert_eq!(p, p2 as usize);
    }
    assert_eq!(s.len(), m.size());
    Ok(())
}

#[test]
fn test_mph_file_not_file() {
    assert!(mph::mph::load("tests/data/nofile").is_err());
}

#[test]
fn test_govmph_reg() -> Result<()> {
    let m = mph::mph::load("tests/data/test.cmph")?;
    let m2 = webgraph::mph::GOVMPH::load(File::open("tests/data/test.cmph")?)?;

    assert_eq!(m.size, m2.size);
    assert_eq!(m.multiplier, m2.multiplier);
    assert_eq!(m.global_seed, m2.global_seed);
    assert_eq!(m.edge_offset_and_seed_length, m2.edge_offset_and_seed.len() as u64);
    assert_eq!(m.array_length, m2.array.len() as u64);

    for i in 0..m.edge_offset_and_seed_length as usize {
        assert_eq!(
            m2.edge_offset_and_seed[i],
            unsafe{*m.edge_offset_and_seed.add(i)},
        );
    }

    for i in 0..m.array_length as usize {
        assert_eq!(
            m2.array[i],
            unsafe{*m.array.add(i)},
        );
    }

    Ok(())
}

#[test]
fn test_spooky() {
    let seed = 0x10f5a0cd248a6c9f;

    let message = [
        0x47, 0x8e, 0x46, 0x26, 0xa1, 0xb3, 0x08, 0x93, 
        0x56, 0xd3, 0x3e, 0x01, 0x9a, 0xda, 0xc6, 0x9f,
        0xc9, 0xcd, 0xf1, 0x25, 0x44, 0xbb, 0xa3, 0x48, 
        0xca, 0x2a, 0xdb, 0x32, 0x48, 0x4d, 0xae, 0x88,
    ];

    for i in 0..message.len() {
        let res = webgraph::spooky::spooky_short(&message[..i], seed);

        let mut data: [u64; 4] = [0; 4];
    
        unsafe{
            webgraph::utils::mph::spooky_short(
                &message as *const u8 as *const _, 
                i, 
                seed,
                &mut data as *mut u64,
            );
        }
    
        assert_eq!(res, data);
    }

    let signature = [
        0xb20617c02c19458d, 0x71143ce6974a84e1,
        0x1c3adc586b5dbda3, 0x3665513702ac5d6b, 
    ];

    let res = webgraph::spooky::spooky_short_rehash(&signature, seed);
    
    let mut data: [u64; 4] = [0; 4];
    unsafe{
        webgraph::utils::mph::spooky_short_rehash(
            &signature as *const u64,
            seed,
            &mut data as *mut u64,
        )
    }

    assert_eq!(res, data);
}
