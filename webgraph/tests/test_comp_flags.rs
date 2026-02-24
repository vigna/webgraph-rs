/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::collections::HashMap;
use webgraph::prelude::*;

#[test]
fn test_code_from_str_all_variants() {
    use dsi_bitstream::dispatch::Codes;
    assert_eq!(CompFlags::code_from_str("UNARY", 3), Some(Codes::Unary));
    assert_eq!(CompFlags::code_from_str("GAMMA", 3), Some(Codes::Gamma));
    assert_eq!(CompFlags::code_from_str("DELTA", 3), Some(Codes::Delta));
    assert_eq!(CompFlags::code_from_str("ZETA", 5), Some(Codes::Zeta(5)));
    assert_eq!(CompFlags::code_from_str("zeta", 2), Some(Codes::Zeta(2)));
    assert_eq!(CompFlags::code_from_str("PI1", 0), Some(Codes::Pi(1)));
    assert_eq!(CompFlags::code_from_str("PI2", 0), Some(Codes::Pi(2)));
    assert_eq!(CompFlags::code_from_str("PI3", 0), Some(Codes::Pi(3)));
    assert_eq!(CompFlags::code_from_str("PI4", 0), Some(Codes::Pi(4)));
    assert_eq!(CompFlags::code_from_str("ZETA1", 99), Some(Codes::Zeta(1)));
    assert_eq!(CompFlags::code_from_str("ZETA2", 99), Some(Codes::Zeta(2)));
    assert_eq!(CompFlags::code_from_str("ZETA3", 99), Some(Codes::Zeta(3)));
    assert_eq!(CompFlags::code_from_str("ZETA4", 99), Some(Codes::Zeta(4)));
    assert_eq!(CompFlags::code_from_str("ZETA5", 99), Some(Codes::Zeta(5)));
    assert_eq!(CompFlags::code_from_str("ZETA6", 99), Some(Codes::Zeta(6)));
    assert_eq!(CompFlags::code_from_str("ZETA7", 99), Some(Codes::Zeta(7)));
    assert_eq!(CompFlags::code_from_str("BOGUS", 3), None);
}

#[test]
fn test_code_to_str_version0() {
    use dsi_bitstream::dispatch::Codes;
    assert_eq!(CompFlags::code_to_str(Codes::Unary, 0), Some("UNARY"));
    assert_eq!(CompFlags::code_to_str(Codes::Gamma, 0), Some("GAMMA"));
    assert_eq!(CompFlags::code_to_str(Codes::Delta, 0), Some("DELTA"));
    // version 0: all zeta variants map to "ZETA"
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(3), 0), Some("ZETA"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(7), 0), Some("ZETA"));
    // version 0: unsupported codes return None
    assert_eq!(CompFlags::code_to_str(Codes::Pi(1), 0), None);
}

#[test]
fn test_code_to_str_version1() {
    use dsi_bitstream::dispatch::Codes;
    assert_eq!(CompFlags::code_to_str(Codes::Unary, 1), Some("UNARY"));
    assert_eq!(CompFlags::code_to_str(Codes::Gamma, 1), Some("GAMMA"));
    assert_eq!(CompFlags::code_to_str(Codes::Delta, 1), Some("DELTA"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(1), 1), Some("ZETA1"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(2), 1), Some("ZETA2"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(3), 1), Some("ZETA3"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(4), 1), Some("ZETA4"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(5), 1), Some("ZETA5"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(6), 1), Some("ZETA6"));
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(7), 1), Some("ZETA7"));
    assert_eq!(CompFlags::code_to_str(Codes::Pi(1), 1), Some("PI1"));
    assert_eq!(CompFlags::code_to_str(Codes::Pi(2), 1), Some("PI2"));
    assert_eq!(CompFlags::code_to_str(Codes::Pi(3), 1), Some("PI3"));
    assert_eq!(CompFlags::code_to_str(Codes::Pi(4), 1), Some("PI4"));
    // version 1: unsupported codes return None
    assert_eq!(CompFlags::code_to_str(Codes::Zeta(8), 1), None);
    assert_eq!(CompFlags::code_to_str(Codes::Pi(5), 1), None);
}

#[test]
fn test_to_properties_be_default() -> Result<()> {
    let cf = CompFlags::default();
    let props_str = cf.to_properties::<BE>(100, 500, 10000)?;
    assert!(props_str.contains("nodes=100"));
    assert!(props_str.contains("arcs=500"));
    assert!(props_str.contains("version=0"));
    assert!(props_str.contains("endianness=big"));
    assert!(props_str.contains("windowsize=7"));
    assert!(props_str.contains("minintervallength=4"));
    assert!(props_str.contains("maxrefcount=3"));
    assert!(props_str.contains("zetak=3"));
    Ok(())
}

#[test]
fn test_to_properties_le() -> Result<()> {
    let cf = CompFlags::default();
    let props_str = cf.to_properties::<LE>(50, 200, 5000)?;
    assert!(props_str.contains("version=1"));
    assert!(props_str.contains("endianness=little"));
    Ok(())
}

#[test]
fn test_to_properties_custom_codes() -> Result<()> {
    use dsi_bitstream::dispatch::Codes;
    // All zeta codes must use the same k
    let cf = CompFlags {
        outdegrees: Codes::Delta,
        references: Codes::Gamma,
        blocks: Codes::Delta,
        intervals: Codes::Zeta(5),
        residuals: Codes::Zeta(5),
        min_interval_length: 4,
        compression_window: 7,
        max_ref_count: 3,
    };
    let props = cf.to_properties::<BE>(10, 20, 1000)?;
    assert!(props.contains("OUTDEGREES_DELTA"));
    assert!(props.contains("REFERENCES_GAMMA"));
    assert!(props.contains("BLOCKS_DELTA"));
    assert!(props.contains("INTERVALS_ZETA"));
    assert!(props.contains("RESIDUALS_ZETA"));
    Ok(())
}

#[test]
fn test_from_properties_default_be() -> Result<()> {
    let cf = CompFlags::default();
    let props_str = cf.to_properties::<BE>(100, 500, 10000)?;
    let f = std::io::BufReader::new(props_str.as_bytes());
    let map: HashMap<String, String> = java_properties::read(f)?;
    let cf2 = CompFlags::from_properties::<BE>(&map)?;
    assert_eq!(cf.outdegrees, cf2.outdegrees);
    assert_eq!(cf.references, cf2.references);
    assert_eq!(cf.blocks, cf2.blocks);
    assert_eq!(cf.intervals, cf2.intervals);
    assert_eq!(cf.residuals, cf2.residuals);
    assert_eq!(cf.compression_window, cf2.compression_window);
    assert_eq!(cf.min_interval_length, cf2.min_interval_length);
    assert_eq!(cf.max_ref_count, cf2.max_ref_count);
    Ok(())
}

#[test]
fn test_from_properties_le() -> Result<()> {
    let cf = CompFlags::default();
    let props_str = cf.to_properties::<LE>(100, 500, 10000)?;
    let f = std::io::BufReader::new(props_str.as_bytes());
    let map: HashMap<String, String> = java_properties::read(f)?;
    let cf2 = CompFlags::from_properties::<LE>(&map)?;
    assert_eq!(cf.outdegrees, cf2.outdegrees);
    Ok(())
}

#[test]
fn test_from_properties_custom_flags() -> Result<()> {
    let mut map = HashMap::new();
    map.insert("version".to_string(), "0".to_string());
    map.insert("endianness".to_string(), "big".to_string());
    map.insert(
        "compressionflags".to_string(),
        "OUTDEGREES_DELTA|REFERENCES_GAMMA|BLOCKS_DELTA|INTERVALS_DELTA|RESIDUALS_ZETA".to_string(),
    );
    map.insert("zetak".to_string(), "5".to_string());
    map.insert("windowsize".to_string(), "10".to_string());
    map.insert("minintervallength".to_string(), "2".to_string());
    map.insert("maxrefcount".to_string(), "5".to_string());

    let cf = CompFlags::from_properties::<BE>(&map)?;
    use dsi_bitstream::dispatch::Codes;
    assert_eq!(cf.outdegrees, Codes::Delta);
    assert_eq!(cf.references, Codes::Gamma);
    assert_eq!(cf.blocks, Codes::Delta);
    assert_eq!(cf.intervals, Codes::Delta);
    assert_eq!(cf.residuals, Codes::Zeta(5));
    assert_eq!(cf.compression_window, 10);
    assert_eq!(cf.min_interval_length, 2);
    assert_eq!(cf.max_ref_count, 5);
    Ok(())
}

#[test]
fn test_from_properties_wrong_endianness() {
    let mut map = HashMap::new();
    map.insert("endianness".to_string(), "big".to_string());
    assert!(CompFlags::from_properties::<LE>(&map).is_err());
}

#[test]
fn test_from_properties_empty_compression_flags() -> Result<()> {
    let mut map = HashMap::new();
    map.insert("endianness".to_string(), "big".to_string());
    map.insert("compressionflags".to_string(), "".to_string());
    let cf = CompFlags::from_properties::<BE>(&map)?;
    // Should use defaults
    use dsi_bitstream::dispatch::Codes;
    assert_eq!(cf.outdegrees, Codes::Gamma);
    Ok(())
}
