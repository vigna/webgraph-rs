/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{bail, ensure, Result};
use dsi_bitstream::traits::{Endianness, BigEndian, LittleEndian};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    Unary,
    Gamma,
    Delta,
    Zeta { k: usize },
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "fuzz", derive(arbitrary::Arbitrary))]
/// The compression flags for reading or compressing a graph.
///
/// As documented, one code might sets multiple values. This is done for
/// compatibility with the previous Java version of the library.
/// But the codes optimizers will return the optimal codes for each of them,
/// so if it identify some big save from using different codes, we can consider
/// splitting them.
pub struct CompFlags {
    /// The instantaneous code to use to encode the `outdegrees`
    pub outdegrees: Code,
    /// The instantaneous code to use to encode the `reference_offset`
    pub references: Code,
    /// The instantaneous code to use to encode the `block_count` and `blocks`
    pub blocks: Code,
    /// The instantaneous code to use to encode the `interval_count`, `interval_start`, and `interval_len`.
    pub intervals: Code,
    /// The instantaneous code to use to encode the `first_residual` and `residual`
    pub residuals: Code,
    /// The minimum length of an interval to be compressed as (start, len)
    pub min_interval_length: usize,
    /// The number of previous nodes to use for reference compression
    pub compression_window: usize,
    /// The maximum recursion depth during decoding, this modulates the tradeoff
    /// between compression ratio and decoding speed
    pub max_ref_count: usize,
}

impl core::default::Default for CompFlags {
    fn default() -> Self {
        CompFlags {
            outdegrees: Code::Gamma,
            references: Code::Unary,
            blocks: Code::Gamma,
            intervals: Code::Gamma,
            residuals: Code::Zeta { k: 3 },
            min_interval_length: 4,
            compression_window: 7,
            max_ref_count: 3,
        }
    }
}

impl CompFlags {
    /// Convert a string from the `compflags` field from the `.properties` file
    /// into which code to use.
    ///
    /// Returns `None` if the string is not recognized.
    pub fn code_from_str(s: &str, k: usize) -> Option<Code> {
        match s.to_uppercase().as_str() {
            "UNARY" => Some(Code::Unary),
            "GAMMA" => Some(Code::Gamma),
            "DELTA" => Some(Code::Delta),
            "ZETA" => Some(Code::Zeta { k }),
            _ => None,
        }
    }

    pub fn code_to_str(c: Code) -> Option<&'static str> {
        match c {
            Code::Unary => Some("UNARY"),
            Code::Gamma => Some("GAMMA"),
            Code::Delta => Some("DELTA"),
            Code::Zeta { k: _ } => Some("ZETA"),
        }
    }

    pub fn to_properties<E: Endianness>(&self, num_nodes: usize, num_arcs: u64) -> Result<String> {
        let mut s = String::new();
        s.push_str("#BVGraph properties\n");
        s.push_str("graphclass=it.unimi.dsi.webgraph.BVGraph\n");

        if core::any::TypeId::of::<E>() == core::any::TypeId::of::<BigEndian>() {
            s.push_str("version=0\n");
        } else {
            s.push_str("version=1\n");
        }
        s.push_str(&format!("endianness={}\n", E::NAME));

        s.push_str(&format!("nodes={}\n", num_nodes));
        s.push_str(&format!("arcs={}\n", num_arcs));
        s.push_str(&format!("minintervallength={}\n", self.min_interval_length));
        s.push_str(&format!("maxrefcount={}\n", self.max_ref_count));
        s.push_str(&format!("windowsize={}\n", self.compression_window));
        s.push_str("compressionflags=");
        let mut cflags = false;
        if self.outdegrees != Code::Gamma {
            s.push_str(&format!(
                "OUTDEGREES_{}|",
                Self::code_to_str(self.outdegrees).unwrap()
            ));
            cflags = true;
        }
        if self.references != Code::Unary {
            s.push_str(&format!(
                "REFERENCES_{}|",
                Self::code_to_str(self.references).unwrap()
            ));
            cflags = true;
        }
        if self.blocks != Code::Gamma {
            s.push_str(&format!(
                "BLOCKS_{}|",
                Self::code_to_str(self.blocks).unwrap()
            ));
            cflags = true;
        }
        if self.intervals != Code::Gamma {
            s.push_str(&format!(
                "INTERVALS_{}|",
                Self::code_to_str(self.intervals).unwrap()
            ));
            cflags = true;
        }
        if matches!(self.residuals, Code::Zeta{k:_}) {
            s.push_str(&format!(
                "RESIDUALS_{}|",
                Self::code_to_str(self.residuals).unwrap()
            ));
            cflags = true;
        }
        if cflags {
            s.pop();
        }
        s.push('\n');
        // check that if a k is specified, it is the same for all codes
        let mut k = None;
        macro_rules! check_and_set_k {
            ($code:expr) => {
                match $code {
                    Code::Zeta{k: new_k} => { 
                        if let Some(old_k) = k {
                            ensure!(old_k == new_k, "Only one value of k is supported")
                        }
                        k = Some(new_k) 
                    }
                    _ => {}
                } 
            };
        }
        check_and_set_k!(self.outdegrees);
        check_and_set_k!(self.references);
        check_and_set_k!(self.blocks);
        check_and_set_k!(self.intervals);
        check_and_set_k!(self.residuals);
        // if no k was specified, use the default one (3)
        s.push_str(&format!("zetak={}\n", k.unwrap_or(3)));
        Ok(s)
    }

    /// Convert the decoded `.properties` file into a `CompFlags` struct.
    /// Also check that the endiannes is correct.
    pub fn from_properties<E: Endianness>(map: &HashMap<String, String>) -> Result<Self> {
        // Default values, same as the Java class
        let endianness = map
            .get("endianness")
            .map(|x| x.to_string())
            .unwrap_or_else(|| BigEndian::NAME.to_string());

        anyhow::ensure!(
            endianness == E::NAME,
            "Wrong endianness, got {} while expected {}",
            endianness,
            E::NAME
        );
        // check that the version was properly set for LE
        if core::any::TypeId::of::<E>() == core::any::TypeId::of::<LittleEndian>() {
            anyhow::ensure!(
                map.get("version").map(|x| x.parse::<u32>().unwrap()) == Some(1),
                "Wrong version, got {} while expected 1",
                map.get("version").unwrap_or(&"None".to_string())
            );
        }

        let mut cf = CompFlags::default();
        let mut k = 3;
        if let Some(spec_k) = map.get("zeta_k") {
            let spec_k = spec_k.parse::<usize>()?;
            if !(1..=7).contains(&spec_k) {
                bail!("Only ζ₁-ζ₇ are supported");
            }
            k = spec_k;
        }
        if let Some(comp_flags) = map.get("compressionflags") {
            if !comp_flags.is_empty() {
                for flag in comp_flags.split('|') {
                    let s: Vec<_> = flag.split('_').collect();
                    // FIXME: this is a hack to avoid having to implement
                    // FromStr for Code
                    let code = CompFlags::code_from_str(s[1], k).unwrap();
                    match s[0] {
                        "OUTDEGREES" => cf.outdegrees = code,
                        "REFERENCES" => cf.references = code,
                        "BLOCKS" => cf.blocks = code,
                        "INTERVALS" => cf.intervals = code,
                        "RESIDUALS" => cf.residuals = code,
                        "OFFSETS" => {
                            ensure!(code == Code::Gamma, "Only γ code is supported for offsets")
                        }
                        _ => bail!("Unknown compression flag {}", flag),
                    }
                }
            }
        }
        if let Some(compression_window) = map.get("compressionwindow") {
            cf.compression_window = compression_window.parse()?;
        }
        if let Some(min_interval_length) = map.get("minintervallength") {
            cf.min_interval_length = min_interval_length.parse()?;
        }
        Ok(cf)
    }
}
