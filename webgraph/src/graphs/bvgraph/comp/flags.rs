/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Context, Result, bail, ensure};
use dsi_bitstream::dispatch::Codes;
use dsi_bitstream::traits::{BigEndian, Endianness, LittleEndian};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "fuzz", derive(arbitrary::Arbitrary))]
/// The compression flags for reading or compressing a graph.
///
/// As documented, one code might set multiple values. This is done for
/// compatibility with the previous Java version of the library.
/// But the code optimizer will return the optimal codes for each of them,
/// so if it identifies some big saving from using different codes, we can consider
/// splitting them.
pub struct CompFlags {
    /// The instantaneous code to use to encode the `outdegrees`
    pub outdegrees: Codes,
    /// The instantaneous code to use to encode the `reference_offset`
    pub references: Codes,
    /// The instantaneous code to use to encode the `block_count` and `blocks`
    pub blocks: Codes,
    /// The instantaneous code to use to encode the `interval_count`, `interval_start`, and `interval_len`.
    pub intervals: Codes,
    /// The instantaneous code to use to encode the `first_residual` and `residual`
    pub residuals: Codes,
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
            outdegrees: Codes::Gamma,
            references: Codes::Unary,
            blocks: Codes::Gamma,
            intervals: Codes::Gamma,
            residuals: Codes::Zeta(3),
            min_interval_length: 4,
            compression_window: 7,
            max_ref_count: 3,
        }
    }
}

const OLD_CODES: [Codes; 10] = [
    Codes::Unary,
    Codes::Gamma,
    Codes::Delta,
    Codes::Zeta(1),
    Codes::Zeta(2),
    Codes::Zeta(3),
    Codes::Zeta(4),
    Codes::Zeta(5),
    Codes::Zeta(6),
    Codes::Zeta(7),
];

impl CompFlags {
    /// Convert a string from the `compflags` field from the `.properties` file
    /// into which code to use.
    ///
    /// Note that ζ codes are supported both with parameterless names (e.g.,
    /// "ZETA") and with the parameter in the name (e.g., "ZETA3"), for
    /// compatibility with previous versions of the library.
    ///
    /// For CLI ergonomics and compatibility, this codes must be the same as
    /// those appearing in the `PrivCode` enum of the `webgraph-cli` crate.
    ///
    /// Returns `None` if the string is not recognized.
    pub fn code_from_str(s: &str, k: usize) -> Option<Codes> {
        match s.to_uppercase().as_str() {
            "UNARY" => Some(Codes::Unary),
            "GAMMA" => Some(Codes::Gamma),
            "DELTA" => Some(Codes::Delta),
            "ZETA" => Some(Codes::Zeta(k)),
            "PI1" => Some(Codes::Pi(1)),
            "PI2" => Some(Codes::Pi(2)),
            "PI3" => Some(Codes::Pi(3)),
            "PI4" => Some(Codes::Pi(4)),
            "ZETA1" => Some(Codes::Zeta(1)),
            "ZETA2" => Some(Codes::Zeta(2)),
            "ZETA3" => Some(Codes::Zeta(3)),
            "ZETA4" => Some(Codes::Zeta(4)),
            "ZETA5" => Some(Codes::Zeta(5)),
            "ZETA6" => Some(Codes::Zeta(6)),
            "ZETA7" => Some(Codes::Zeta(7)),
            _ => None,
        }
    }

    pub fn code_to_str(c: Codes, version: usize) -> Option<&'static str> {
        if version == 0 {
            match c {
                Codes::Unary => Some("UNARY"),
                Codes::Gamma => Some("GAMMA"),
                Codes::Delta => Some("DELTA"),
                Codes::Zeta(_) => Some("ZETA"),
                _ => None,
            }
        } else {
            match c {
                Codes::Unary => Some("UNARY"),
                Codes::Gamma => Some("GAMMA"),
                Codes::Delta => Some("DELTA"),
                Codes::Zeta(1) => Some("ZETA1"),
                Codes::Zeta(2) => Some("ZETA2"),
                Codes::Zeta(3) => Some("ZETA3"),
                Codes::Zeta(4) => Some("ZETA4"),
                Codes::Zeta(5) => Some("ZETA5"),
                Codes::Zeta(6) => Some("ZETA6"),
                Codes::Zeta(7) => Some("ZETA7"),
                Codes::Pi(1) => Some("PI1"),
                Codes::Pi(2) => Some("PI2"),
                Codes::Pi(3) => Some("PI3"),
                Codes::Pi(4) => Some("PI4"),
                _ => None,
            }
        }
    }

    fn contains_new_codes(&self) -> bool {
        !OLD_CODES.contains(&self.outdegrees)
            || !OLD_CODES.contains(&self.references)
            || !OLD_CODES.contains(&self.blocks)
            || !OLD_CODES.contains(&self.intervals)
            || !OLD_CODES.contains(&self.residuals)
    }

    pub fn to_properties<E: Endianness>(
        &self,
        num_nodes: usize,
        num_arcs: u64,
        bitstream_len: u64,
    ) -> Result<String> {
        let mut s = String::new();
        s.push_str("#BVGraph properties\n");
        s.push_str("graphclass=it.unimi.dsi.webgraph.BVGraph\n");

        // Version 1 if we have big-endian or new codes
        let version = (core::any::TypeId::of::<E>() != core::any::TypeId::of::<BigEndian>()
            || self.contains_new_codes()) as usize;

        s.push_str(&format!("version={version}\n"));
        s.push_str(&format!("endianness={}\n", E::NAME));

        s.push_str(&format!("nodes={num_nodes}\n"));
        s.push_str(&format!("arcs={num_arcs}\n"));
        s.push_str(&format!("minintervallength={}\n", self.min_interval_length));
        s.push_str(&format!("maxrefcount={}\n", self.max_ref_count));
        s.push_str(&format!("windowsize={}\n", self.compression_window));
        s.push_str(&format!(
            "bitsperlink={}\n",
            bitstream_len as f64 / num_arcs as f64
        ));
        s.push_str(&format!(
            "bitspernode={}\n",
            bitstream_len as f64 / num_nodes as f64
        ));
        s.push_str(&format!("length={bitstream_len}\n"));

        fn stirling(n: u64) -> f64 {
            let n = n as f64;
            n * (n.ln() - 1.0) + 0.5 * (2.0 * std::f64::consts::PI * n).ln()
        }

        let n_squared = (num_nodes * num_nodes) as u64;
        let theoretical_bound =
            (stirling(n_squared) - stirling(num_arcs) - stirling(n_squared - num_arcs))
                / 2.0_f64.ln();
        s.push_str(&format!(
            "compratio={:.3}\n",
            bitstream_len as f64 / theoretical_bound
        ));

        s.push_str("compressionflags=");
        let mut comp_flags = false;
        if self.outdegrees != Codes::Gamma {
            s.push_str(&format!(
                "OUTDEGREES_{}|",
                Self::code_to_str(self.outdegrees, version).with_context(|| format!(
                    "Code {:?} is not supported for outdegrees in version {version}",
                    self.outdegrees
                ))?
            ));
            comp_flags = true;
        }
        if self.references != Codes::Unary {
            s.push_str(&format!(
                "REFERENCES_{}|",
                Self::code_to_str(self.references, version).with_context(|| format!(
                    "Code {:?} is not supported for references in version {version}",
                    self.references
                ))?
            ));
            comp_flags = true;
        }
        if self.blocks != Codes::Gamma {
            s.push_str(&format!(
                "BLOCKS_{}|",
                Self::code_to_str(self.blocks, version).with_context(|| format!(
                    "Code {:?} is not supported for blocks in version {version}",
                    self.blocks
                ))?
            ));
            comp_flags = true;
        }
        if self.intervals != Codes::Gamma {
            s.push_str(&format!(
                "INTERVALS_{}|",
                Self::code_to_str(self.intervals, version).with_context(|| format!(
                    "Code {:?} is not supported for intervals in version {version}",
                    self.intervals
                ))?
            ));
            comp_flags = true;
        }
        if (version == 0 && !matches!(self.residuals, Codes::Zeta(_)))
            || self.residuals != (Codes::Zeta(3))
        {
            s.push_str(&format!(
                "RESIDUALS_{}|",
                Self::code_to_str(self.residuals, version).with_context(|| format!(
                    "Code {:?} is not supported for residuals in version {version}",
                    self.residuals
                ))?
            ));
            comp_flags = true;
        }
        if comp_flags {
            s.pop();
        }
        s.push('\n');
        if version == 0 {
            // check that if a k is specified, it is the same for all codes
            let mut k = None;
            macro_rules! check_and_set_k {
                ($code:expr) => {
                    match $code {
                        Codes::Zeta(new_k) => {
                            if let Some(old_k) = k {
                                ensure!(
                                    old_k == new_k,
                                    "Only one value of k is supported in version 0"
                                )
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
        }
        Ok(s)
    }

    /// Convert the decoded `.properties` file into a `CompFlags` struct.
    /// Also check that the endianness is correct.
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
        if let Some(spec_k) = map.get("zetak") {
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
                    let code = CompFlags::code_from_str(s[1], k).unwrap();
                    match s[0] {
                        "OUTDEGREES" => cf.outdegrees = code,
                        "REFERENCES" => cf.references = code,
                        "BLOCKS" => cf.blocks = code,
                        "INTERVALS" => cf.intervals = code,
                        "RESIDUALS" => cf.residuals = code,
                        "OFFSETS" => {
                            ensure!(code == Codes::Gamma, "Only γ code is supported for offsets")
                        }
                        _ => bail!("Unknown compression flag {}", flag),
                    }
                }
            }
        }
        if let Some(compression_window) = map.get("windowsize") {
            cf.compression_window = compression_window.parse()?;
        }
        if let Some(min_interval_length) = map.get("minintervallength") {
            cf.min_interval_length = min_interval_length.parse()?;
        }
        if let Some(max_ref_count) = map.get("maxrefcount") {
            cf.max_ref_count = max_ref_count.parse()?;
        }
        Ok(cf)
    }
}
