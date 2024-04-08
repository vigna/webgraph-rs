/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::path::PathBuf;

use crate::graphs::Code;
use crate::prelude::CompFlags;
use anyhow::anyhow;
use anyhow::ensure;
use clap::Args;
use clap::ValueEnum;
use common_traits::UnsignedInt;
use sysinfo::System;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
/// Our own enum for the codes, this is used to implement ValueEnum here
/// instead of in dsi-bitstream. We can also consider doing the opposite.
pub enum PrivCode {
    Unary,
    Gamma,
    Delta,
    Zeta1,
    Zeta2,
    Zeta3,
    Zeta4,
    Zeta5,
    Zeta6,
    Zeta7,
}

impl From<PrivCode> for Code {
    fn from(value: PrivCode) -> Self {
        match value {
            PrivCode::Unary => Code::Unary,
            PrivCode::Gamma => Code::Gamma,
            PrivCode::Delta => Code::Delta,
            PrivCode::Zeta1 => Code::Zeta { k: 1 },
            PrivCode::Zeta2 => Code::Zeta { k: 2 },
            PrivCode::Zeta3 => Code::Zeta { k: 3 },
            PrivCode::Zeta4 => Code::Zeta { k: 4 },
            PrivCode::Zeta5 => Code::Zeta { k: 5 },
            PrivCode::Zeta6 => Code::Zeta { k: 6 },
            PrivCode::Zeta7 => Code::Zeta { k: 7 },
        }
    }
}

#[derive(Args, Debug)]
/// Args needed to parse a standard csv file
pub struct CSVArgs {
    #[arg(long, default_value_t = '#')]
    /// Ignore lines that start with this symbol
    pub line_comment_simbol: char,

    #[arg(long, default_value_t = 0)]
    /// How many lines to skip, ignoring comment lines
    pub lines_to_skip: usize,

    #[arg(long)]
    /// How many lines to parse,
    /// after skipping the first lines_to_skip
    /// and ignoring comment lines
    pub max_lines: Option<usize>,

    #[arg(long, default_value_t = ',')]
    /// The index of the column containing the source node str.
    pub csv_separator: char,

    #[arg(long, default_value_t = 0)]
    /// The index of the column containing the source node str.
    pub src_column: usize,

    #[arg(long, default_value_t = 1)]
    /// The index of the column containing the source node str.
    pub dst_column: usize,

    #[arg(long, default_value_t = false)]
    /// If src and dst are already valid node_ids.
    pub numeric: bool,
}

#[derive(Args, Debug)]
pub struct NumCpusArg {
    #[arg(short = 'j', long, default_value_t = rayon::current_num_threads().max(1))]
    /// The number of cores to use
    pub num_cpus: usize,
}

#[derive(Args, Debug)]
/// Shared cli arguments for permuting a graph
/// Reference on how to use it: <https://stackoverflow.com/questions/75514455/how-to-parse-common-subcommand-arguments-with-clap-in-rust>
pub struct PermutationArgs {
    /* TODO!:
    #[arg(short = 'e', long, default_value_t = false)]
    /// Load the permutations from ε-serde format instead of the java format, i.e. an array of 64-bit big-endian integers
    epserde: bool,
    */
    #[clap(long)]
    /// The path to the permutations to, optionally, apply to the graph.
    pub permutation: Option<PathBuf>,

    #[clap(short = 'b', long, value_parser = batch_size, default_value = "50%")]
    /// The number of pairs to be used in batches. Two times this number of
    /// `usize` will be allocated to sort pairs. You can use the SI and NIST
    /// multipliers k, M, G, T, P, ki, Mi, Gi, Ti, and Pi. You can also use a
    /// percentage of the available memory by appending a `%` to the number.
    pub batch_size: usize,
}

/// Parses a batch size.
///
/// This function accepts either a number (possibly followed by a
/// SI or NIST multiplier k, M, G, T, P, ki, Mi, Gi, Ti, or Pi), or a percentage
/// (followed by a `%`) that is interpreted as a percentage of the core
/// memory. The function returns the number of pairs to be used for batches.
pub fn batch_size(arg: &str) -> anyhow::Result<usize> {
    const PREF_SYMS: [(&str, u64); 10] = [
        ("k", 1E3 as u64),
        ("m", 1E6 as u64),
        ("g", 1E9 as u64),
        ("t", 1E12 as u64),
        ("p", 1E15 as u64),
        ("ki", 1 << 10),
        ("mi", 1 << 20),
        ("gi", 1 << 30),
        ("ti", 1 << 40),
        ("pi", 1 << 50),
    ];
    let arg = arg.trim().to_ascii_lowercase();
    ensure!(!arg.is_empty(), "empty string");

    if arg.ends_with('%') {
        let perc = arg[..arg.len() - 1].parse::<f64>()?;
        ensure!(perc >= 0.0 || perc <= 100.0, "percentage out of range");
        let mut system = System::new();
        system.refresh_memory();
        let num_pairs: usize = (((system.total_memory() as f64) * (perc / 100.0)
            / (std::mem::size_of::<(usize, usize)>() as f64))
            as u64)
            .try_into()?;
        return Ok(num_pairs.align_to(1 << 20)); // Round up to MiBs
    }

    arg.chars().position(|c| c.is_alphabetic()).map_or_else(
        || Ok(arg.parse::<usize>()?),
        |pos| {
            let (num, pref_sym) = arg.split_at(pos);
            let multiplier = PREF_SYMS
                .iter()
                .find(|(x, _)| *x == pref_sym)
                .map(|(_, m)| m)
                .ok_or(anyhow!("invalid prefix symbol"))?;

            Ok((num.parse::<u64>()? * multiplier).try_into()?)
        },
    )
}

#[derive(Args, Debug)]
/// Shared cli arguments for compression
/// Reference on how to use it: <https://stackoverflow.com/questions/75514455/how-to-parse-common-subcommand-arguments-with-clap-in-rust>
pub struct CompressArgs {
    /// The endianess of the graph to write
    #[clap(short = 'E', long)]
    pub endianess: Option<String>,

    /// The compression windows
    #[clap(short = 'w', long, default_value_t = 7)]
    pub compression_window: usize,
    /// The minimum interval length
    #[clap(short = 'l', long, default_value_t = 4)]
    pub min_interval_length: usize,
    /// The maximum recursion depth for references (-1 for infinite recursion depth)
    #[clap(short = 'c', long, default_value_t = 3)]
    pub max_ref_count: isize,

    #[arg(value_enum)]
    #[clap(long, default_value = "gamma")]
    /// The code to use for the outdegree
    pub outdegrees: PrivCode,

    #[arg(value_enum)]
    #[clap(long, default_value = "unary")]
    /// The code to use for the reference offsets
    pub references: PrivCode,

    #[arg(value_enum)]
    #[clap(long, default_value = "gamma")]
    /// The code to use for the blocks
    pub blocks: PrivCode,

    #[arg(value_enum)]
    #[clap(long, default_value = "zeta3")]
    /// The code to use for the residuals
    pub residuals: PrivCode,
}

impl From<CompressArgs> for CompFlags {
    fn from(value: CompressArgs) -> Self {
        CompFlags {
            outdegrees: value.outdegrees.into(),
            references: value.references.into(),
            blocks: value.blocks.into(),
            intervals: PrivCode::Gamma.into(),
            residuals: value.residuals.into(),
            min_interval_length: value.min_interval_length,
            compression_window: value.compression_window,
            max_ref_count: match value.max_ref_count {
                -1 => usize::MAX,
                _ => value.max_ref_count as usize,
            },
        }
    }
}
