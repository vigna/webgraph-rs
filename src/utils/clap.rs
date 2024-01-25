/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::CompFlags;
use clap::Args;
use clap::ValueEnum;
use dsi_bitstream::codes::Code;
use rand::Rng;
use std::path::Path;

/// Create a new random dir inside the given folder
pub fn temp_dir<P: AsRef<Path>>(base: P) -> String {
    let mut base = base.as_ref().to_owned();
    const ALPHABET: &[u8] = b"0123456789abcdef";
    let mut rnd = rand::thread_rng();
    let mut random_str = String::new();
    loop {
        random_str.clear();
        for _ in 0..16 {
            let idx = rnd.gen_range(0..ALPHABET.len());
            random_str.push(ALPHABET[idx] as char);
        }
        base.push(&random_str);

        if !base.exists() {
            std::fs::create_dir(&base).unwrap();
            return base.to_string_lossy().to_string();
        }
        base.pop();
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
/// Our own enum for the codes, this is used to implement ValueEnum here
/// instead of in dsi-bitstream. We can also consider doing the opposite.
pub enum PrivCode {
    Unary,
    Gamma,
    Delta,
    Zeta3,
}

impl From<PrivCode> for Code {
    fn from(value: PrivCode) -> Self {
        match value {
            PrivCode::Unary => Code::Unary,
            PrivCode::Gamma => Code::Gamma,
            PrivCode::Delta => Code::Delta,
            PrivCode::Zeta3 => Code::Zeta { k: 3 },
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
/// Shared cli arguments for permutating a graph
/// Reference on how to use it: <https://stackoverflow.com/questions/75514455/how-to-parse-common-subcommand-arguments-with-clap-in-rust>
pub struct PermutationArgs {
    #[clap(short = 's', long, default_value_t = 1_000_000)]
    /// The size of a batch.
    pub batch_size: usize,

    /// Directory where of the **MANY LARGE** temporary files.
    /// If not passed it creates a random folder inside either
    /// the env var `TMPDIR` or `/tmp` if the env var is not specified.
    #[arg(short = 't', long, default_value_t = std::env::temp_dir().to_string_lossy().to_string())]
    pub temp_dir: String,
}

#[derive(Args, Debug)]
/// Shared cli arguments for compression
/// Reference on how to use it: <https://stackoverflow.com/questions/75514455/how-to-parse-common-subcommand-arguments-with-clap-in-rust>
pub struct CompressArgs {
    /// The endianess of the graph to write
    #[clap(short = 'e', long)]
    pub endianess: Option<String>,

    /// The compression windows
    #[clap(short = 'w', long, default_value_t = 7)]
    pub compression_window: usize,
    /// The minimum interval length
    #[clap(short = 'l', long, default_value_t = 4)]
    pub min_interval_length: usize,
    /// The maximum recursion depth for references
    #[clap(short = 'c', long, default_value_t = 3)]
    pub max_ref_count: usize,

    #[arg(value_enum)]
    #[clap(short, long, default_value = "gamma")]
    /// The code to use for the outdegree
    pub outdegrees_code: PrivCode,

    #[arg(value_enum)]
    #[clap(short, long, default_value = "unary")]
    /// The code to use for the reference offsets
    pub references_code: PrivCode,

    #[arg(value_enum)]
    #[clap(short, long, default_value = "gamma")]
    /// The code to use for the blocks
    pub blocks_code: PrivCode,

    #[arg(value_enum)]
    #[clap(short, long, default_value = "gamma")]
    /// The code to use for the intervals
    pub intervals_code: PrivCode,

    #[arg(value_enum)]
    #[clap(short = 'e', long, default_value = "zeta3")]
    /// The code to use for the residuals
    pub residuals_code: PrivCode,
}

impl From<CompressArgs> for CompFlags {
    fn from(value: CompressArgs) -> Self {
        CompFlags {
            outdegrees: value.outdegrees_code.into(),
            references: value.references_code.into(),
            blocks: value.blocks_code.into(),
            intervals: value.intervals_code.into(),
            residuals: value.residuals_code.into(),
            min_interval_length: value.min_interval_length,
            compression_window: value.compression_window,
            max_ref_count: value.max_ref_count,
        }
    }
}
