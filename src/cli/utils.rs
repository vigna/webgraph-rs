/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use clap::Args;
use clap::ValueEnum;
use webgraph::graphs::Code;
use webgraph::prelude::CompFlags;

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
            max_ref_count: value.max_ref_count,
        }
    }
}
