/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Command-line interface structs, functions, and methods.
//!
//! Each module correspond to a group of commands, and each command is
//! implemented as a submodule.

use crate::build_info;
use crate::prelude::CompFlags;
use anyhow::{anyhow, ensure, Context, Result};
use clap::{Args, Command, ValueEnum};
use common_traits::UnsignedInt;
use dsi_bitstream::codes::Codes;
use std::path::{Path, PathBuf};
use sysinfo::System;

pub mod analyze;
pub mod bench;
pub mod build;
pub mod check;
pub mod from;
pub mod perm;
pub mod run;
pub mod to;
pub mod transform;

pub const DEFAULT_STACK_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
/// Enum for instantanous codes.
///
/// It is used to implement [`ValueEnum`] here instead of in [`dsi_bitstream`].
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

impl From<PrivCode> for Codes {
    fn from(value: PrivCode) -> Self {
        match value {
            PrivCode::Unary => Codes::Unary,
            PrivCode::Gamma => Codes::Gamma,
            PrivCode::Delta => Codes::Delta,
            PrivCode::Zeta1 => Codes::Zeta { k: 1 },
            PrivCode::Zeta2 => Codes::Zeta { k: 2 },
            PrivCode::Zeta3 => Codes::Zeta { k: 3 },
            PrivCode::Zeta4 => Codes::Zeta { k: 4 },
            PrivCode::Zeta5 => Codes::Zeta { k: 5 },
            PrivCode::Zeta6 => Codes::Zeta { k: 6 },
            PrivCode::Zeta7 => Codes::Zeta { k: 7 },
        }
    }
}

#[derive(Args, Debug)]
/// Shared CLI arguments for reading files containing arcs.
pub struct ArcsArgs {
    #[arg(long, default_value_t = '#')]
    /// Ignore lines that start with this symbol.
    pub line_comment_simbol: char,

    #[arg(long, default_value_t = 0)]
    /// How many lines to skip, ignoring comment lines.
    pub lines_to_skip: usize,

    #[arg(long)]
    /// How many lines to parse, after skipping the first lines_to_skip and
    /// ignoring comment lines.
    pub max_arcs: Option<usize>,

    #[arg(long, default_value_t = '\t')]
    /// The column separator.
    pub separator: char,

    #[arg(long, default_value_t = 0)]
    /// The index of the column containing the source node of an arc.
    pub source_column: usize,

    #[arg(long, default_value_t = 1)]
    /// The index of the column containing the target node of an arc.
    pub target_column: usize,

    #[arg(long, default_value_t = false)]
    /// Source and destinations are node identifiers.
    pub exact: bool,
}

/// Shared CLI arguments for commands that specify a number of threads.
#[derive(Args, Debug)]
pub struct NumThreadsArg {
    #[arg(short = 'j', long, default_value_t = rayon::current_num_threads().max(1))]
    /// The number of threads to use
    pub num_threads: usize,
}

/// Shared CLI arguments for commands that specify a batch size.
#[derive(Args, Debug)]
pub struct BatchSizeArg {
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
        // TODO: try_align_to when available
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
/// Shared CLI arguments for compression.
pub struct CompressArgs {
    /// The endianness of the graph to write
    #[clap(short = 'E', long)]
    pub endianness: Option<String>,

    /// The compression windows
    #[clap(short = 'w', long, default_value_t = 7)]
    pub compression_window: usize,
    /// The minimum interval length
    #[clap(short = 'i', long, default_value_t = 4)]
    pub min_interval_length: usize,
    /// The maximum recursion depth for references (-1 for infinite recursion depth)
    #[clap(short = 'r', long, default_value_t = 3)]
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

/// Create a threadpool with the given number of threads and set the stack to either the env var or to
/// the default stack size `DEFAULT_STACK_SIZE`.
pub fn get_thread_pool(num_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .stack_size(
            std::env::var("RUST_MIN_STACK")
                .map(|x| dbg!(x.parse::<usize>().unwrap()))
                .unwrap_or(crate::cli::DEFAULT_STACK_SIZE),
        )
        .build()
        .expect("Failed to create thread pool")
}

/// Appends a string to the filename of a path.
///
/// # Panics
/// * Will panic if there is no filename.
/// * Will panic in test mode if the path has an extension.
pub fn append(path: impl AsRef<Path>, s: impl AsRef<str>) -> PathBuf {
    debug_assert!(path.as_ref().extension().is_none());
    let mut path_buf = path.as_ref().to_owned();
    let mut filename = path_buf.file_name().unwrap().to_owned();
    filename.push(s.as_ref());
    path_buf.push(filename);
    path_buf
}

/// Create all parent directories of the given file path.
pub fn create_parent_dir(file_path: impl AsRef<Path>) -> Result<()> {
    // ensure that the dst directory exists
    if let Some(parent_dir) = file_path.as_ref().parent() {
        std::fs::create_dir_all(parent_dir).with_context(|| {
            format!(
                "Failed to create the directory {:?}",
                parent_dir.to_string_lossy()
            )
        })?;
    }
    Ok(())
}

/// The entry point of the command-line interface.
pub fn main<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let start = std::time::Instant::now();
    // it's ok to fail since this might be called multiple times in tests
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    let command = Command::new("webgraph")
        .about("Webgraph tools to build, convert, modify, and analyze webgraph files.")
        .version(build_info::version_string())
        .subcommand_required(true)
        .arg_required_else_help(true)
        .after_help(
            "Environment (noteworthy environment variables used):
RUST_MIN_STACK: minimum thread stack size (in bytes)
TMPDIR: where to store temporary files (potentially very large ones)
",
        );

    macro_rules! impl_dispatch {
        ($command:expr, $($module:ident),*) => {{
            let command = build::cli($command);
            $(
                let command = $module::cli(command);
            )*
            let command = command.display_order(0); // sort args alphabetically
            let mut completion_command = command.clone();
            let matches = command.get_matches_from(args);
            let subcommand = matches.subcommand();
            // if no command is specified, print the help message
            if subcommand.is_none() {
                completion_command.print_help().unwrap();
                return Ok(());
            }
            match subcommand.unwrap() {
                (build::COMMAND_NAME, sub_m) => build::main(sub_m, &mut completion_command),
                $(
                    ($module::COMMAND_NAME, sub_m) => $module::main(sub_m),
                )*
                (command_name, _) => {
                    // this shouldn't happen as clap should catch this
                    eprintln!("Unknown command: {:?}", command_name);
                    completion_command.print_help().unwrap();
                    std::process::exit(1);
                }
            }
        }};
    }

    impl_dispatch!(command, analyze, bench, check, from, perm, run, to, transform)?;

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}

/// Pretty prints seconds in a humanly readable format.
fn pretty_print_elapsed(elapsed: f64) -> String {
    let mut result = String::new();
    let mut elapsed_seconds = elapsed as u64;
    let weeks = elapsed_seconds / (60 * 60 * 24 * 7);
    elapsed_seconds %= 60 * 60 * 24 * 7;
    let days = elapsed_seconds / (60 * 60 * 24);
    elapsed_seconds %= 60 * 60 * 24;
    let hours = elapsed_seconds / (60 * 60);
    elapsed_seconds %= 60 * 60;
    let minutes = elapsed_seconds / 60;
    //elapsed_seconds %= 60;

    match weeks {
        0 => {}
        1 => result.push_str("1 week "),
        _ => result.push_str(&format!("{} weeks ", weeks)),
    }
    match days {
        0 => {}
        1 => result.push_str("1 day "),
        _ => result.push_str(&format!("{} days ", days)),
    }
    match hours {
        0 => {}
        1 => result.push_str("1 hour "),
        _ => result.push_str(&format!("{} hours ", hours)),
    }
    match minutes {
        0 => {}
        1 => result.push_str("1 minute "),
        _ => result.push_str(&format!("{} minutes ", minutes)),
    }

    result.push_str(&format!("{:.3} seconds ({}s)", elapsed % 60.0, elapsed));
    result
}
