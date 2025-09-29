/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Command-line interface structs, functions, and methods.
//!
//! Each module correspond to a group of commands, and each command is
//! implemented as a submodule.

use anyhow::{anyhow, bail, ensure, Context, Result};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use common_traits::{ToBytes, UnsignedInt};
use dsi_bitstream::dispatch::Codes;
use epserde::ser::Serialize;
use jiff::fmt::friendly::{Designator, Spacing, SpanPrinter};
use jiff::SpanRound;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::SystemTime;
use sux::bits::BitFieldVec;
use sysinfo::System;
use webgraph::prelude::CompFlags;
use webgraph::utils::Granularity;

#[cfg(not(any(feature = "le_bins", feature = "be_bins")))]
compile_error!("At least one of the features `le_bins` or `be_bins` must be enabled.");

pub mod build_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));

    pub fn version_string() -> String {
        format!(
            "{}
git info: {} {} {}
build info: built on {} for {} with {}",
            PKG_VERSION,
            GIT_VERSION.unwrap_or(""),
            GIT_COMMIT_HASH.unwrap_or(""),
            match GIT_DIRTY {
                None => "",
                Some(true) => "(dirty)",
                Some(false) => "(clean)",
            },
            BUILD_DATE,
            TARGET,
            RUSTC_VERSION
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
/// Enum for instantaneous codes.
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
    pub line_comment_symbol: char,

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

/// Parses the number of threads from a string.
///
/// This function is meant to be used with `#[arg(...,  value_parser =
/// num_threads_parser)]`.
pub fn num_threads_parser(arg: &str) -> Result<usize> {
    let num_threads = arg.parse::<usize>()?;
    ensure!(num_threads > 0, "Number of threads must be greater than 0");
    Ok(num_threads)
}

/// Shared CLI arguments for commands that specify a number of threads.
#[derive(Args, Debug)]
pub struct NumThreadsArg {
    #[arg(short = 'j', long, default_value_t = rayon::current_num_threads().max(1), value_parser = num_threads_parser)]
    /// The number of threads to use.
    pub num_threads: usize,
}

/// Shared CLI arguments for commands that specify a granularity.
#[derive(Args, Debug)]
pub struct GranularityArgs {
    #[arg(long, conflicts_with("node_granularity"))]
    /// The tentative number of arcs used to define the size of a parallel job
    /// (advanced option).
    pub arc_granularity: Option<u64>,

    #[arg(long, conflicts_with("arc_granularity"))]
    /// The tentative number of nodes used to define the size of a parallel job
    /// (advanced option).
    pub node_granularity: Option<usize>,
}

impl GranularityArgs {
    pub fn into_granularity(&self) -> Granularity {
        match (self.arc_granularity, self.node_granularity) {
            (Some(_), Some(_)) => unreachable!(),
            (Some(arc_granularity), None) => Granularity::Arcs(arc_granularity),
            (None, Some(node_granularity)) => Granularity::Nodes(node_granularity),
            (None, None) => Granularity::default(),
        }
    }
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

#[derive(Debug, Clone, Copy, ValueEnum)]
/// How to store vectors of floats.
pub enum FloatVectorFormat {
    /// Java-compatible format: a sequence of big-endian floats (32 or 64 bits).
    Java,
    /// A slice of floats (32 or 64 bits) serialized using ε-serde.
    Epserde,
    /// ASCII format, one float per line.
    Ascii,
    /// A JSON Array.
    Json,
}

impl FloatVectorFormat {
    /// Stores float values in the specified `path` using the format defined by
    /// `self`.
    ///
    /// If the result is a textual format, i.e., ASCII or JSON, `precision`
    /// will be used to truncate the float values to the specified number of
    /// decimal digits.
    pub fn store<F>(
        &self,
        path: impl AsRef<Path>,
        values: &[F],
        precision: Option<usize>,
    ) -> Result<()>
    where
        F: ToBytes + core::fmt::Display + epserde::ser::Serialize + Copy,
        for<'a> &'a [F]: epserde::ser::Serialize,
    {
        let precision = precision.unwrap_or(f64::DIGITS as usize);
        create_parent_dir(&path)?;
        let path_display = path.as_ref().display();
        let mut file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create vector at {}", path_display))?;

        match self {
            FloatVectorFormat::Epserde => {
                log::info!("Storing in ε-serde format at {}", path_display);
                values
                    .serialize(&mut file)
                    .with_context(|| format!("Could not write vector to {}", path_display))?;
            }
            FloatVectorFormat::Java => {
                log::info!("Storing in Java format at {}", path_display);
                for word in values.iter() {
                    file.write_all(word.to_be_bytes().as_ref())
                        .with_context(|| format!("Could not write vector to {}", path_display))?;
                }
            }
            FloatVectorFormat::Ascii => {
                log::info!("Storing in ASCII format at {}", path_display);
                for word in values.iter() {
                    writeln!(file, "{word:.precision$}")
                        .with_context(|| format!("Could not write vector to {}", path_display))?;
                }
            }
            FloatVectorFormat::Json => {
                log::info!("Storing in JSON format at {}", path_display);
                write!(file, "[")?;
                for word in values.iter().take(values.len().saturating_sub(2)) {
                    write!(file, "{word:.precision$}, ")
                        .with_context(|| format!("Could not write vector to {}", path_display))?;
                }
                if let Some(last) = values.last() {
                    write!(file, "{last:.precision$}")
                        .with_context(|| format!("Could not write vector to {}", path_display))?;
                }
                write!(file, "]")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
/// How to store vectors of integers.
pub enum IntVectorFormat {
    /// Java-compatible format: a sequence of big-endian longs (64 bits).
    Java,
    /// A slice of usize serialized using ε-serde.
    Epserde,
    /// A BitFieldVec stored using ε-serde. It stores each element using
    /// ⌊log₂(max)⌋ + 1 bits. It requires to allocate the `BitFieldVec` in RAM
    /// before serializing it.
    BitFieldVec,
    /// ASCII format, one float per line.
    Ascii,
    /// A JSON Array.
    Json,
}

impl IntVectorFormat {
    /// Stores a vector of `u64` in the specified `path`` using the format defined by `self`.
    ///
    /// `max` is the maximum value of the vector. If it is not provided, it will
    /// be computed from the data.
    pub fn store(&self, path: impl AsRef<Path>, data: &[u64], max: Option<u64>) -> Result<()> {
        // Ensure the parent directory exists
        create_parent_dir(&path)?;

        let mut file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create vector at {}", path.as_ref().display()))?;
        let mut buf = BufWriter::new(&mut file);

        debug_assert_eq!(
            max,
            max.map(|_| { data.iter().copied().max().unwrap_or(0) }),
            "The wrong maximum value was provided for the vector"
        );

        match self {
            IntVectorFormat::Epserde => {
                log::info!("Storing in epserde format at {}", path.as_ref().display());
                data.serialize(&mut buf).with_context(|| {
                    format!("Could not write vector to {}", path.as_ref().display())
                })?;
            }
            IntVectorFormat::BitFieldVec => {
                log::info!(
                    "Storing in BitFieldVec format at {}",
                    path.as_ref().display()
                );
                let max = max.unwrap_or_else(|| {
                    data.iter()
                        .copied()
                        .max()
                        .unwrap_or_else(|| panic!("Empty vector"))
                });
                let bit_width = max.len() as usize;
                log::info!("Using {} bits per element", bit_width);
                let mut bit_field_vec = <BitFieldVec<u64, _>>::with_capacity(bit_width, data.len());
                bit_field_vec.extend(data.iter().copied());
                bit_field_vec.store(&path).with_context(|| {
                    format!("Could not write vector to {}", path.as_ref().display())
                })?;
            }
            IntVectorFormat::Java => {
                log::info!("Storing in Java format at {}", path.as_ref().display());
                for word in data.iter() {
                    buf.write_all(&word.to_be_bytes()).with_context(|| {
                        format!("Could not write vector to {}", path.as_ref().display())
                    })?;
                }
            }
            IntVectorFormat::Ascii => {
                log::info!("Storing in ASCII format at {}", path.as_ref().display());
                for word in data.iter() {
                    writeln!(buf, "{}", word).with_context(|| {
                        format!("Could not write vector to {}", path.as_ref().display())
                    })?;
                }
            }
            IntVectorFormat::Json => {
                log::info!("Storing in JSON format at {}", path.as_ref().display());
                write!(buf, "[")?;
                for word in data.iter().take(data.len().saturating_sub(2)) {
                    write!(buf, "{}, ", word).with_context(|| {
                        format!("Could not write vector to {}", path.as_ref().display())
                    })?;
                }
                if let Some(last) = data.last() {
                    write!(buf, "{}", last).with_context(|| {
                        format!("Could not write vector to {}", path.as_ref().display())
                    })?;
                }
                write!(buf, "]")?;
            }
        };

        Ok(())
    }

    #[cfg(target_pointer_width = "64")]
    /// Stores a vector of `usize` in the specified `path` using the format defined by `self`.
    /// `max` is the maximum value of the vector, if it is not provided, it will
    /// be computed from the data.
    ///
    /// This helper method is available only on 64-bit architectures as Java's format
    /// uses of 64-bit integers.
    pub fn store_usizes(
        &self,
        path: impl AsRef<Path>,
        data: &[usize],
        max: Option<usize>,
    ) -> Result<()> {
        self.store(
            path,
            unsafe { core::mem::transmute::<&[usize], &[u64]>(data) },
            max.map(|x| x as u64),
        )
    }
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

/// Creates a [`ThreadPool`](rayon::ThreadPool) with the given number of threads.
pub fn get_thread_pool(num_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
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

/// Creates all parent directories of the given file path.
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

/// Parse a duration from a string.
/// For compatibility with Java, if no suffix is given, it is assumed to be in milliseconds.
/// You can use suffixes, the available ones are:
/// - `s` for seconds
/// - `m` for minutes
/// - `h` for hours
/// - `d` for days
///
/// Example: `1d2h3m4s567` this is parsed as: 1 day, 2 hours, 3 minutes, 4 seconds, and 567 milliseconds.
fn parse_duration(value: &str) -> Result<Duration> {
    if value.is_empty() {
        bail!("Empty duration string, if you want every 0 milliseconds use `0`.");
    }
    let mut duration = Duration::from_secs(0);
    let mut acc = String::new();
    for c in value.chars() {
        if c.is_ascii_digit() {
            acc.push(c);
        } else if c.is_whitespace() {
            continue;
        } else {
            let dur = acc.parse::<u64>()?;
            match c {
                's' => duration += Duration::from_secs(dur),
                'm' => duration += Duration::from_secs(dur * 60),
                'h' => duration += Duration::from_secs(dur * 60 * 60),
                'd' => duration += Duration::from_secs(dur * 60 * 60 * 24),
                _ => return Err(anyhow!("Invalid duration suffix: {}", c)),
            }
            acc.clear();
        }
    }
    if !acc.is_empty() {
        let dur = acc.parse::<u64>()?;
        duration += Duration::from_millis(dur);
    }
    Ok(duration)
}

pub fn init_env_logger() -> Result<()> {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));

    let start = std::time::Instant::now();
    let printer = SpanPrinter::new()
        .spacing(Spacing::None)
        .designator(Designator::Compact);
    let span_round = SpanRound::new()
        .largest(jiff::Unit::Day)
        .smallest(jiff::Unit::Millisecond)
        .days_are_24_hours();

    builder.format(move |buf, record| {
        let Ok(ts) = jiff::Timestamp::try_from(SystemTime::now()) else {
            return Err(std::io::Error::other("Failed to get timestamp"));
        };
        let style = buf.default_level_style(record.level());
        let elapsed = start.elapsed();
        let span = jiff::Span::new()
            .seconds(elapsed.as_secs() as i64)
            .milliseconds(elapsed.subsec_millis() as i64);
        let span = span.round(span_round).expect("Failed to round span");
        writeln!(
            buf,
            "{} {} {style}{}{style:#} [{:?}] {} - {}",
            ts.strftime("%F %T%.3f"),
            printer.span_to_string(&span),
            record.level(),
            std::thread::current().id(),
            record.target(),
            record.args()
        )
    });
    builder.init();
    Ok(())
}

#[derive(Args, Debug)]
pub struct GlobalArgs {
    #[arg(long, value_parser = parse_duration, global=true, display_order = 1000)]
    /// How often to log progress. Default is 10s. You can use the suffixes `s`
    /// for seconds, `m` for minutes, `h` for hours, and `d` for days. If no
    /// suffix is provided it is assumed to be in milliseconds.
    /// Example: `1d2h3m4s567` is parsed as 1 day + 2 hours + 3 minutes + 4
    /// seconds + 567 milliseconds = 93784567 milliseconds.
    pub log_interval: Option<Duration>,
}

#[derive(Subcommand, Debug)]
pub enum SubCommands {
    #[command(subcommand)]
    Analyze(analyze::SubCommands),
    #[command(subcommand)]
    Bench(bench::SubCommands),
    #[command(subcommand)]
    Build(build::SubCommands),
    #[command(subcommand)]
    Check(check::SubCommands),
    #[command(subcommand)]
    From(from::SubCommands),
    #[command(subcommand)]
    Perm(perm::SubCommands),
    #[command(subcommand)]
    Run(run::SubCommands),
    #[command(subcommand)]
    To(to::SubCommands),
    #[command(subcommand)]
    Transform(transform::SubCommands),
}

#[derive(Parser, Debug)]
#[command(name = "webgraph", version=build_info::version_string())]
/// Webgraph tools to build, convert, modify, and analyze graphs.
///
/// Noteworthy environment variables:
///
/// - RUST_MIN_STACK: minimum thread stack size (in bytes); we suggest
///   RUST_MIN_STACK=8388608 (8MiB)
///
/// - TMPDIR: where to store temporary files (potentially very large ones)
///
/// - RUST_LOG: configuration for env_logger
///   <https://docs.rs/env_logger/latest/env_logger/>
pub struct Cli {
    #[command(subcommand)]
    pub command: SubCommands,
    #[clap(flatten)]
    pub args: GlobalArgs,
}

pub mod dist;
pub mod sccs;

pub mod analyze;
pub mod bench;
pub mod build;
pub mod check;
pub mod from;
pub mod perm;
pub mod run;
pub mod to;
pub mod transform;

/// The entry point of the command-line interface.
pub fn cli_main<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let start = std::time::Instant::now();
    let cli = Cli::parse_from(args);
    match cli.command {
        SubCommands::Analyze(args) => {
            analyze::main(cli.args, args)?;
        }
        SubCommands::Bench(args) => {
            bench::main(cli.args, args)?;
        }
        SubCommands::Build(args) => {
            build::main(cli.args, args, Cli::command())?;
        }
        SubCommands::Check(args) => {
            check::main(cli.args, args)?;
        }
        SubCommands::From(args) => {
            from::main(cli.args, args)?;
        }
        SubCommands::Perm(args) => {
            perm::main(cli.args, args)?;
        }
        SubCommands::Run(args) => {
            run::main(cli.args, args)?;
        }
        SubCommands::To(args) => {
            to::main(cli.args, args)?;
        }
        SubCommands::Transform(args) => {
            transform::main(cli.args, args)?;
        }
    }

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
