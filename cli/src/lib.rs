/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![doc = include_str!("../README.md")]
#![deny(unstable_features)]
#![deny(trivial_casts)]
#![deny(unconditional_recursion)]
#![deny(clippy::empty_loop)]
#![deny(unreachable_code)]
#![deny(unreachable_pub)]
#![deny(unreachable_patterns)]
#![deny(unused_macro_rules)]
#![deny(unused_doc_comments)]
#![allow(clippy::type_complexity)]

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use common_traits::{AsBytes, FromBytes, ToBytes, UnsignedInt};
use dsi_bitstream::dispatch::Codes;
use epserde::deser::Deserialize;
use epserde::ser::Serialize;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::SystemTime;
use sux::bits::BitFieldVec;
use webgraph::prelude::CompFlags;
use webgraph::utils::{Granularity, MemoryUsage};

macro_rules! SEQ_PROC_WARN {
    () => {"Processing the graph sequentially: for parallel processing please build the Elias-Fano offsets list using 'webgraph build ef {}'"}
}

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
///
/// For CLI ergonomics and compatibility, these codes must be the same as those
/// appearing in [`CompFlags::code_from_str`].
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
    Pi1,
    Pi2,
    Pi3,
    Pi4,
}

impl From<PrivCode> for Codes {
    fn from(value: PrivCode) -> Self {
        match value {
            PrivCode::Unary => Codes::Unary,
            PrivCode::Gamma => Codes::Gamma,
            PrivCode::Delta => Codes::Delta,
            PrivCode::Zeta1 => Codes::Zeta(1),
            PrivCode::Zeta2 => Codes::Zeta(2),
            PrivCode::Zeta3 => Codes::Zeta(3),
            PrivCode::Zeta4 => Codes::Zeta(4),
            PrivCode::Zeta5 => Codes::Zeta(5),
            PrivCode::Zeta6 => Codes::Zeta(6),
            PrivCode::Zeta7 => Codes::Zeta(7),
            PrivCode::Pi1 => Codes::Pi(1),
            PrivCode::Pi2 => Codes::Pi(2),
            PrivCode::Pi3 => Codes::Pi(3),
            PrivCode::Pi4 => Codes::Pi(4),
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
    /// Sources and destinations are not node identifiers starting from 0, but labels.
    pub labels: bool,
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

/// Shared CLI arguments for commands that specify a memory usage.
#[derive(Args, Debug)]
pub struct MemoryUsageArg {
    #[clap(short = 'm', long = "memory-usage", value_parser = memory_usage_parser, default_value = "50%")]
    /// The number of pairs to be used in batches.
    /// If the number ends with a "b" or "B" it is interpreted as a number of bytes, otherwise as a number of elements.
    /// You can use the SI and NIST multipliers k, M, G, T, P, ki, Mi, Gi, Ti, and Pi.
    /// You can also use a percentage of the available memory by appending a "%" to the number.
    pub memory_usage: MemoryUsage,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
/// Formats for storing and loading vectors of floats.
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
    /// If the result is a textual format, that is, ASCII or JSON, `precision`
    /// will be used to truncate the float values to the specified number of
    /// decimal digits. If `None`, [zmij](https://crates.io/crates/zmij)
    /// formatting will be used.
    pub fn store<F>(
        &self,
        path: impl AsRef<Path>,
        values: &[F],
        precision: Option<usize>,
    ) -> Result<()>
    where
        F: ToBytes + core::fmt::Display + epserde::ser::Serialize + Copy + zmij::Float,
        for<'a> &'a [F]: epserde::ser::Serialize,
    {
        create_parent_dir(&path)?;
        let path_display = path.as_ref().display();
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create vector at {}", path_display))?;
        let mut file = BufWriter::new(file);

        match self {
            FloatVectorFormat::Epserde => {
                log::info!("Storing in ε-serde format at {}", path_display);
                unsafe {
                    values
                        .serialize(&mut file)
                        .with_context(|| format!("Could not write vector to {}", path_display))
                }?;
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
                let mut buf = zmij::Buffer::new();
                for word in values.iter() {
                    match precision {
                        None => writeln!(file, "{}", buf.format(*word)),
                        Some(precision) => writeln!(file, "{word:.precision$}"),
                    }
                    .with_context(|| format!("Could not write vector to {}", path_display))?;
                }
            }
            FloatVectorFormat::Json => {
                log::info!("Storing in JSON format at {}", path_display);
                let mut buf = zmij::Buffer::new();
                write!(file, "[")?;
                for word in values.iter().take(values.len().saturating_sub(1)) {
                    match precision {
                        None => write!(file, "{}, ", buf.format(*word)),
                        Some(precision) => write!(file, "{word:.precision$}, "),
                    }
                    .with_context(|| format!("Could not write vector to {}", path_display))?;
                }
                if let Some(last) = values.last() {
                    match precision {
                        None => write!(file, "{}", buf.format(*last)),
                        Some(precision) => write!(file, "{last:.precision$}"),
                    }
                    .with_context(|| format!("Could not write vector to {}", path_display))?;
                }
                write!(file, "]")?;
            }
        }

        Ok(())
    }

    /// Loads float values from the specified `path` using the format defined
    /// by `self`.
    pub fn load<F>(&self, path: impl AsRef<Path>) -> Result<Vec<F>>
    where
        F: FromBytes + std::str::FromStr + Copy,
        <F as AsBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
        <F as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
        Vec<F>: epserde::deser::Deserialize,
    {
        let path = path.as_ref();
        let path_display = path.display();

        match self {
            FloatVectorFormat::Epserde => {
                log::info!("Loading ε-serde format from {}", path_display);
                Ok(unsafe {
                    <Vec<F>>::load_full(path)
                        .with_context(|| format!("Could not load vector from {}", path_display))?
                })
            }
            FloatVectorFormat::Java => {
                log::info!("Loading Java format from {}", path_display);
                let file = std::fs::File::open(path)
                    .with_context(|| format!("Could not open {}", path_display))?;
                let file_len = file.metadata()?.len() as usize;
                let byte_size = size_of::<F>();
                ensure!(
                    file_len % byte_size == 0,
                    "File size ({}) is not a multiple of {} bytes",
                    file_len,
                    byte_size
                );
                let n = file_len / byte_size;
                let mut reader = BufReader::new(file);
                let mut result = Vec::with_capacity(n);
                let mut buf = vec![0u8; byte_size];
                for i in 0..n {
                    reader.read_exact(&mut buf).with_context(|| {
                        format!("Could not read value at index {i} from {}", path_display)
                    })?;
                    let bytes = buf.as_slice().try_into().map_err(|_| {
                        anyhow!("Could not convert bytes at index {i} in {}", path_display)
                    })?;
                    result.push(F::from_be_bytes(bytes));
                }
                Ok(result)
            }
            FloatVectorFormat::Ascii => {
                log::info!("Loading ASCII format from {}", path_display);
                let file = std::fs::File::open(path)
                    .with_context(|| format!("Could not open {}", path_display))?;
                let reader = BufReader::new(file);
                reader
                    .lines()
                    .enumerate()
                    .filter(|(_, line)| line.as_ref().map_or(true, |l| !l.trim().is_empty()))
                    .map(|(i, line)| {
                        let line = line.with_context(|| {
                            format!("Error reading line {} of {}", i + 1, path_display)
                        })?;
                        line.trim().parse::<F>().map_err(|e| {
                            anyhow!("Error parsing line {} of {}: {}", i + 1, path_display, e)
                        })
                    })
                    .collect()
            }
            FloatVectorFormat::Json => {
                log::info!("Loading JSON format from {}", path_display);
                let file = std::fs::File::open(path)
                    .with_context(|| format!("Could not open {}", path_display))?;
                let mut reader = BufReader::new(file);
                let mut result = Vec::new();
                let mut byte = [0u8; 1];

                // Skip whitespace and opening bracket
                loop {
                    reader
                        .read_exact(&mut byte)
                        .with_context(|| format!("Unexpected end of file in {}", path_display))?;
                    match byte[0] {
                        b'[' => break,
                        b if b.is_ascii_whitespace() => continue,
                        _ => bail!("Expected '[' at start of JSON array in {}", path_display),
                    }
                }

                // Parse comma-separated values until ']'
                let mut token = String::new();
                let mut index = 0usize;
                loop {
                    reader
                        .read_exact(&mut byte)
                        .with_context(|| format!("Unexpected end of file in {}", path_display))?;
                    match byte[0] {
                        b']' => {
                            let trimmed = token.trim();
                            if !trimmed.is_empty() {
                                result.push(trimmed.parse::<F>().map_err(|e| {
                                    anyhow!(
                                        "Error parsing element {} of {}: {}",
                                        index + 1,
                                        path_display,
                                        e
                                    )
                                })?);
                            }
                            break;
                        }
                        b',' => {
                            let trimmed = token.trim();
                            result.push(trimmed.parse::<F>().map_err(|e| {
                                anyhow!(
                                    "Error parsing element {} of {}: {}",
                                    index + 1,
                                    path_display,
                                    e
                                )
                            })?);
                            token.clear();
                            index += 1;
                        }
                        c => {
                            token.push(c as char);
                        }
                    }
                }
                Ok(result)
            }
        }
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
    /// ASCII format, one integer per line.
    Ascii,
    /// A JSON Array.
    Json,
}

impl IntVectorFormat {
    /// Stores a vector of `u64` in the specified `path` using the format defined by `self`.
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
                unsafe {
                    data.serialize(&mut buf).with_context(|| {
                        format!("Could not write vector to {}", path.as_ref().display())
                    })
                }?;
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
                unsafe {
                    bit_field_vec.store(&path).with_context(|| {
                        format!("Could not write vector to {}", path.as_ref().display())
                    })
                }?;
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
                for word in data.iter().take(data.len().saturating_sub(1)) {
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
    /// uses 64-bit integers.
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
/// memory. If the value ends with a `b` or `B` it is interpreted as a number of
/// bytes, otherwise as a number of elements.
pub fn memory_usage_parser(arg: &str) -> anyhow::Result<MemoryUsage> {
    const PREF_SYMS: [(&str, u64); 10] = [
        ("ki", 1 << 10),
        ("mi", 1 << 20),
        ("gi", 1 << 30),
        ("ti", 1 << 40),
        ("pi", 1 << 50),
        ("k", 1E3 as u64),
        ("m", 1E6 as u64),
        ("g", 1E9 as u64),
        ("t", 1E12 as u64),
        ("p", 1E15 as u64),
    ];
    let arg = arg.trim().to_ascii_lowercase();
    ensure!(!arg.is_empty(), "empty string");

    if arg.ends_with('%') {
        let perc = arg[..arg.len() - 1].parse::<f64>()?;
        ensure!((0.0..=100.0).contains(&perc), "percentage out of range");
        return Ok(MemoryUsage::from_perc(perc));
    }

    let num_digits = arg
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .count();

    let number = arg[..num_digits].parse::<f64>()?;
    let suffix = &arg[num_digits..].trim();

    let prefix = suffix.strip_suffix('b').unwrap_or(suffix);
    let multiplier = PREF_SYMS
        .iter()
        .find(|(x, _)| *x == prefix)
        .map(|(_, m)| m)
        .ok_or(anyhow!("invalid prefix symbol {}", suffix))?;

    let value = (number * (*multiplier as f64)) as usize;
    ensure!(value > 0, "batch size must be greater than zero");

    if suffix.ends_with('b') {
        Ok(MemoryUsage::MemorySize(value))
    } else {
        Ok(MemoryUsage::BatchSize(value))
    }
}

#[derive(Args, Debug, Clone)]
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

    /// Whether to use Zuckerli's reference selection algorithm. This slows down the compression
    /// process and requires more memory, but improves compression ratio and decoding speed.
    #[clap(long)]
    pub bvgraphz: bool,

    /// How many nodes to process in a chunk; the default (10000) is usually a good
    /// value.
    #[clap(long, default_value = "10000")]
    pub chunk_size: usize,
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
                max_ref_count => {
                    assert!(
                        max_ref_count >= 0,
                        "max_ref_count cannot be negative, except for -1, which means infinite recursion depth, but got {}",
                        max_ref_count
                    );
                    value.max_ref_count as usize
                }
            },
        }
    }
}

/// Creates a [`ThreadPool`](rayon::ThreadPool) with the given number of threads.
pub fn get_thread_pool(num_threads: usize) -> rayon::ThreadPool {
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .expect("Failed to create thread pool");
    log::info!("Using {} threads", thread_pool.current_num_threads());
    thread_pool
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
    path_buf.set_file_name(filename);
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

/// Parses a duration from a string.
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

/// Initializes the `env_logger` logger with a custom format including
/// timestamps with elapsed time since initialization.
pub fn init_env_logger() -> Result<()> {
    use jiff::SpanRound;
    use jiff::fmt::friendly::{Designator, Spacing, SpanPrinter};

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
    /// How often to log progress. Default is 10s. You can use the suffixes "s"
    /// for seconds, "m" for minutes, "h" for hours, and "d" for days. If no
    /// suffix is provided it is assumed to be in milliseconds.
    /// Example: "1d2h3m4s567" is parsed as 1 day + 2 hours + 3 minutes + 4
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
#[doc = include_str!("common_env.txt")]
pub struct Cli {
    #[command(subcommand)]
    pub command: SubCommands,
    #[clap(flatten)]
    pub args: GlobalArgs,
}

pub mod dist;
pub mod rank;
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

/// Pretty-prints seconds in a human-readable format.
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

#[cfg(test)]
mod tests {
    use super::*;

    mod float_vector_format {
        use super::*;

        #[test]
        fn test_ascii_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0];
            FloatVectorFormat::Ascii
                .store(&path, &values, None)
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            // Default precision is f64::DIGITS (15)
            for (line, expected) in content.lines().zip(&values) {
                let parsed: f64 = line.trim().parse().unwrap();
                assert!((parsed - expected).abs() < 1e-10);
            }
            assert_eq!(content.lines().count(), 3);
        }

        #[test]
        fn test_ascii_f32() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let values: Vec<f32> = vec![1.5, 2.75, 3.0];
            FloatVectorFormat::Ascii
                .store(&path, &values, None)
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            for (line, expected) in content.lines().zip(&values) {
                let parsed: f32 = line.trim().parse().unwrap();
                assert!((parsed - expected).abs() < 1e-6);
            }
        }

        #[test]
        fn test_ascii_with_precision() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let values: Vec<f64> = vec![1.123456789, 2.987654321];
            FloatVectorFormat::Ascii
                .store(&path, &values, Some(3))
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            let lines: Vec<&str> = content.lines().collect();
            assert_eq!(lines[0], "1.123");
            assert_eq!(lines[1], "2.988");
        }

        #[test]
        fn test_json_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0];
            FloatVectorFormat::Json.store(&path, &values, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            let parsed: Vec<f64> = serde_json::from_str(&content).unwrap();
            assert_eq!(parsed, values);
        }

        #[test]
        fn test_json_with_precision() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let values: Vec<f64> = vec![1.123456789, 2.987654321];
            FloatVectorFormat::Json
                .store(&path, &values, Some(2))
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            assert_eq!(content, "[1.12, 2.99]");
        }

        #[test]
        fn test_json_empty() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let values: Vec<f64> = vec![];
            FloatVectorFormat::Json.store(&path, &values, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            assert_eq!(content, "[]");
        }

        #[test]
        fn test_json_single_element() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let values: Vec<f64> = vec![42.0];
            FloatVectorFormat::Json.store(&path, &values, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            let parsed: Vec<f64> = serde_json::from_str(&content).unwrap();
            assert_eq!(parsed, values);
        }

        #[test]
        fn test_java_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0];
            FloatVectorFormat::Java.store(&path, &values, None).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(bytes.len(), 3 * 8);
            for (i, expected) in values.iter().enumerate() {
                let chunk: [u8; 8] = bytes[i * 8..(i + 1) * 8].try_into().unwrap();
                let val = f64::from_be_bytes(chunk);
                assert_eq!(val, *expected);
            }
        }

        #[test]
        fn test_java_f32() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let values: Vec<f32> = vec![1.5, 2.75, 3.0];
            FloatVectorFormat::Java.store(&path, &values, None).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(bytes.len(), 3 * 4);
            for (i, expected) in values.iter().enumerate() {
                let chunk: [u8; 4] = bytes[i * 4..(i + 1) * 4].try_into().unwrap();
                let val = f32::from_be_bytes(chunk);
                assert_eq!(val, *expected);
            }
        }

        #[test]
        fn test_epserde_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0];
            FloatVectorFormat::Epserde
                .store(&path, &values, None)
                .unwrap();
            // Just verify the file was created and is non-empty
            let metadata = std::fs::metadata(&path).unwrap();
            assert!(metadata.len() > 0);
        }

        #[test]
        fn test_ascii_empty() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let values: Vec<f64> = vec![];
            FloatVectorFormat::Ascii
                .store(&path, &values, None)
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            assert!(content.is_empty());
        }

        #[test]
        fn test_creates_parent_dirs() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("a").join("b").join("test.txt");
            let values: Vec<f64> = vec![1.0];
            FloatVectorFormat::Ascii
                .store(&path, &values, None)
                .unwrap();
            assert!(path.exists());
        }

        #[test]
        fn test_roundtrip_ascii_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0, 0.0, -1.25];
            FloatVectorFormat::Ascii
                .store(&path, &values, None)
                .unwrap();
            let loaded: Vec<f64> = FloatVectorFormat::Ascii.load(&path).unwrap();
            assert_eq!(loaded, values);
        }

        #[test]
        fn test_roundtrip_json_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0, 0.0, -1.25];
            FloatVectorFormat::Json.store(&path, &values, None).unwrap();
            let loaded: Vec<f64> = FloatVectorFormat::Json.load(&path).unwrap();
            assert_eq!(loaded, values);
        }

        #[test]
        fn test_roundtrip_java_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0, 0.0, -1.25];
            FloatVectorFormat::Java.store(&path, &values, None).unwrap();
            let loaded: Vec<f64> = FloatVectorFormat::Java.load(&path).unwrap();
            assert_eq!(loaded, values);
        }

        #[test]
        fn test_roundtrip_epserde_f64() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let values: Vec<f64> = vec![1.5, 2.75, 3.0, 0.0, -1.25];
            FloatVectorFormat::Epserde
                .store(&path, &values, None)
                .unwrap();
            let loaded: Vec<f64> = FloatVectorFormat::Epserde.load(&path).unwrap();
            assert_eq!(loaded, values);
        }

        #[test]
        fn test_roundtrip_empty() {
            let dir = tempfile::tempdir().unwrap();
            for (fmt, ext) in [
                (FloatVectorFormat::Ascii, "txt"),
                (FloatVectorFormat::Json, "json"),
                (FloatVectorFormat::Java, "bin"),
                (FloatVectorFormat::Epserde, "eps"),
            ] {
                let path = dir.path().join(format!("empty.{ext}"));
                let values: Vec<f64> = vec![];
                fmt.store(&path, &values, None).unwrap();
                let loaded: Vec<f64> = fmt.load(&path).unwrap();
                assert_eq!(loaded, values, "roundtrip failed for {ext}");
            }
        }

        #[test]
        fn test_roundtrip_f32() {
            let dir = tempfile::tempdir().unwrap();
            let values: Vec<f32> = vec![1.5, 2.75, 3.0, 0.0, -1.25];
            for (fmt, ext) in [
                (FloatVectorFormat::Ascii, "txt"),
                (FloatVectorFormat::Json, "json"),
                (FloatVectorFormat::Java, "bin"),
                (FloatVectorFormat::Epserde, "eps"),
            ] {
                let path = dir.path().join(format!("f32.{ext}"));
                fmt.store(&path, &values, None).unwrap();
                let loaded: Vec<f32> = fmt.load(&path).unwrap();
                assert_eq!(loaded, values, "f32 roundtrip failed for {ext}");
            }
        }
    }

    mod int_vector_format {
        use super::*;

        #[test]
        fn test_ascii() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let data: Vec<u64> = vec![10, 20, 30];
            IntVectorFormat::Ascii.store(&path, &data, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            let lines: Vec<u64> = content.lines().map(|l| l.trim().parse().unwrap()).collect();
            assert_eq!(lines, data);
        }

        #[test]
        fn test_ascii_empty() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let data: Vec<u64> = vec![];
            IntVectorFormat::Ascii.store(&path, &data, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            assert!(content.is_empty());
        }

        #[test]
        fn test_json() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let data: Vec<u64> = vec![10, 20, 30];
            IntVectorFormat::Json.store(&path, &data, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            let parsed: Vec<u64> = serde_json::from_str(&content).unwrap();
            assert_eq!(parsed, data);
        }

        #[test]
        fn test_json_empty() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let data: Vec<u64> = vec![];
            IntVectorFormat::Json.store(&path, &data, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            assert_eq!(content, "[]");
        }

        #[test]
        fn test_json_single_element() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.json");
            let data: Vec<u64> = vec![42];
            IntVectorFormat::Json.store(&path, &data, None).unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            let parsed: Vec<u64> = serde_json::from_str(&content).unwrap();
            assert_eq!(parsed, data);
        }

        #[test]
        fn test_java() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let data: Vec<u64> = vec![1, 256, 65535];
            IntVectorFormat::Java.store(&path, &data, None).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(bytes.len(), 3 * 8);
            for (i, expected) in data.iter().enumerate() {
                let chunk: [u8; 8] = bytes[i * 8..(i + 1) * 8].try_into().unwrap();
                let val = u64::from_be_bytes(chunk);
                assert_eq!(val, *expected);
            }
        }

        #[test]
        fn test_java_empty() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let data: Vec<u64> = vec![];
            IntVectorFormat::Java.store(&path, &data, None).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            assert!(bytes.is_empty());
        }

        #[test]
        fn test_epserde() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let data: Vec<u64> = vec![10, 20, 30];
            IntVectorFormat::Epserde.store(&path, &data, None).unwrap();
            let metadata = std::fs::metadata(&path).unwrap();
            assert!(metadata.len() > 0);
        }

        #[test]
        fn test_bitfieldvec() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let data: Vec<u64> = vec![1, 3, 7, 15];
            IntVectorFormat::BitFieldVec
                .store(&path, &data, Some(15))
                .unwrap();
            let metadata = std::fs::metadata(&path).unwrap();
            assert!(metadata.len() > 0);
        }

        #[test]
        fn test_bitfieldvec_max_computed() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let data: Vec<u64> = vec![1, 3, 7, 15];
            // max is None, so it should be computed from data
            IntVectorFormat::BitFieldVec
                .store(&path, &data, None)
                .unwrap();
            assert!(path.exists());
        }

        #[test]
        fn test_creates_parent_dirs() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("a").join("b").join("test.txt");
            let data: Vec<u64> = vec![1];
            IntVectorFormat::Ascii.store(&path, &data, None).unwrap();
            assert!(path.exists());
        }

        #[cfg(target_pointer_width = "64")]
        #[test]
        fn test_store_usizes() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let data: Vec<usize> = vec![10, 20, 30];
            IntVectorFormat::Ascii
                .store_usizes(&path, &data, None)
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            let lines: Vec<usize> = content.lines().map(|l| l.trim().parse().unwrap()).collect();
            assert_eq!(lines, data);
        }

        #[cfg(target_pointer_width = "64")]
        #[test]
        fn test_store_usizes_java() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.bin");
            let data: Vec<usize> = vec![1, 256, 65535];
            IntVectorFormat::Java
                .store_usizes(&path, &data, None)
                .unwrap();
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(bytes.len(), 3 * 8);
            for (i, expected) in data.iter().enumerate() {
                let chunk: [u8; 8] = bytes[i * 8..(i + 1) * 8].try_into().unwrap();
                let val = u64::from_be_bytes(chunk) as usize;
                assert_eq!(val, *expected);
            }
        }
    }
}
