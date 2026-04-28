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
use dsi_bitstream::dispatch::Codes;
use epserde::deser::{Deserialize, Flags, MemCase};
use epserde::ser::Serialize;
use num_traits::{FromBytes, ToBytes};
use std::fmt::Display;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;
use sux::bits::BitFieldVec;
use sux::utils::PrimitiveUnsignedExt;
use value_traits::slices::SliceByValue;
use webgraph::prelude::CompFlags;
#[cfg(target_pointer_width = "64")]
use webgraph::prelude::JavaPermutation;
use webgraph::traits::{IntoParLenders, NodeLabelsLender};
use webgraph::utils::{Granularity, MemoryUsage};

macro_rules! SEQ_PROC_WARN {
    () => {"Processing the graph sequentially: for parallel processing please build the Elias–Fano offsets list using 'webgraph build ef {}'"}
}

#[cfg(not(any(feature = "le_bins", feature = "be_bins")))]
compile_error!("At least one of the features `le_bins` or `be_bins` must be enabled.");

/// Calls [`par_comp`] dispatching on a runtime endianness string.
///
/// The macro returns a [`Result`] with the output of the call if the endianness
/// is recognized, and an error otherwise.
///
/// # Arguments
///
/// * `config` - the [`BvCompConfig`] to call [`par_comp`] on;
///
/// * `graph` - an implementation of [`IntoParLenders`] with `Label = usize`;
///
/// * `endianness` - a string specifying the endianness type to use for the call;
///   it must implement `AsRef<str>`, and must be equal to the name of one of the
///   endianness types supported by the binary.
///
/// [`par_comp`]: webgraph::prelude::BvCompConfig::par_comp
/// [`BvCompConfig`]: webgraph::prelude::BvCompConfig
/// [`IntoParLenders`]: webgraph::prelude::IntoParLenders
#[macro_export]
macro_rules! par_comp {
    ($config:expr, $graph:expr, $endianness:expr) => {
        // Dispatch to a helper function so that each endianness gets its
        // own monomorphization without the `impl Trait` reference appearing
        // in multiple arms of a single `match` (which would cause the
        // borrow checker to require `'static`).
        $crate::__par_comp_dispatch(&mut $config, $graph, $endianness.as_str())
    };
}

/// Implementation detail of [`par_comp!`]. Dispatches to the correct
/// endianness-specific [`par_comp`].
///
/// [`par_comp`]: webgraph::prelude::BvCompConfig::par_comp
pub fn __par_comp_dispatch<
    PL: dsi_progress_logger::ProgressLog,
    G: for<'a> IntoParLenders<ParLender: NodeLabelsLender<'a, Label = usize>>,
>(
    config: &mut webgraph::prelude::BvCompConfig<PL>,
    graph: G,
    endianness: &str,
) -> anyhow::Result<u64> {
    use dsi_bitstream::prelude::Endianness;
    #[cfg(feature = "be_bins")]
    if endianness == dsi_bitstream::prelude::BE::NAME {
        return config.par_comp::<dsi_bitstream::prelude::BE, _>(graph);
    }
    #[cfg(feature = "le_bins")]
    if endianness == dsi_bitstream::prelude::LE::NAME {
        return config.par_comp::<dsi_bitstream::prelude::LE, _>(graph);
    }
    anyhow::bail!("Unknown endianness: {}", endianness)
}

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

/// Enum for instantaneous codes.
///
/// It is used to implement [`ValueEnum`] here instead of in [`dsi_bitstream`].
///
/// For CLI ergonomics and compatibility, these codes must be the same as those
/// appearing in [`CompFlags::code_from_str`].​
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum PrivCode {
    Unary,
    Gamma,
    Delta,
    Omega,
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
            PrivCode::Omega => Codes::Omega,
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

/// Shared CLI arguments for reading files containing arcs.​
#[derive(Args, Debug)]
pub struct ArcsArgs {
    #[arg(long, default_value_t = '#')]
    /// Ignore lines that start with this symbol.​
    pub line_comment_symbol: char,

    #[arg(long, default_value_t = 0)]
    /// Number of lines to skip, ignoring comment lines.​
    pub lines_to_skip: usize,

    #[arg(long)]
    /// Maximum number of lines to parse, after skipping and ignoring comment
    /// lines.​
    pub max_arcs: Option<usize>,

    #[arg(long, default_value_t = '\t')]
    /// The column separator.​
    pub separator: char,

    #[arg(long, default_value_t = 0)]
    /// The index of the column containing the source node of an arc.​
    pub source_column: usize,

    #[arg(long, default_value_t = 1)]
    /// The index of the column containing the target node of an arc.​
    pub target_column: usize,

    #[arg(long, default_value_t = false)]
    /// Treat source and target values as string labels rather than numeric
    /// node identifiers.​
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

/// Shared CLI arguments for commands that specify a number of threads.​
#[derive(Args, Debug)]
pub struct NumThreadsArg {
    #[arg(short = 't', long, default_value_t = rayon::current_num_threads().max(1), value_parser = num_threads_parser)]
    /// The number of threads to use.​
    pub num_threads: usize,
}

/// Shared CLI arguments for commands that specify a granularity.​
#[derive(Args, Debug)]
pub struct GranularityArgs {
    #[arg(long, conflicts_with("node_granularity"))]
    /// The tentative number of arcs used to define the size of a parallel job
    /// (advanced option).​
    pub arc_granularity: Option<u64>,

    #[arg(long, conflicts_with("arc_granularity"))]
    /// The tentative number of nodes used to define the size of a parallel job
    /// (advanced option).​
    pub node_granularity: Option<usize>,
}

impl GranularityArgs {
    pub fn into_granularity(&self) -> Granularity {
        self.into_granularity_or(Granularity::default())
    }

    pub fn into_granularity_or(&self, default: Granularity) -> Granularity {
        match (self.arc_granularity, self.node_granularity) {
            (Some(_), Some(_)) => unreachable!(),
            (Some(arc_granularity), None) => Granularity::Arcs(arc_granularity),
            (None, Some(node_granularity)) => Granularity::Nodes(node_granularity),
            (None, None) => default,
        }
    }
}

/// Shared CLI arguments for commands that specify a memory usage.
///
/// Accepts a plain number, a number with a suffix, a percentage, or the
/// special value `auto`. If the value ends with `b` or `B` it is interpreted
/// as a number of bytes; otherwise, it is interpreted as a number of
/// elements. The available SI and NIST multipliers are k, M, G, T, P, ki,
/// Mi, Gi, Ti, and Pi. A trailing `%` interprets the value as a percentage of
/// the available memory. The value `auto` uses a non-linear formula that
/// behaves like 50% of RAM on small machines but grows sub-linearly on large
/// ones, capped at 1 TiB (256 MiB on 32-bit platforms); see
/// [`MemoryUsage::default`] for details. The default is `auto`.​
#[derive(Args, Debug)]
pub struct MemoryUsageArg {
    #[clap(short = 'm', long = "memory-usage", value_parser = memory_usage_parser, default_value = "auto")]
    /// The memory usage for batches (a number of elements with an optional
    /// SI/NIST suffix; append "b"/"B" for bytes, "%" for a percentage of
    /// available memory, or "auto" for a non-linear default).​
    pub memory_usage: MemoryUsage,
}

/// Formats for storing and loading slices of floats.​
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum FloatSliceFormat {
    /// Java-compatible format: a sequence of big-endian floats (32 or 64 bits).​
    Java,
    /// A sequence of floats (32 or 64 bits) serialized using ε-serde.​
    Epserde,
    /// ASCII format, one float per line.​
    #[default]
    Ascii,
    /// A JSON array.​
    Json,
}

impl FloatSliceFormat {
    /// Stores a slice of floats in the specified `path` using the format defined by
    /// `self`.
    ///
    /// If the result is a textual format, that is, ASCII or JSON, `precision`
    /// will be used to round the float values to the specified number of
    /// decimal digits. If `None`, [zmij] formatting will be used.
    ///
    /// [zmij]: https://crates.io/crates/zmij
    pub fn store<F>(
        &self,
        path: impl AsRef<Path>,
        values: &[F],
        precision: Option<usize>,
    ) -> Result<()>
    where
        F: ToBytes + Display + epserde::ser::Serialize + Copy + zmij::Float,
        for<'a> &'a [F]: epserde::ser::Serialize,
    {
        create_parent_dir(&path)?;
        let path_display = path.as_ref().display();
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create slice at {}", path_display))?;
        let mut file = BufWriter::new(file);

        match self {
            FloatSliceFormat::Epserde => {
                log::info!("Storing in ε-serde format at {}", path_display);

                unsafe {
                    values
                        .serialize(&mut file)
                        .with_context(|| format!("Could not write slice to {}", path_display))
                }?;
            }
            FloatSliceFormat::Java => {
                log::info!("Storing in Java format at {}", path_display);
                for word in values.iter() {
                    file.write_all(word.to_be_bytes().as_ref())
                        .with_context(|| format!("Could not write slice to {}", path_display))?;
                }
            }
            FloatSliceFormat::Ascii => {
                log::info!("Storing in ASCII format at {}", path_display);
                let mut buf = zmij::Buffer::new();
                for word in values.iter() {
                    match precision {
                        None => writeln!(file, "{}", buf.format(*word)),
                        Some(precision) => writeln!(file, "{word:.precision$}"),
                    }
                    .with_context(|| format!("Could not write slice to {}", path_display))?;
                }
            }
            FloatSliceFormat::Json => {
                log::info!("Storing in JSON format at {}", path_display);
                let mut buf = zmij::Buffer::new();
                write!(file, "[")?;
                for word in values.iter().take(values.len().saturating_sub(1)) {
                    match precision {
                        None => write!(file, "{}, ", buf.format(*word)),
                        Some(precision) => write!(file, "{word:.precision$}, "),
                    }
                    .with_context(|| format!("Could not write slice to {}", path_display))?;
                }
                if let Some(last) = values.last() {
                    match precision {
                        None => write!(file, "{}", buf.format(*last)),
                        Some(precision) => write!(file, "{last:.precision$}"),
                    }
                    .with_context(|| format!("Could not write slice to {}", path_display))?;
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
        F: FromBytes + FromStr + Copy + serde::de::DeserializeOwned,
        <F as FromBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
        <F as FromStr>::Err: std::error::Error + Send + Sync + 'static,
        Vec<F>: epserde::deser::Deserialize,
    {
        let path = path.as_ref();
        let path_display = path.display();

        match self {
            FloatSliceFormat::Epserde => {
                log::info!("Loading ε-serde format from {}", path_display);
                Ok(unsafe {
                    <Vec<F>>::load_full(path)
                        .with_context(|| format!("Could not load slice from {}", path_display))?
                })
            }
            FloatSliceFormat::Java => {
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
                    result.push(F::from_be_bytes(&bytes));
                }
                Ok(result)
            }
            FloatSliceFormat::Ascii => {
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
            FloatSliceFormat::Json => {
                log::info!("Loading JSON format from {}", path_display);
                let file = std::fs::File::open(path)
                    .with_context(|| format!("Could not open {}", path_display))?;
                let reader = BufReader::new(file);
                serde_json::from_reader(reader)
                    .with_context(|| format!("Could not parse JSON from {}", path_display))
            }
        }
    }
}

/// How to store slices of integers.​
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum IntSliceFormat {
    #[cfg(target_pointer_width = "64")]
    /// Java-compatible format: a sequence of big-endian 64-bit integers; available only on 64-bit platforms.​
    Java,
    /// A sequence of usize serialized using ε-serde.​
    Epserde,
    /// A BitFieldVec stored using ε-serde: it stores each element using
    /// ⌊log₂(max)⌋ + 1 bits, and it requires to allocate the `BitFieldVec` in RAM
    /// before serializing it.​
    BitFieldVec,
    /// ASCII format, one integer per line.​
    #[default]
    Ascii,
    /// A JSON array.​
    Json,
}

/// Loaded integer slice, returned by [`IntSliceFormat::load`].
///
/// Depending on the format, the data may be backed by a memory-mapped file
/// (Java, ε-serde, and [`BitFieldVec`]) or fully loaded into memory (ASCII,
/// JSON).
///
/// This enum implements [`SliceByValue`] with `Value = usize`, so it can
/// be used directly wherever a [`SliceByValue`] is expected. However, each
/// access goes through enum dispatch, which may be undesirable in
/// performance-critical code.
///
/// For native, dispatch-free access, match on the variants and use each
/// inner type directly—they all implement [`SliceByValue<Value = usize>`]:
///
/// ```ignore
/// match args.fmt.load(&path)? {
///     IntSlice::Owned(v) => do_transform(&v, ...),
///     IntSlice::Java(j) => do_transform(&j, ...),
///     IntSlice::Epserde(m) => do_transform(m.uncase(), ...),
///     IntSlice::BitFieldVec(m) => do_transform(m.uncase(), ...),
/// }
/// ```
///
/// This incurs one monomorphization of `do_transform` per arm; arms that
/// share the same inner type can be merged to reduce monomorphization cost.
pub enum IntSlice {
    /// Fully loaded into memory (ASCII, JSON).
    Owned(Box<[usize]>),
    #[cfg(target_pointer_width = "64")]
    /// Memory-mapped Java big-endian format (64-bit only).
    Java(JavaPermutation),
    /// Memory-mapped ε-serde serialized slice.
    Epserde(MemCase<Box<[usize]>>),
    /// Memory-mapped ε-serde serialized [`BitFieldVec`].
    BitFieldVec(MemCase<sux::bits::BitFieldVec>),
}

impl SliceByValue for IntSlice {
    type Value = usize;

    unsafe fn get_value_unchecked(&self, index: usize) -> usize {
        match self {
            IntSlice::Owned(v) => unsafe { *v.get_unchecked(index) },
            #[cfg(target_pointer_width = "64")]
            IntSlice::Java(j) => unsafe { j.get_value_unchecked(index) },
            IntSlice::Epserde(m) => unsafe { *m.uncase().get_unchecked(index) },
            IntSlice::BitFieldVec(m) => unsafe { m.uncase().get_value_unchecked(index) },
        }
    }

    fn len(&self) -> usize {
        match self {
            IntSlice::Owned(v) => v.len(),
            #[cfg(target_pointer_width = "64")]
            IntSlice::Java(j) => j.len(),
            IntSlice::Epserde(m) => m.uncase().len(),
            IntSlice::BitFieldVec(m) => m.uncase().len(),
        }
    }
}

/// Dispatches on an [`IntSlice`], binding the concrete inner
/// [`SliceByValue`] type to `$var` and evaluating `$body` for each variant.
///
/// The bound types are `&Box<[usize]>` (Owned and Epserde),
/// `&JavaPermutation` (Java, 64-bit only), and `&BitFieldVec`
/// (BitFieldVec). All bound types are `Sized`.
#[macro_export]
macro_rules! dispatch_int_slice {
    ($slice:expr, |$var:ident| $body:expr) => {
        match $slice {
            $crate::IntSlice::Owned(ref __v) => {
                let $var = __v;
                $body
            }
            #[cfg(target_pointer_width = "64")]
            $crate::IntSlice::Java(ref __j) => {
                let $var = __j;
                $body
            }
            $crate::IntSlice::Epserde(ref __m) => {
                let $var = __m.uncase();
                $body
            }
            $crate::IntSlice::BitFieldVec(ref __m) => {
                let $var = __m.uncase();
                $body
            }
        }
    };
}

impl IntSliceFormat {
    /// Stores a slice of `usize` in the specified `path` using the format
    /// defined by `self`.
    ///
    /// `max` is the maximum value of the slice. If it is not provided, it will
    /// be computed from the data.
    pub fn store(&self, path: impl AsRef<Path>, data: &[usize], max: Option<usize>) -> Result<()> {
        // Ensure the parent directory exists
        create_parent_dir(&path)?;

        let mut file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create slice at {}", path.as_ref().display()))?;
        let mut buf = BufWriter::new(&mut file);

        debug_assert_eq!(
            max,
            max.map(|_| { data.iter().copied().max().unwrap_or(0) }),
            "The wrong maximum value was provided for the slice"
        );

        match self {
            IntSliceFormat::Epserde => {
                log::info!("Storing in ε-serde format at {}", path.as_ref().display());

                unsafe {
                    data.serialize(&mut buf).with_context(|| {
                        format!("Could not write slice to {}", path.as_ref().display())
                    })
                }?;
            }
            IntSliceFormat::BitFieldVec => {
                log::info!(
                    "Storing in BitFieldVec format at {}",
                    path.as_ref().display()
                );
                let max = max.unwrap_or_else(|| {
                    data.iter()
                        .copied()
                        .max()
                        .unwrap_or_else(|| panic!("Empty slice"))
                });
                let bit_width = max.bit_len() as usize;
                log::info!("Using {} bits per element", bit_width);
                let mut bit_field_vec = BitFieldVec::with_capacity(bit_width, data.len());
                bit_field_vec.extend(data.iter().copied());

                unsafe {
                    bit_field_vec.store(&path).with_context(|| {
                        format!("Could not write slice to {}", path.as_ref().display())
                    })
                }?;
            }
            #[cfg(target_pointer_width = "64")]
            IntSliceFormat::Java => {
                log::info!("Storing in Java format at {}", path.as_ref().display());
                for word in data.iter() {
                    buf.write_all(&word.to_be_bytes()).with_context(|| {
                        format!("Could not write slice to {}", path.as_ref().display())
                    })?;
                }
            }
            IntSliceFormat::Ascii => {
                log::info!("Storing in ASCII format at {}", path.as_ref().display());
                for word in data.iter() {
                    writeln!(buf, "{}", word).with_context(|| {
                        format!("Could not write slice to {}", path.as_ref().display())
                    })?;
                }
            }
            IntSliceFormat::Json => {
                log::info!("Storing in JSON format at {}", path.as_ref().display());
                serde_json::to_writer(&mut buf, data).with_context(|| {
                    format!("Could not write slice to {}", path.as_ref().display())
                })?;
            }
        };

        Ok(())
    }

    /// Loads integer values from the specified `path` using the format defined
    /// by `self`, returning an [`IntSlice`].
    ///
    /// The ε-serde-based formats (Epserde, BitFieldVec) and the Java format
    /// use memory mapping; ASCII and JSON are fully loaded into memory.
    pub fn load(&self, path: impl AsRef<Path>) -> Result<IntSlice> {
        let path = path.as_ref();
        let path_display = path.display();

        match self {
            IntSliceFormat::Epserde => {
                log::info!("Loading ε-serde format from {}", path_display);
                let mem_case = unsafe {
                    <Box<[usize]>>::mmap(path, Flags::RANDOM_ACCESS)
                        .with_context(|| format!("Could not load slice from {}", path_display))?
                };
                Ok(IntSlice::Epserde(mem_case))
            }
            IntSliceFormat::BitFieldVec => {
                log::info!("Loading BitFieldVec format from {}", path_display);
                let mem_case = unsafe {
                    <BitFieldVec>::mmap(path, Flags::RANDOM_ACCESS)
                        .with_context(|| format!("Could not load slice from {}", path_display))?
                };
                Ok(IntSlice::BitFieldVec(mem_case))
            }
            #[cfg(target_pointer_width = "64")]
            IntSliceFormat::Java => {
                log::info!("Loading Java format from {}", path_display);
                let perm = JavaPermutation::mmap(path, mmap_rs::MmapFlags::RANDOM_ACCESS)
                    .with_context(|| format!("Could not load slice from {}", path_display))?;
                Ok(IntSlice::Java(perm))
            }
            IntSliceFormat::Ascii => {
                log::info!("Loading ASCII format from {}", path_display);
                let file = std::fs::File::open(path)
                    .with_context(|| format!("Could not open {}", path_display))?;
                let reader = BufReader::new(file);
                let v: Vec<usize> = reader
                    .lines()
                    .enumerate()
                    .filter(|(_, line)| line.as_ref().map_or(true, |l| !l.trim().is_empty()))
                    .map(|(i, line)| {
                        let line = line.with_context(|| {
                            format!("Error reading line {} of {}", i + 1, path_display)
                        })?;
                        line.trim().parse::<usize>().map_err(|e| {
                            anyhow!("Error parsing line {} of {}: {}", i + 1, path_display, e)
                        })
                    })
                    .collect::<Result<_>>()?;
                Ok(IntSlice::Owned(v.into_boxed_slice()))
            }
            IntSliceFormat::Json => {
                log::info!("Loading JSON format from {}", path_display);
                let file = std::fs::File::open(path)
                    .with_context(|| format!("Could not open {}", path_display))?;
                let reader = BufReader::new(file);
                let v: Vec<usize> = serde_json::from_reader(reader)
                    .with_context(|| format!("Could not parse JSON from {}", path_display))?;
                Ok(IntSlice::Owned(v.into_boxed_slice()))
            }
        }
    }
}

/// Parses a batch size.
///
/// This function accepts `auto` (which uses the non-linear default from
/// [`MemoryUsage::default`]), a number (possibly followed by a SI or NIST
/// multiplier k, M, G, T, P, ki, Mi, Gi, Ti, or Pi), or a percentage
/// (followed by a `%`) that is interpreted as a percentage of the physical
/// RAM. If the value ends with a `b` or `B` it is interpreted as a number of
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

    if arg == "auto" {
        return Ok(MemoryUsage::default());
    }

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

    if suffix.is_empty() {
        let value = number as usize;
        ensure!(value > 0, "batch size must be greater than zero");
        return Ok(MemoryUsage::BatchSize(value));
    }

    let prefix = suffix.strip_suffix('b').unwrap_or(suffix);
    let multiplier = PREF_SYMS
        .iter()
        .find(|(x, _)| *x == prefix)
        .map(|(_, m)| m)
        .ok_or(anyhow!("invalid suffix {}", suffix))?;

    let value = (number * (*multiplier as f64)) as usize;
    ensure!(value > 0, "batch size must be greater than zero");

    if suffix.ends_with('b') {
        Ok(MemoryUsage::MemorySize(value))
    } else {
        Ok(MemoryUsage::BatchSize(value))
    }
}

/// Shared CLI arguments for compression.​
#[derive(Args, Debug, Clone)]
pub struct CompressArgs {
    /// The endianness of the graph to write [default: same as source].​
    #[clap(short = 'E', long)]
    pub endianness: Option<String>,

    /// The compression window.​
    #[clap(short = 'w', long, default_value_t = 7)]
    pub compression_window: usize,
    /// The minimum interval length.​
    #[clap(short = 'i', long, default_value_t = 4)]
    pub min_interval_length: usize,
    /// The maximum recursion depth for references (-1 for infinite recursion depth).​
    #[clap(short = 'r', long, default_value_t = 3)]
    pub max_ref_count: isize,

    #[arg(value_enum)]
    #[clap(long, default_value = "gamma")]
    /// The code to use for outdegrees.​
    pub outdegrees: PrivCode,

    #[arg(value_enum)]
    #[clap(long, default_value = "unary")]
    /// The code to use for reference offsets.​
    pub references: PrivCode,

    #[arg(value_enum)]
    #[clap(long, default_value = "gamma")]
    /// The code to use for blocks.​
    pub blocks: PrivCode,

    #[arg(value_enum)]
    #[clap(long, default_value = "zeta3")]
    /// The code to use for residuals.​
    pub residuals: PrivCode,

    /// Use Zuckerli's reference selection algorithm (slower, more memory,
    /// but better compression and decoding speed).​
    #[clap(long)]
    pub bvgraphz: bool,

    /// Number of nodes per chunk with --bvgraphz.​
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

/// Creates a [`ThreadPool`] with the given number of threads.
///
/// [`ThreadPool`]: rayon::ThreadPool
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

/// Computes cutpoints for splitting a graph into chunks for parallel
/// compression.
///
/// If `use_dcf` is true, loads the DCF file for the given basename and uses
/// `FairChunks` to balance by arc count. Otherwise, falls back to uniform
/// cutpoints by node count.
pub fn cutpoints(
    basename: &Path,
    num_nodes: usize,
    num_arcs: Option<u64>,
    use_dcf: bool,
) -> Result<Vec<usize>> {
    use epserde::prelude::*;
    use sux::utils::FairChunks;
    use value_traits::slices::SliceByValue;
    use webgraph::prelude::{DCF, DEG_CUMUL_EXTENSION};

    if use_dcf {
        let dcf_path = basename.with_extension(DEG_CUMUL_EXTENSION);
        ensure!(
            dcf_path.exists(),
            "DCF file {} does not exist; build it with `webgraph build dcf`",
            dcf_path.display()
        );
        let dcf = unsafe { DCF::mmap(&dcf_path, Flags::RANDOM_ACCESS) }?;
        let dcf = dcf.uncase();
        ensure!(
            dcf.len() == num_nodes + 1,
            "DCF has {} entries, expected {} (num_nodes + 1)",
            dcf.len(),
            num_nodes + 1
        );
        ensure!(dcf.index_value(0) == 0, "DCF does not start with 0");
        let num_arcs: u64 = num_arcs.expect("num_arcs_hint required for --dcf");
        ensure!(
            dcf.index_value(num_nodes) == num_arcs,
            "DCF ends with {}, expected {} (num_arcs)",
            dcf.index_value(num_nodes),
            num_arcs
        );
        let num_threads = rayon::current_num_threads();
        let target_weight = num_arcs.div_ceil(num_threads as u64);
        let cutpoints: Vec<usize> = std::iter::once(0)
            .chain(FairChunks::new(target_weight, &dcf).map(|r| r.end))
            .collect();
        log::info!(
            "Using DCF-based splitting into {} parts",
            cutpoints.len() - 1
        );
        Ok(cutpoints)
    } else {
        let dcf_path = basename.with_extension(DEG_CUMUL_EXTENSION);
        if dcf_path.exists() {
            log::warn!(
                "A DCF (degree cumulative function) file exists at {}; consider using --dcf for better load balancing",
                dcf_path.display()
            );
        } else {
            log::warn!(
                "No DCF (degree cumulative function) file found; consider building one with `webgraph build dcf {}` for better load balancing",
                basename.display()
            );
        }
        let n = rayon::current_num_threads();
        let step = num_nodes.div_ceil(n);
        Ok((0..n + 1).map(move |i| (i * step).min(num_nodes)).collect())
    }
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

/// Shared CLI arguments for commands that use a log interval.​
#[derive(Args, Debug, Clone)]
pub struct LogIntervalArg {
    #[arg(long, value_parser = parse_duration, default_value = "10s")]
    /// How often to log progress (default: 10s). Supported suffixes: "s"
    /// (seconds), "m" (minutes), "h" (hours), "d" (days). Without a suffix,
    /// the value is interpreted as milliseconds. Example: "1d2h3m4s567" means
    /// 1 day + 2 hours + 3 minutes + 4 seconds + 567 milliseconds.​
    pub log_interval: Duration,
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
    Seq(seq::SubCommands),
    #[command(subcommand)]
    To(to::SubCommands),
    #[command(subcommand)]
    Transform(transform::SubCommands),
}

#[derive(Parser, Debug)]
#[command(name = "webgraph", version=build_info::version_string(), max_term_width = 100)]
/// WebGraph tools to build, convert, modify, and analyze graphs.​
#[doc = include_str!("common_env.txt")]
pub struct Cli {
    #[command(subcommand)]
    pub command: SubCommands,
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
pub mod seq;
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
            analyze::main(args)?;
        }
        SubCommands::Bench(args) => {
            bench::main(args)?;
        }
        SubCommands::Build(args) => {
            build::main(args, Cli::command())?;
        }
        SubCommands::Check(args) => {
            check::main(args)?;
        }
        SubCommands::From(args) => {
            from::main(args)?;
        }
        SubCommands::Perm(args) => {
            perm::main(args)?;
        }
        SubCommands::Run(args) => {
            run::main(args)?;
        }
        SubCommands::Seq(args) => {
            seq::main(args)?;
        }
        SubCommands::To(args) => {
            to::main(args)?;
        }
        SubCommands::Transform(args) => {
            transform::main(args)?;
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
