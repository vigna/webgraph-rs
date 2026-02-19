/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{FloatVectorFormat, GlobalArgs, GranularityArgs, NumThreadsArg, get_thread_pool};
use anyhow::{Result, ensure};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{ProgressLog, concurrent_progress_logger, progress_logger};
use predicates::prelude::*;
use std::path::PathBuf;
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::prelude::BvGraph;
use webgraph_algo::rank::pagerank::preds::{L1Norm, MaxIter};
use webgraph_algo::rank::{Mode, PageRank};

/// The PageRank mode.
#[derive(clap::ValueEnum, Debug, Clone, Copy, Default)]
pub enum CliMode {
    /// Use the preference vector as dangling-node distribution.
    #[default]
    StronglyPreferential,
    /// Use a uniform dangling-node distribution regardless of the preference
    /// vector.
    WeaklyPreferential,
    /// Zero out the dangling-node contribution (pseudorank).
    PseudoRank,
}

impl From<CliMode> for Mode {
    fn from(m: CliMode) -> Self {
        match m {
            CliMode::StronglyPreferential => Mode::StronglyPreferential,
            CliMode::WeaklyPreferential => Mode::WeaklyPreferential,
            CliMode::PseudoRank => Mode::PseudoRank,
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "pagerank",
    about = "Compute PageRank using parallel Gauss–Seidel iteration.",
    long_about = None
)]
pub struct CliArgs {
    /// The basename of the transpose of the graph.
    pub transpose: PathBuf,

    #[arg(short, long)]
    /// Where to store the rank vector.
    pub output: PathBuf,

    #[arg(short, long, default_value_t = 0.85)]
    /// The damping factor α (must be in the interval [0 . . 1).
    pub alpha: f64,

    #[arg(long)]
    /// Maximum number of iterations.
    pub max_iter: Option<usize>,

    #[arg(short, long, default_value_t = 1e-6)]
    /// The ℓ₁ error threshold to stop.
    pub threshold: f64,

    #[arg(short, long)]
    /// Path to a preference (personalization) vector.
    pub preference: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = FloatVectorFormat::Ascii)]
    /// The input format for the preference vector.
    pub preference_fmt: FloatVectorFormat,

    #[arg(short, long, value_enum, default_value_t = CliMode::StronglyPreferential)]
    /// The PageRank mode.
    pub mode: CliMode,

    #[arg(long, value_enum, default_value_t = FloatVectorFormat::Ascii)]
    /// The output format for the rank vector.
    pub fmt: FloatVectorFormat,

    #[arg(long)]
    /// Decimal digits for text output formats.
    pub precision: Option<usize>,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    ensure!(
        // Note that 0.0..1.0 is [0.0..1.0) in mathematical notation
        (0.0..1.0).contains(&args.alpha),
        "The damping factor must be in [0 . . 1), got {}",
        args.alpha
    );

    match get_endianness(&args.transpose)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => pagerank::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => pagerank::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn pagerank<E: Endianness>(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    let mut pl = progress_logger![];
    pl.display_memory(true);
    if let Some(log_interval) = global_args.log_interval {
        pl.log_interval(log_interval);
    }

    let mut cpl = concurrent_progress_logger![];
    cpl.display_memory(true);
    if let Some(log_interval) = global_args.log_interval {
        cpl.log_interval(log_interval);
    }

    let thread_pool = get_thread_pool(args.num_threads.num_threads);

    log::info!(
        "Loading the transpose graph from {}",
        args.transpose.display()
    );
    let transpose = BvGraph::with_basename(&args.transpose).load()?;

    let preference: Option<Vec<f64>> = args
        .preference
        .as_ref()
        .map(|path| args.preference_fmt.load(path))
        .transpose()?;

    // Build stopping predicate
    let mut predicate = L1Norm::try_from(args.threshold)?.boxed();
    if let Some(max_iter) = args.max_iter {
        predicate = predicate.or(MaxIter::from(max_iter)).boxed();
    }

    // Configure PageRank
    let mut pr = PageRank::new(&transpose);
    pr.alpha(args.alpha)
        .mode(args.mode.into())
        .granularity(args.granularity.into_granularity())
        .preference(preference.as_deref());

    // Run
    thread_pool.install(|| pr.run_with_logging(predicate, &mut pl, &mut cpl));

    log::info!(
        "Completed after {} iteration(s), norm delta = {}",
        pr.iterations(),
        pr.norm_delta()
    );

    // Store results
    args.fmt.store(&args.output, pr.rank(), args.precision)?;

    Ok(())
}
