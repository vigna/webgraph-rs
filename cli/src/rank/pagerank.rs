/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{FloatVectorFormat, GlobalArgs, GranularityArgs, NumThreadsArg, get_thread_pool};
use anyhow::{Context, Result, ensure};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{ProgressLog, concurrent_progress_logger, progress_logger};
use predicates::prelude::*;
use std::io::BufRead;
use std::path::PathBuf;
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::prelude::{BvGraph, SequentialLabeling};
use webgraph_algo::rank::PageRank;
use webgraph_algo::rank::pagerank::preds::{L1Norm, MaxIter};

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
    /// Path to a preference (personalization) vector (one f64 per line).
    pub preference: Option<PathBuf>,

    #[arg(short = 'w', long, default_value_t = false)]
    /// Weakly preferential PageRank: use a uniform dangling-node distribution
    /// even when a custom preference vector is provided. By default, the
    /// preference vector is used as dangling-node distribution (strongly
    /// preferential). Mutually exclusive with --dangling-distribution.
    pub weakly_preferential: bool,

    #[arg(long)]
    /// Path to a dangling-node distribution vector (one f64 per line).
    /// Mutually exclusive with --weakly-preferential.
    pub dangling_distribution: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    /// Zero out the dangling-node contribution (pseudo-rank).
    pub pseudorank: bool,

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
        args.alpha >= 0.0 && args.alpha < 1.0,
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

    ensure!(
        !(args.weakly_preferential && args.dangling_distribution.is_some()),
        "--weakly-preferential and --dangling-distribution are mutually exclusive"
    );

    let preference = args.preference.as_ref().map(load_f64_vector).transpose()?;
    let dangling_distribution = args
        .dangling_distribution
        .as_ref()
        .map(load_f64_vector)
        .transpose()?;

    let n = transpose.num_nodes();
    let uniform_dangling;
    let effective_dangling = if args.weakly_preferential && preference.is_some() {
        // In weakly preferential mode with a custom preference, use a uniform
        // dangling distribution to override the default (strongly preferential)
        // behaviour. This vector is a waste of space, but the only way to avoid
        // the waste is to move it to a parameter implementing SliceByValue or a
        // closure, which would further require a typestate pattern in the
        // setters.
        uniform_dangling = vec![1.0 / n as f64; n];
        Some(uniform_dangling.as_slice())
    } else {
        dangling_distribution.as_deref()
    };

    // Build stopping predicate
    let mut predicate = L1Norm::try_from(args.threshold)?.boxed();
    if let Some(max_iter) = args.max_iter {
        predicate = predicate.or(MaxIter::from(max_iter)).boxed();
    }

    // Configure PageRank
    let mut pr = PageRank::new(&transpose);
    pr.alpha(args.alpha)
        .pseudo_rank(args.pseudorank)
        .granularity(args.granularity.into_granularity())
        .preference(preference.as_deref())
        .dangling_distribution(effective_dangling);

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

/// Reads a text file containing one `f64` per line.
fn load_f64_vector(path: impl AsRef<std::path::Path>) -> Result<Vec<f64>> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .with_context(|| format!("Could not open vector file {}", path.display()))?;
    let reader = std::io::BufReader::new(file);
    reader
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let line = line
                .with_context(|| format!("Error reading line {} of {}", i + 1, path.display()))?;
            line.trim().parse::<f64>().with_context(|| {
                format!(
                    "Error parsing line {} of {}: {:?}",
                    i + 1,
                    path.display(),
                    line
                )
            })
        })
        .collect()
}
