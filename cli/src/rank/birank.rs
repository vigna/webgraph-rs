/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{FloatSliceFormat, GranularityArgs, LogIntervalArg, NumThreadsArg, get_thread_pool};
use anyhow::{Result, ensure};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{concurrent_progress_logger, progress_logger};
use predicates::prelude::*;
use std::path::PathBuf;
use value_traits::slices::SliceByValue;
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::prelude::BvGraph;
use webgraph::traits::RandomAccessGraph;
use webgraph_algo::rank::BiRank;
use webgraph_algo::rank::birank::PredParams;
use webgraph_algo::rank::preds::{L1Norm, LInfNorm, MaxIter};

#[derive(Parser, Debug)]
#[command(
    name = "birank",
    about = "Computes BiRank on a bipartite graph using parallel power iteration.",
    long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph (arcs from sources to targets).​
    pub graph: PathBuf,

    /// The basename of the transpose of the graph.​
    pub transpose: PathBuf,

    /// The number of source nodes (|U|); source nodes are [0 . . num_sources),
    /// target nodes are [num_sources . . n).​
    pub num_sources: usize,

    #[arg(short, long)]
    /// Where to store the rank vector.​
    pub output: PathBuf,

    #[arg(short, long, default_value_t = 0.85)]
    /// The damping factor α for target (P) nodes (must be in [0 . . 1]).​
    pub alpha: f64,

    #[arg(short, long, default_value_t = 0.85)]
    /// The damping factor β for source (U) nodes (must be in [0 . . 1]).​
    pub beta: f64,

    #[arg(long)]
    /// Maximum number of iterations.​
    pub max_iter: Option<usize>,

    #[arg(short, long)]
    /// The ℓ₁ norm threshold to stop.​
    pub l1_threshold: Option<f64>,

    #[arg(long)]
    /// The ℓ_∞ norm threshold to stop.​
    pub linf_threshold: Option<f64>,

    #[arg(short, long)]
    /// Path to a preference (query) vector.​
    pub preference: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = FloatSliceFormat::Ascii)]
    /// The input format for the preference vector.​
    pub preference_fmt: FloatSliceFormat,

    #[arg(long, value_enum, default_value_t = FloatSliceFormat::Ascii)]
    /// The output format for the rank vector.​
    pub fmt: FloatSliceFormat,

    #[arg(long)]
    /// Decimal digits for text output formats.​
    pub precision: Option<usize>,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    ensure!(
        (0.0..=1.0).contains(&args.alpha),
        "Alpha must be in [0\u{2009}.\u{2009}.\u{2009}1], got {}",
        args.alpha
    );
    ensure!(
        (0.0..=1.0).contains(&args.beta),
        "Beta must be in [0\u{2009}.\u{2009}.\u{2009}1], got {}",
        args.beta
    );

    let graph_endianness = get_endianness(&args.graph)?.clone();
    let transpose_endianness = get_endianness(&args.transpose)?;
    ensure!(
        graph_endianness == transpose_endianness,
        "Graph and transpose have different endianness: {} vs {}",
        graph_endianness,
        transpose_endianness
    );

    match graph_endianness.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => birank::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => birank::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn run_and_store<
    G: RandomAccessGraph + Sync + Send,
    H: RandomAccessGraph + Sync + Send,
    V: SliceByValue<Value = f64> + Sync + Send,
>(
    br: &mut BiRank<G, H, V>,
    predicate: impl predicates::Predicate<PredParams> + Send,
    pl: &mut (impl dsi_progress_logger::ProgressLog + Send),
    cpl: &mut impl dsi_progress_logger::ConcurrentProgressLog,
    thread_pool: &rayon::ThreadPool,
    args: &CliArgs,
) -> Result<()> {
    thread_pool.install(|| br.run_with_logging(predicate, pl, cpl));

    log::info!(
        "Completed after {} iteration(s), L1 norm delta = {}, Linf norm delta = {}",
        br.iterations(),
        br.l1_norm_delta(),
        br.linf_norm_delta()
    );

    args.fmt.store(&args.output, br.rank(), args.precision)?;
    Ok(())
}

pub fn birank<E: Endianness>(args: CliArgs) -> Result<()> {
    let mut pl = progress_logger![display_memory = true, log_interval = args.log_interval.log_interval];

    let mut cpl = concurrent_progress_logger![display_memory = true, log_interval = args.log_interval.log_interval];

    let thread_pool = get_thread_pool(args.num_threads.num_threads);

    log::info!("Loading graph from {}", args.graph.display());
    let graph = BvGraph::with_basename(&args.graph).load()?;

    log::info!("Loading transpose graph from {}", args.transpose.display());
    let transpose = BvGraph::with_basename(&args.transpose).load()?;

    let preference: Option<Vec<f64>> = args
        .preference
        .as_ref()
        .map(|path| args.preference_fmt.load(path))
        .transpose()?;

    // Build stopping predicate
    ensure!(
        args.l1_threshold.is_some() || args.linf_threshold.is_some() || args.max_iter.is_some(),
        "At least one stopping criterion must be specified \
         (--threshold, --linf-threshold, or --max-iter)"
    );
    let mut predicate: predicates::BoxPredicate<PredParams> = predicates::constant::never().boxed();
    if let Some(threshold) = args.l1_threshold {
        predicate = predicate.or(L1Norm::try_from(threshold)?).boxed();
    }
    if let Some(linf_threshold) = args.linf_threshold {
        predicate = predicate.or(LInfNorm::try_from(linf_threshold)?).boxed();
    }
    if let Some(max_iter) = args.max_iter {
        predicate = predicate.or(MaxIter::from(max_iter)).boxed();
    }

    // Configure and run BiRank
    if let Some(pref) = &preference {
        let mut br = BiRank::new(&graph, &transpose, args.num_sources).preference(pref.as_slice());
        br.alpha(args.alpha)
            .beta(args.beta)
            .granularity(args.granularity.into_granularity());
        run_and_store(&mut br, predicate, &mut pl, &mut cpl, &thread_pool, &args)?;
    } else {
        let mut br = BiRank::new(&graph, &transpose, args.num_sources);
        br.alpha(args.alpha)
            .beta(args.beta)
            .granularity(args.granularity.into_granularity());
        run_and_store(&mut br, predicate, &mut pl, &mut cpl, &thread_pool, &args)?;
    }

    Ok(())
}
