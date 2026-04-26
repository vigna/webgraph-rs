/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{FloatSliceFormat, GranularityArgs, LogIntervalArg, NumThreadsArg, get_thread_pool};
use anyhow::{Result, bail};
use clap::{ArgGroup, Args, Parser};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{ProgressLog, concurrent_progress_logger};
use epserde::deser::{Deserialize, Flags};
use rand::SeedableRng;
use std::path::PathBuf;
use webgraph::{
    graphs::bvgraph::get_endianness,
    prelude::{BvGraph, DCF, DEG_CUMUL_EXTENSION},
};
use webgraph_algo::distances::hyperball::HyperBallBuilder;

#[derive(Args, Debug, Clone)]
#[clap(group = ArgGroup::new("centralities"))]
/// Centralities that can be computed with hyperball.
///
/// To compress the result you can use named pipes or process substitution
/// like `--harmonic >(zstd > harmonic.zstd)`.​
pub struct Centralities {
    /// Storage format for centralities.​
    #[clap(long, value_enum, default_value_t = FloatSliceFormat::Ascii)]
    pub fmt: FloatSliceFormat,
    #[clap(long)]
    /// Number of decimal digits for centralities in text formats.​
    pub precision: Option<usize>,

    /// Computes the approximate sum of distances and saves them at the given path.​
    #[clap(long)]
    pub sum_of_distances: Option<PathBuf>,
    /// Computes the approximate number of reachable nodes and saves them at the given path.​
    #[clap(long)]
    pub reachable_nodes: Option<PathBuf>,
    /// Computes the approximate harmonic centralities and saves them at the given path.​
    #[clap(long)]
    pub harmonic: Option<PathBuf>,
    /// Computes the approximate closeness centralities and saves them at the given path.​
    #[clap(long)]
    pub closeness: Option<PathBuf>,
    #[clap(long)]
    /// Computes the approximate neighborhood function and saves it at the given path.​
    pub neighborhood_function: Option<PathBuf>,
}

impl Centralities {
    pub const fn should_compute_sum_of_distances(&self) -> bool {
        self.sum_of_distances.is_some() || self.closeness.is_some()
    }
    pub const fn should_compute_sum_of_inverse_distances(&self) -> bool {
        self.harmonic.is_some()
    }
}

#[derive(Parser, Debug)]
#[command(group(
        ArgGroup::new("symmetric or transpose")
        .required(true)
        .multiple(false)
        .args(["transpose", "symmetric"]),
))]
#[command(
    name = "hyperball",
    about = "Computes centralities using HyperBall.",
    long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    /// The basename of the transpose of the graph. If available, HyperBall will
    /// perform systolic iterations which will speed up the computation. If the
    /// graph is symmetric, use the --symm option instead.​
    pub transpose: Option<PathBuf>,

    #[clap(long = "symm", short, default_value_t = false)]
    /// The graph is symmetric (it will be used as its own transpose).​
    pub symmetric: bool,

    #[clap(short = 'm', long, default_value_t = 8)]
    /// The base-2 logarithm of the number of registers for the HyperLogLog
    /// cardinality estimators.​
    pub log2m: u32,

    #[clap(short, long)]
    /// Uses an external (spill-to-disk) output store, keeping only one counter
    /// array in RAM instead of two; halves the counter memory at the cost
    /// of extra I/O after each iteration.​
    pub external: bool,

    #[clap(short = '8', long)]
    /// Use HyperLogLog8 (byte-sized registers with SIMD-accelerated merges);
    /// trades ~33% extra space for significantly faster merge operations.​
    pub hll8: bool,

    #[clap(long)]
    /// Maximum number of iterations to run.​
    pub upper_bound: Option<usize>,

    #[clap(long)]
    /// A value that will be used to stop the computation by relative increment
    /// if the neighborhood function is being computed. Otherwise, the
    /// computation will stop when all estimators do not change their values.​
    pub threshold: Option<f64>,

    #[clap(flatten)]
    pub centralities: Centralities,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,

    #[clap(long, default_value_t = 0)]
    /// The seed of the pseudorandom number generator used for initialization.​
    pub seed: u64,
}

pub fn main(args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => hyperball::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => hyperball::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn hyperball<E: Endianness>(args: CliArgs) -> Result<()> {
    let mut pl = concurrent_progress_logger![];
    if let Some(log_interval) = args.log_interval.log_interval {
        pl.log_interval(log_interval);
    }
    let thread_pool = get_thread_pool(args.num_threads.num_threads);

    let graph = BvGraph::with_basename(&args.basename).load()?;

    log::info!("Loading DCF...");
    if !args.basename.with_extension(DEG_CUMUL_EXTENSION).exists() {
        bail!(
            "Missing DCF file. Please run `webgraph build dcf {}`.",
            args.basename.display()
        );
    }

    let deg_cumul = unsafe {
        DCF::mmap(
            args.basename.with_extension(DEG_CUMUL_EXTENSION),
            Flags::RANDOM_ACCESS,
        )
    }?;

    // As soon as we can use a more recent compiler, this can be avoided using
    // Some(&BvGraph::with_basename(transposed_path).load()?) below.
    let mut _transpose_loaded = None;

    let transpose = if args.symmetric {
        Some(&graph)
    } else if let Some(transposed_path) = args.transpose {
        log::info!("Loading transpose...");
        _transpose_loaded = Some(BvGraph::with_basename(transposed_path).load()?);
        _transpose_loaded.as_ref()
    } else {
        None
    };

    /// Runs HyperBall and stores results. We use a macro because different
    /// estimation logics produce different `HyperBall` types.
    macro_rules! run_and_store {
        ($hb:expr) => {{
            let mut hb = $hb;

            let rng = rand::rngs::SmallRng::seed_from_u64(args.seed);
            thread_pool.install(|| {
                hb.run(
                    args.upper_bound.unwrap_or(usize::MAX),
                    args.threshold,
                    rng,
                    &mut pl,
                )
            })?;

            log::info!("Storing the results...");

            macro_rules! store_centrality {
                ($flag:ident, $method:ident, $description:expr) => {{
                    if let Some(path) = args.centralities.$flag {
                        log::info!("Saving {} to {}", $description, path.display());
                        let value = hb.$method()?;
                        args.centralities
                            .fmt
                            .store(path, &value, args.centralities.precision)?;
                    }
                }};
            }

            store_centrality!(sum_of_distances, sum_of_distances, "sum of distances");
            store_centrality!(harmonic, harmonic_centralities, "harmonic centralities");
            store_centrality!(closeness, closeness_centrality, "closeness centralities");
            store_centrality!(reachable_nodes, reachable_nodes, "reachable nodes");
            store_centrality!(
                neighborhood_function,
                neighborhood_function,
                "neighborhood function"
            );
        }};
    }

    macro_rules! configure_and_run {
        ($builder:expr) => {{
            let hb = $builder
                .granularity(args.granularity.into_granularity())
                .sum_of_distances(args.centralities.should_compute_sum_of_distances())
                .sum_of_inverse_distances(
                    args.centralities.should_compute_sum_of_inverse_distances(),
                )
                .build(&mut pl);
            run_and_store!(hb);
        }};
    }

    match (args.hll8, args.external) {
        (false, false) => configure_and_run!(HyperBallBuilder::with_hyper_log_log(
            &graph,
            transpose,
            deg_cumul.uncase(),
            args.log2m,
            None,
        )?),
        (false, true) => configure_and_run!(HyperBallBuilder::with_hyper_log_log_external(
            &graph,
            transpose,
            deg_cumul.uncase(),
            args.log2m,
            None,
        )?),
        (true, false) => configure_and_run!(HyperBallBuilder::with_hyper_log_log8(
            &graph,
            transpose,
            deg_cumul.uncase(),
            args.log2m,
            None,
        )?),
        (true, true) => configure_and_run!(HyperBallBuilder::with_hyper_log_log8_external(
            &graph,
            transpose,
            deg_cumul.uncase(),
            args.log2m,
            None,
        )?),
    }

    Ok(())
}
