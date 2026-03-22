/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{FloatSliceFormat, GranularityArgs, LogIntervalArg, NumThreadsArg, get_thread_pool};
use anyhow::{Result, bail, ensure};
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

    /// Compute the approximate sum of distances and save them at the given path.​
    #[clap(long)]
    pub sum_of_distances: Option<PathBuf>,
    /// Compute the approximate number of reachable nodes and save them at the given path.​
    #[clap(long)]
    pub reachable_nodes: Option<PathBuf>,
    /// Compute the approximate harmonic centralities and save them at the given path.​
    #[clap(long)]
    pub harmonic: Option<PathBuf>,
    /// Compute the approximate closeness centralities and save them at the given path.​
    #[clap(long)]
    pub closeness: Option<PathBuf>,
    #[clap(long)]
    /// Compute the approximate neighborhood function and save it at the given path.​
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
#[command(
    name = "hyperball",
    about = "Computes centralities using HyperBall.",
    long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    #[clap(long, default_value_t = false)]
    /// The graph is symmetric (it will be used as its own transpose).​
    pub symm: bool,

    /// The basename of the transposed graph. If available, HyperBall will
    /// perform systolic iterations which will speed up the computation.
    /// If the graph is symmetric, use the --symm option instead.​
    #[clap(short, long)]
    pub transposed: Option<PathBuf>,

    #[clap(flatten)]
    pub centralities: Centralities,

    #[clap(short = 'm', long, default_value_t = 8)]
    /// The base-2 logarithm of the number of registers for the HyperLogLog
    /// cardinality estimators.​
    pub log2m: u32,

    #[clap(short = '8', long)]
    /// Use HyperLogLog8 (byte-sized registers with SIMD-accelerated merges);
    /// trades ~33% extra space for significantly faster merge operations.​
    pub hll8: bool,

    #[clap(long, default_value_t = usize::MAX)]
    /// Maximum number of iterations to run.​
    pub upper_bound: usize,

    #[clap(long)]
    /// A value that will be used to stop the computation by relative increment
    /// if the neighborhood function is being computed. Otherwise, the
    /// computation will stop when all estimators do not change their values.​
    pub threshold: Option<f64>,

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
    ensure!(
        !args.symm || args.transposed.is_none(),
        "If the graph is symmetric, you should not pass the transpose."
    );

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

    log::info!("Loading Transposed graph...");
    let mut transposed = None;
    if let Some(transposed_path) = args.transposed.as_ref() {
        transposed = Some(BvGraph::with_basename(transposed_path).load()?);
    }
    let mut transposed_ref = transposed.as_ref();
    if args.symm {
        transposed_ref = Some(&graph);
    }

    /// Runs HyperBall and stores results. We use a macro because different
    /// estimation logics produce different `HyperBall` types.
    macro_rules! run_and_store {
        ($hb:expr) => {{
            let mut hb = $hb;
            log::info!("Starting HyperBall...");
            let rng = rand::rngs::SmallRng::seed_from_u64(args.seed);
            thread_pool.install(|| hb.run(args.upper_bound, args.threshold, rng, &mut pl))?;

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

    if args.hll8 {
        log::info!("Using HyperLogLog8 (byte-sized registers)");
        let hb = HyperBallBuilder::with_hyper_log_log8(
            &graph,
            transposed_ref,
            deg_cumul.uncase(),
            args.log2m,
            None,
        )?
        .granularity(args.granularity.into_granularity())
        .sum_of_distances(args.centralities.should_compute_sum_of_distances())
        .sum_of_inverse_distances(args.centralities.should_compute_sum_of_inverse_distances())
        .build(&mut pl);
        run_and_store!(hb);
    } else {
        log::info!("Using HyperLogLog (packed registers)");
        let hb = HyperBallBuilder::with_hyper_log_log(
            &graph,
            transposed_ref,
            deg_cumul.uncase(),
            args.log2m,
            None,
        )?
        .granularity(args.granularity.into_granularity())
        .sum_of_distances(args.centralities.should_compute_sum_of_distances())
        .sum_of_inverse_distances(args.centralities.should_compute_sum_of_inverse_distances())
        .build(&mut pl);
        run_and_store!(hb);
    }

    Ok(())
}
