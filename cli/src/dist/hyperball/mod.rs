/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{get_thread_pool, FloatVectorFormat, GlobalArgs, GranularityArgs, NumThreadsArg};
use anyhow::{ensure, Result};
use clap::{ArgGroup, Args, Parser};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
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
/// like `--harmonic >(zstd > harmonic.zstd)`.
pub struct Centralities {
    /// How all the centralities will be stored.
    #[clap(long, value_enum, default_value_t = FloatVectorFormat::Ascii)]
    pub fmt: FloatVectorFormat,
    #[clap(long)]
    /// How many decimal digits will be used to store centralities in text formats.
    pub precision: Option<usize>,

    /// Compute the approximate sum of distances and save them as at the given path.
    #[clap(long)]
    pub sum_of_distances: Option<PathBuf>,
    /// Compute the approximate number of reachable nodes and save them as at the given path.
    #[clap(long)]
    pub reachable_nodes: Option<PathBuf>,
    /// Compute the approximate harmonic centralities and save them as at the given path.
    #[clap(long)]
    pub harmonic: Option<PathBuf>,
    /// Compute the approximate closeness centralities and save them as at the given path.
    #[clap(long)]
    pub closeness: Option<PathBuf>,
    #[clap(long)]
    /// Compute the approximate neighborhood function and save it as at the given path.
    pub neighborhood_function: Option<PathBuf>,
}

impl Centralities {
    pub fn should_compute_sum_of_distances(&self) -> bool {
        self.sum_of_distances.is_some() || self.closeness.is_some()
    }
    pub fn should_compute_sum_of_inverse_distances(&self) -> bool {
        self.harmonic.is_some()
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "hyperball",
    about = "Use hyperball to compute centralities.",
    long_about = ""
)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,

    #[clap(long, default_value_t = false)]
    /// Whether the graph is symmetric or not. If true, the algorithm will
    /// use the graph as its transposed.
    pub symm: bool,

    /// The basename of the transposed graph. If available, HyperBall will
    /// perform systolic iterations which will speed up the computation.
    /// If the graph is symmetric, use the --symm option instead.
    #[clap(short, long)]
    pub transposed: Option<PathBuf>,

    /// Compute the approximate neighborhood function, which will be
    /// store in ASCII format as BASENAME.nf.
    #[clap(short, long)]
    pub neighborhood_function: bool,

    #[clap(flatten)]
    pub centralities: Centralities,

    #[clap(short = 'm', long, default_value_t = 14)]
    /// The base-2 logarithm of the number of registers for the HyperLogLog
    /// cardinality estimators.
    pub log2m: usize,

    #[clap(long, default_value_t = usize::MAX)]
    /// Maximum number of iterations to run.
    pub upper_bound: usize,

    #[clap(long)]
    /// A value that will be used to stop the computation by relative increment
    /// if the neighborhood function is being computed. Otherwise, the
    /// computation will stop all estimators do not change their values.
    pub threshold: Option<f64>,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,

    #[clap(long, default_value_t = 0)]
    /// The seed of the pseudorandom number generator used for initialization.
    pub seed: u64,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    ensure!(
        !args.symm || args.transposed.is_none(),
        "If the graph is symmetric, you should not pass the transpose."
    );

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => hyperball::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => hyperball::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn hyperball<E: Endianness>(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    let mut pl = concurrent_progress_logger![];
    if let Some(log_interval) = global_args.log_interval {
        pl.log_interval(log_interval);
    }
    let thread_pool = get_thread_pool(args.num_threads.num_threads);

    let graph = BvGraph::with_basename(&args.basename).load()?;

    log::info!("Loading DCF...");
    if !args.basename.with_extension(DEG_CUMUL_EXTENSION).exists() {
        log::error!(
            "Missing DCF file. Please run `webgraph build dcf {}`.",
            args.basename.display()
        );
    }
    let deg_cumul = DCF::mmap(
        args.basename.with_extension(DEG_CUMUL_EXTENSION),
        Flags::RANDOM_ACCESS,
    )?;

    log::info!("Loading Transposed graph...");
    let mut transposed = None;
    if let Some(transposed_path) = args.transposed.as_ref() {
        transposed = Some(BvGraph::with_basename(transposed_path).load()?);
    }
    let mut transposed_ref = transposed.as_ref();
    if args.symm {
        transposed_ref = Some(&graph);
    }

    let mut hb = HyperBallBuilder::with_hyper_log_log(
        &graph,
        transposed_ref,
        deg_cumul.as_ref(),
        args.log2m,
        None,
    )?
    .granularity(args.granularity.into_granularity())
    .sum_of_distances(args.centralities.should_compute_sum_of_distances())
    .sum_of_inverse_distances(args.centralities.should_compute_sum_of_inverse_distances())
    .build(&mut pl);

    log::info!("Starting Hyperball...");
    let rng = rand::rngs::SmallRng::seed_from_u64(args.seed);
    hb.run(args.upper_bound, args.threshold, &thread_pool, rng, &mut pl)?;

    log::info!("Storing the results...");

    /// here we use a macro to avoid duplicating the code, it can't be a function
    /// because different centralities have different return types
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

    Ok(())
}
