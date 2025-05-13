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

#[derive(Args, Debug, Clone, Copy)]
#[clap(group = ArgGroup::new("centralities"))]
/// Centralities that can be computed with hyperball.
/// The output files will be named <BASENAME>.<centrality_name>.
pub struct Centralities {
    /// How **all** the centralities will be stored.
    #[clap(long, value_enum, default_value_t = FloatVectorFormat::ZstdAscii)]
    pub fmt: FloatVectorFormat,
    /// How many decimal digits will be used to store centralities in text formats.
    pub precision: Option<usize>,

    /// Compute the approximate sum of distances and save them as <BASENAME>.sum_of_distances
    #[clap(long)]
    pub sum_of_distances: bool,
    /// Compute the approximate number of reachable nodes and save them as <BASENAME>.reachable_nodes
    #[clap(long)]
    pub reachable_nodes: bool,
    /// Compute the approximate harmonic centralities and save them as <BASENAME>.harmonic
    #[clap(long)]
    pub harmonic: bool,
    /// Compute the approximate closeness centralities and save them as <BASENAME>.closeness
    #[clap(long)]
    pub closeness: bool,
    /// Compute the approximate lin centralities and save them as <BASENAME>.lin
    #[clap(long)]
    pub lin: bool,
    /// Compute the approximate nieminen centralities and save them as <BASENAME>.nieminen
    #[clap(long)]
    pub nieminen: bool,
    /// Compute **all** the centralities and save them as <BASENAME>.<centrality_name>.
    #[clap(long)]
    pub all: bool,
    // TODO!: discounted ?
    // TODO!: neighborhood_function ?
}

impl Centralities {
    pub fn should_compute_sum_of_distances(&self) -> bool {
        self.sum_of_distances || self.all || self.nieminen || self.lin || self.closeness
    }
    pub fn should_compute_sum_of_inverse_distances(&self) -> bool {
        self.all || self.harmonic
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "hyperball",
    about = "Use hyperball to compute centralities. (WORK IN PROGRESS)",
    long_about = ""
)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    #[clap(long, default_value_t = false)]
    /// Whether the graph is symmetric or not. If true, the algorithm will
    /// use the graph as its transposed. This speeds up the computation.
    pub symm: bool,

    /// The basename of the transposed graph. If passed hyperball will do
    /// systolic iterations which will speed up the computation.
    #[clap(short, long)]
    pub transposed: Option<PathBuf>,

    #[clap(flatten)]
    pub centralities: Centralities,

    #[clap(short = 'm', long, default_value_t = 14)]
    /// The log2 of the number of registers for the hyperloglog estimators.
    pub log2m: usize,

    #[clap(long, default_value_t = usize::MAX)]
    /// Maximum number of iterations to run. If the algorithm converges before
    /// this number, it will stop.
    pub upper_bound: usize,

    #[clap(long)]
    /// A value that will be used to stop the computation by
    /// relative increment if the neighborhood function is being computed.
    /// If not passed, the computation will stop when no estimators are modified.
    pub threshold: Option<f64>,

    /// How many random values will be inserted in each hyperloglog counter at
    /// the start of the algorithm. By default, it's 1 for every node.
    #[clap(short = 'w', long)]
    pub weights: Option<PathBuf>,

    // #[clap(long, value_enum, default_value_t = VectorFormat::Java)]
    // /// What format the weights are in.
    // pub weights_fmt: VectorFormat,
    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,

    #[clap(long, default_value_t = 42)]
    /// The random state to use for the initialization of the hyperloglog
    /// estimators.
    pub seed: u64,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    ensure!(
        !args.symm || args.transposed.is_none(),
        "If the graph is symmetric, you should not pass the transposed graph."
    );
    ensure!(
        args.centralities.all
            || args.centralities.sum_of_distances
            || args.centralities.reachable_nodes
            || args.centralities.harmonic
            || args.centralities.closeness
            || args.centralities.lin
            || args.centralities.nieminen,
        "You should pass at least one centrality to compute. Use --all to compute all of them."
    );

    match get_endianness(&args.src)?.as_str() {
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

    let graph = BvGraph::with_basename(&args.src).load()?;

    log::info!("Loading DCF...");
    if !args.src.with_extension(DEG_CUMUL_EXTENSION).exists() {
        log::error!(
            "Missing DCF file. Please run `webgraph build dcf {}`.",
            args.src.display()
        );
    }
    let deg_cumul = DCF::mmap(
        args.src.with_extension(DEG_CUMUL_EXTENSION),
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

    let weights = args.weights.map(|_weights| {
        log::info!("Loading Weights...");
        todo!("Load weights");
    });

    let mut hbll = HyperBallBuilder::with_hyper_log_log(
        &graph,
        transposed_ref,
        deg_cumul.as_ref(),
        args.log2m,
        weights,
    )?
    .granularity(args.granularity.into_granularity())
    .sum_of_distances(args.centralities.should_compute_sum_of_distances())
    .sum_of_inverse_distances(args.centralities.should_compute_sum_of_inverse_distances())
    .build(&mut pl);

    log::info!("Starting Hyperball...");
    let rng = rand::rngs::SmallRng::seed_from_u64(args.seed);
    hbll.run(args.upper_bound, args.threshold, &thread_pool, rng, &mut pl)?;

    log::info!("Storing the results...");

    /// here we use a macro to avoid duplicatinge the code, it can't be a function
    /// because different centralities have different return types
    macro_rules! store_centrality {
        ($flag:ident, $method:ident, $extension:literal, $description:expr) => {{
            if args.centralities.all || args.centralities.$flag {
                let path = args.src.with_extension($extension);
                log::info!("Saving {} to {}", $description, path.display());
                let value = hbll.$method()?;
                args.centralities
                    .fmt
                    .store(path, &value, args.centralities.precision)?;
            }
        }};
    }

    store_centrality!(
        sum_of_distances,
        sum_of_distances,
        "sum_of_distances",
        "sum of distances"
    );
    store_centrality!(
        harmonic,
        harmonic_centralities,
        "harmonic",
        "harmonic centralities"
    );
    store_centrality!(
        closeness,
        closeness_centrality,
        "closeness",
        "closeness centralities"
    );
    store_centrality!(lin, lin_centrality, "lin", "lin centralities");
    store_centrality!(
        nieminen,
        nieminen_centrality,
        "nieminen",
        "nieminen centralities"
    );
    store_centrality!(
        reachable_nodes,
        reachable_nodes,
        "reachable_nodes",
        "reachable nodes"
    );

    Ok(())
}
