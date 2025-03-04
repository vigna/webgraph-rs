/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use self::llp::preds::MinAvgImprov;

use crate::cli::create_parent_dir;
use crate::cli::NumThreadsArg;
use crate::prelude::*;
use anyhow::{bail, Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use llp::invert_permutation;
use llp::preds::{MaxUpdates, MinGain, MinModified, PercModified};

use predicates::prelude::*;
use rayon::prelude::*;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "llp";

#[derive(Args, Debug)]
#[command(about = "Computes a permutation of a graph using Layered Label Propagation.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    /// A filename for the LLP permutation in binary big-endian format.
    pub perm: PathBuf,

    #[arg(short, long)]
    /// Save the permutation in ε-serde format.
    pub epserde: bool,

    #[arg(short, long, allow_hyphen_values = true, use_value_delimiter = true, value_delimiter = ',', default_values_t = vec!["-0".to_string(), "-1".to_string(), "-2".to_string(), "-3".to_string(), "-4".to_string(), "-5".to_string(), "-6".to_string(), "-7".to_string(), "-8".to_string(), "-9".to_string(), "-10".to_string()])]
    /// The ɣ's to use in LLP, separated by commas. The format is given by a
    /// integer numerator (if missing, assumed to be one), a dash, and then a
    /// power-of-two exponent for the denominator. For example, -2 is 1/4, and
    /// 0-0 is 0.
    pub gammas: Vec<String>,

    #[arg(short = 'u', long, default_value_t = 100)]
    /// If specified, the maximum number of updates for a given ɣ.
    pub max_updates: usize,

    #[arg(short = 'M', long)]
    /// If true, updates will be stopped when the number of modified nodes is less
    /// than the square root of the number of nodes of the graph.
    pub modified: bool,

    #[arg(short = 'p', long)]
    /// If true, updates will be stopped when the number of modified nodes is less
    /// than the specified percentage of the number of nodes of the graph.
    pub perc_modified: Option<f64>,

    #[arg(short = 't', long, default_value_t = MinGain::DEFAULT_THRESHOLD)]
    /// The gain threshold used to stop the computation (0 to disable).
    pub gain_threshold: f64,

    #[arg(short = 'i', long, default_value_t = MinAvgImprov::DEFAULT_THRESHOLD)]
    /// The threshold on the average (over the last ten updates) gain
    /// improvement used to stop the computation (-Inf to disable).
    pub improv_threshold: f64,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[arg(short, long, default_value_t = 0)]
    /// The seed to use for the PRNG.
    pub seed: u64,

    #[arg(long, conflicts_with("slack"))]
    /// The tentative number of arcs used define the size of a parallel job
    /// (advanced option).
    pub granularity: Option<usize>,

    #[arg(long, conflicts_with("granularity"))]
    /// The slack for relative granularity.
    pub slack: Option<f64>,

    #[arg(long)]
    /// The chunk size used to localize the random permutation
    /// (advanced option).
    pub chunk_size: Option<usize>,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    create_parent_dir(&args.perm)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => llp::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => llp::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn llp<E: Endianness + 'static + Send + Sync>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodesRead<E> + BitSeek,
{
    let start = std::time::Instant::now();

    // Load the graph in THP memory
    log::info!(
        "Loading graph {} in THP memory...",
        args.src.to_string_lossy()
    );
    let graph = BvGraph::with_basename(&args.src)
        .mode::<LoadMmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .endianness::<E>()
        .load()?;

    // Load degree cumulative function in THP memory
    log::info!("Loading DCF in THP memory...");
    let deg_cumul = DCF::load_mmap(
        args.src.with_extension(DEG_CUMUL_EXTENSION),
        Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS,
    )
    .with_context(|| {
        format!(
            "Could not load degree cumulative function for basename {}",
            args.src.display()
        )
    })?;

    // parse the gamma format
    let mut gammas = vec![];
    for gamma in args.gammas {
        let t: Vec<_> = gamma.split('-').collect();
        if t.len() != 2 {
            bail!("Invalid gamma: {}", gamma);
        }
        gammas.push(
            if t[0].is_empty() {
                1.0
            } else {
                t[0].parse::<usize>()? as f64
            } * (0.5_f64).powf(t[1].parse::<usize>()? as f64),
        );
    }

    gammas.sort_by(|a, b| a.total_cmp(b));

    let mut predicate = MinGain::try_from(args.gain_threshold)?.boxed();
    predicate = predicate
        .or(MinAvgImprov::try_from(args.improv_threshold)?)
        .boxed();
    predicate = predicate.or(MaxUpdates::from(args.max_updates)).boxed();

    if args.modified {
        predicate = predicate.or(MinModified::default()).boxed();
    }

    if let Some(perc_modified) = args.perc_modified {
        predicate = predicate.or(PercModified::try_from(perc_modified)?).boxed();
    }

    let num_nodes = graph.num_nodes();

    let granularity = if let Some(granularity) = args.granularity {
        Some(Granularity::Absolute(granularity))
    } else {
        args.slack.map(|slack| Granularity::Relative {
            slack,
            min_len: 1000,
            max_len: 100000, // TODO: make this dependent on default values
        })
    };

    // compute the LLP
    let labels = llp::layered_label_propagation(
        graph,
        &*deg_cumul,
        gammas,
        Some(args.num_threads.num_threads),
        args.chunk_size,
        granularity,
        args.seed,
        predicate,
    )
    .context("Could not compute the LLP")?;

    let mut llp_perm = (0..num_nodes).collect::<Vec<_>>();
    llp_perm.par_sort_by(|&a, &b| labels[a].cmp(&labels[b]));

    let mut llp_inv_perm = vec![0; llp_perm.len()];
    invert_permutation(llp_perm.as_ref(), llp_inv_perm.as_mut());

    log::info!("Elapsed: {}", start.elapsed().as_secs_f64());
    log::info!("Saving permutation...");

    let perm = args.perm;

    if args.epserde {
        llp_inv_perm
            .store(&perm)
            .with_context(|| format!("Could not write permutation to {}", perm.display()))?;
    } else {
        let mut file = std::fs::File::create(&perm)
            .with_context(|| format!("Could not create permutation at {}", perm.display()))?;
        let mut buf = BufWriter::new(&mut file);
        for word in llp_inv_perm.into_iter() {
            buf.write_all(&word.to_be_bytes())
                .with_context(|| format!("Could not write permutation to {}", perm.display()))?;
        }
    }
    log::info!("Completed in {} seconds", start.elapsed().as_secs_f64());
    Ok(())
}
