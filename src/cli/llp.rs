/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use self::llp::preds::MinAvgImprov;

use super::utils::*;
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
#[command(about = "Performs an LLP round.", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,

    /// A filename for the LLP permutation.
    perm: PathBuf,

    #[arg(short, long, allow_hyphen_values = true, use_value_delimiter = true, value_delimiter = ',', default_values_t = vec!["-0".to_string(), "-1".to_string(), "-2".to_string(), "-3".to_string(), "-4".to_string(), "-5".to_string(), "-6".to_string(), "-7".to_string(), "-8".to_string(), "-9".to_string(), "-10".to_string(), "0-0".to_string()])]
    /// The ɣ's to use in LLP, separated by commas. The format is given by a
    /// integer numerator (if missing, assumed to be one), a dash, and then a
    /// power-of-two exponent for the denominator. For example, -2 is 1/4, and
    /// 0-0 is 0.
    gammas: Vec<String>,

    #[arg(short = 'u', long, default_value_t = 100)]
    /// If specified, the maximum number of updates for a given ɣ.
    max_updates: usize,

    #[arg(short = 'M', long)]
    /// If true, updates will be stopped when the number of modified nodes is less
    /// than the square root of the number of nodes of the graph.
    modified: bool,

    #[arg(short = 'p', long)]
    /// If true, updates will be stopped when the number of modified nodes is less
    /// than the specified percentage of the number of nodes of the graph.
    perc_modified: Option<f64>,

    #[arg(short = 't', long, default_value_t = MinGain::DEFAULT_THRESHOLD)]
    /// The gain threshold used to stop the computation (0 to disable).
    gain_threshold: f64,

    #[arg(short = 'i', long, default_value_t = MinAvgImprov::DEFAULT_THRESHOLD)]
    /// The threshold on the average (over the last ten updates) gain
    /// improvement used to stop the computation (-Inf to disable).
    improv_threshold: f64,

    #[clap(flatten)]
    num_cpus: NumCpusArg,

    #[arg(short, long, default_value_t = 0)]
    /// The seed to use for the PRNG.
    seed: u64,

    #[arg(short, long)]
    /// Save the permutation in ε-serde format.
    epserde: bool,

    #[arg(long)]
    /// The tentative number of arcs used define the size of a parallel job
    /// (advanced option).
    granularity: Option<usize>,

    #[arg(long)]
    /// The chunk size used to localize the random permutation
    /// (advanced option).
    chunk_size: Option<usize>,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => llp_impl::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => llp_impl::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn llp_impl<E: Endianness + 'static + Send + Sync>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let start = std::time::Instant::now();

    // Load the graph in THP memory
    log::info!(
        "Loading graph {} in THP memory...",
        args.basename.to_string_lossy()
    );
    let graph = BVGraph::with_basename(&args.basename)
        .mode::<LoadMmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .endianness::<E>()
        .load()?;

    // Load degree cumulative function in THP memory
    log::info!("Loading DCF in THP memory...");
    let deg_cumul = DCF::load_mmap(
        args.basename.with_extension(DEG_CUMUL_EXTENSION),
        Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS,
    )
    .with_context(|| {
        format!(
            "Could not load degree cumulative function for basename {}",
            args.basename.display()
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

    // compute the LLP
    let labels = llp::layered_label_propagation(
        &graph,
        &*deg_cumul,
        gammas,
        Some(args.num_cpus.num_cpus),
        args.chunk_size,
        args.granularity,
        args.seed,
        predicate,
    )
    .context("Could not compute the LLP")?;

    let mut llp_perm = (0..graph.num_nodes()).collect::<Vec<_>>();
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
