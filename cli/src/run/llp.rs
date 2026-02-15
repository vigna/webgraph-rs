/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use crate::GranularityArgs;
use crate::NumThreadsArg;
use crate::create_parent_dir;
use crate::get_thread_pool;
use anyhow::{Context, Result, bail};
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use webgraph::prelude::*;
use webgraph_algo::llp::preds::{MaxUpdates, MinAvgImprov, MinGain, MinModified, PercModified};
use webgraph_algo::{combine_labels, labels_to_ranks};

use predicates::prelude::*;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use tempfile::tempdir;

#[derive(Parser, Debug)]
#[command(name = "llp", about = "Computes a permutation of a graph using Layered Label Propagation.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    /// A filename for the LLP permutation in binary big-endian format. If not
    /// provided, we will compute the labels but not combine them into the final
    /// permutation. If you don't set this parameter, be sure to set "work_dir"
    /// so the labels will not be deleted at the end.
    pub perm: Option<PathBuf>,

    /// The folder where the LLP labels are stored. If not specified, a temp
    /// dir will be used which will be deleted at the end of the computation.
    /// The temp dir parent folder can be specified with the TMPDIR environment
    /// variable.
    /// Setting a work_dir has two purposes: saving the information they
    /// compute and to be able to resume the computation of gammas as their
    /// computation on large graphs might take days.
    /// The labels represent information about communities in the graph, nodes
    /// similar will have the same label.
    /// To resume computation you can compute the remaining gammas without
    /// passing "perm", and then finally run "combine" that will combine all the
    /// labels of the gammas present in the folder into a final permutation.
    #[arg(short, long)]
    pub work_dir: Option<PathBuf>,

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

    #[clap(flatten)]
    pub granularity: GranularityArgs,

    #[arg(long)]
    /// The chunk size used to localize the random permutation
    /// (advanced option).
    pub chunk_size: Option<usize>,
}

/// Stores labels with or without epserde.
pub fn store_perm(data: &[usize], perm: impl AsRef<Path>, epserde: bool) -> Result<()> {
    if epserde {
        unsafe {
            data.store(&perm).with_context(|| {
                format!("Could not write permutation to {}", perm.as_ref().display())
            })
        }
    } else {
        let mut file = std::fs::File::create(&perm).with_context(|| {
            format!(
                "Could not create permutation at {}",
                perm.as_ref().display()
            )
        })?;
        let mut buf = BufWriter::new(&mut file);
        for word in data.iter() {
            buf.write_all(&word.to_be_bytes()).with_context(|| {
                format!("Could not write permutation to {}", perm.as_ref().display())
            })?;
        }
        Ok(())
    }
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    if args.perm.is_none() && args.work_dir.is_none() {
        log::warn!(concat!(
            "If `perm` is not set the llp will just compute the labels and not produce the final permutation. ",
            "But you didn't set `work_dir` so the labels will be stored in a temp dir that will be deleted at the end of computation. ",
            "Either set `perm` if you want to compute the permutation, or `work_dir` if you want the labels and combine them later."
        ));
        return Ok(());
    }

    if let Some(perm) = &args.perm {
        create_parent_dir(perm)?;
    }

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => llp::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => llp::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn llp<E: Endianness + 'static + Send + Sync>(
    _global_args: GlobalArgs,
    args: CliArgs,
) -> Result<()>
where
    MemoryFactory<E, MmapHelper<u32>>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, LoadMmap>: BitSeek,
{
    let start = std::time::Instant::now();
    // due to ownership problems, we always create the temp dir, but only use it
    // if the user didn't provide a work_dir
    let temp_dir = tempdir()?;
    let work_dir = args.work_dir.as_deref().unwrap_or(temp_dir.path());
    log::info!("Using workdir: {}", work_dir.display());

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
    let deg_cumul = unsafe {
        DCF::load_mmap(
            args.src.with_extension(DEG_CUMUL_EXTENSION),
            Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS,
        )
        .with_context(|| {
            format!(
                "Could not load degree cumulative function for basename {}",
                args.src.display()
            )
        })
    }?;

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

    let granularity = args.granularity.into_granularity();

    let thread_pool = get_thread_pool(args.num_threads.num_threads);
    thread_pool.install(|| -> Result<()> {
        // compute the LLP
        webgraph_algo::llp::layered_label_propagation_labels_only(
            graph,
            deg_cumul.uncase(),
            gammas,
            args.chunk_size,
            granularity,
            args.seed,
            predicate,
            work_dir,
        )
        .context("Could not compute LLP")?;

        log::info!("Elapsed: {}", start.elapsed().as_secs_f64());

        if let Some(perm_path) = args.perm {
            let labels = combine_labels(work_dir)?;
            log::info!("Combined labels...");
            let rank_perm = labels_to_ranks(&labels);
            log::info!("Saving permutation...");
            store_perm(&rank_perm, perm_path, args.epserde)?;
        }

        Ok(())
    })
}
