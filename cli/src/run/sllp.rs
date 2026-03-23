/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GranularityArgs;
use crate::IntSliceFormat;
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
use std::path::PathBuf;
use tempfile::tempdir;

use super::llp::store_perm;

#[derive(Parser, Debug)]
#[command(
    name = "sllp",
    about = "Computes a permutation of a graph using Synchronous Layered Label Propagation.",
    long_about = None,
    next_line_help = true
)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,

    /// Output filename for the SLLP permutation. If not provided, only labels
    /// are computed without combining them into a permutation. In that case,
    /// --work-dir should be set to preserve the labels.
    pub perm: Option<PathBuf>,

    /// The folder where the SLLP labels are stored. If not specified, a
    /// temporary directory is used and deleted at the end; the parent folder
    /// for temporary directories can be set with the TMPDIR environment
    /// variable. A work directory serves to save the labels and to resume
    /// the computation of gammas, whose computation on large graphs might
    /// take days.
    #[arg(short, long)]
    pub work_dir: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.
    pub fmt: IntSliceFormat,

    #[arg(short, long, allow_hyphen_values = true, use_value_delimiter = true, value_delimiter = ',', default_values_t = vec!["-0".to_string(), "-1".to_string(), "-2".to_string(), "-3".to_string(), "-4".to_string(), "-5".to_string(), "-6".to_string(), "-7".to_string(), "-8".to_string(), "-9".to_string(), "-10".to_string()])]
    /// The ɣ's to use in SLLP, separated by commas. The format is given by an
    /// integer numerator (if missing, assumed to be one), a dash, and then a
    /// power-of-two exponent for the denominator. For example, -2 is 1/4, and
    /// 0-0 is 0.
    pub gammas: Vec<String>,

    #[arg(short = 'u', long, default_value_t = 100)]
    /// Maximum number of updates for a given ɣ.
    pub max_updates: usize,

    #[arg(short = 'M', long)]
    /// Stop updates when the number of modified nodes falls below the square
    /// root of the number of nodes.
    pub modified: bool,

    #[arg(short = 'p', long)]
    /// Stop updates when the fraction of modified nodes falls below this
    /// percentage.
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

    #[clap(flatten)]
    pub granularity: GranularityArgs,
}

pub fn main(args: CliArgs) -> Result<()> {
    if args.perm.is_none() && args.work_dir.is_none() {
        log::warn!(concat!(
            "If `perm` is not set the sllp will just compute the labels and not produce the final permutation. ",
            "But you didn't set `work_dir` so the labels will be stored in a temp dir that will be deleted at the end of computation. ",
            "Either set `perm` if you want to compute the permutation, or `work_dir` if you want the labels and combine them later."
        ));
        return Ok(());
    }

    if let Some(perm) = &args.perm {
        create_parent_dir(perm)?;
    }

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => sllp::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => sllp::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn sllp<E: Endianness + 'static + Send + Sync>(args: CliArgs) -> Result<()>
where
    MemoryFactory<E, MmapHelper<u32>>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, LoadMmap>: BitSeek,
{
    let start = std::time::Instant::now();
    let temp_dir = tempdir()?;
    let work_dir = args.work_dir.as_deref().unwrap_or(temp_dir.path());
    log::info!("Using workdir: {}", work_dir.display());

    log::info!(
        "Loading graph {} in THP memory...",
        args.basename.to_string_lossy()
    );
    let graph = BvGraph::with_basename(&args.basename)
        .mode::<LoadMmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .endianness::<E>()
        .load()?;

    log::info!("Loading DCF in THP memory...");
    let deg_cumul = unsafe {
        DCF::load_mmap(
            args.basename.with_extension(DEG_CUMUL_EXTENSION),
            Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS,
        )
        .with_context(|| {
            format!(
                "Could not load degree cumulative function for basename {}",
                args.basename.display()
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
        webgraph_algo::llp::sync_llp::sync_layered_label_propagation(
            graph,
            deg_cumul.uncase(),
            gammas,
            granularity,
            predicate,
            work_dir,
        )
        .context("Could not compute SLLP")?;

        log::info!("Elapsed: {}", start.elapsed().as_secs_f64());

        if let Some(perm_path) = args.perm {
            let labels = combine_labels(work_dir)?;
            log::info!("Combined labels...");
            let rank_perm = labels_to_ranks(&labels);
            log::info!("Saving permutation...");
            store_perm(&rank_perm, perm_path, args.fmt)?;
        }

        Ok(())
    })
}
