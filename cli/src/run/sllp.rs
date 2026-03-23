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
use log::info;
use mmap_rs::{MmapFlags, MmapMut};
use rayon::prelude::*;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use webgraph::prelude::*;
use webgraph::utils::MmapHelper;
use webgraph_algo::llp::preds::{MaxUpdates, MinAvgImprov, MinGain, MinModified, PercModified};
use webgraph_algo::llp::{LabelsStore, combine, invert_permutation};

use predicates::prelude::*;

use super::llp::store_perm;

/// Create a file of the given byte length and return a mutable mmap over it.
fn create_mmap(path: &Path, byte_len: usize) -> Result<MmapHelper<usize, MmapMut>> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("Could not create {}", path.display()))?
        .set_len(byte_len as u64)
        .with_context(|| format!("Could not extend {}", path.display()))?;
    MmapHelper::<usize, MmapMut>::mmap_mut(path, MmapFlags::empty())
        .with_context(|| format!("Could not mmap {}", path.display()))
}

/// Mmap-backed version of [`webgraph_algo::combine_labels`].
///
/// All large arrays (`result_labels`, `temp_perm`) are backed by memory-mapped
/// files in `work_dir` so the operation does not require O(n) heap memory.
/// Returns an mmap over the combined labels.
fn combine_labels_mmap(
    work_dir: &Path,
    num_nodes: usize,
) -> Result<MmapHelper<usize, MmapMut>> {
    let byte_len = num_nodes * size_of::<usize>();
    let result_path = work_dir.join("_combine_result.tmp");
    let perm_path = work_dir.join("_combine_perm.tmp");

    let mut result_labels = create_mmap(&result_path, byte_len)
        .context("Could not create result_labels mmap")?;
    let mut temp_perm = create_mmap(&perm_path, byte_len)
        .context("Could not create temp_perm mmap")?;

    // Scan work directory for label files.
    let mut gammas = vec![];
    let iter = std::fs::read_dir(work_dir)?
        .filter_map(Result::ok)
        .filter(|path| {
            let name = path.file_name();
            let s = name.to_string_lossy();
            s.starts_with("labels_")
                && s.ends_with(".bin")
                && path.file_type().is_ok_and(|ft| ft.is_file())
        });

    let mmap_flags = Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS;
    for entry in iter {
        let path = work_dir.join(entry.file_name());
        let res = unsafe {
            <LabelsStore<Vec<usize>>>::mmap(&path, Flags::default())
                .with_context(|| format!("Could not load labels from {}", path.display()))?
        };
        let res = res.uncase();
        info!(
            "Found labels from {:?} with gamma {} and cost {} and num_nodes {}",
            path.display(),
            res.gamma,
            res.gap_cost,
            res.labels.len(),
        );
        anyhow::ensure!(
            res.labels.len() == num_nodes,
            "Labels '{}' have length {} but expected {}",
            path.display(),
            res.labels.len(),
            num_nodes,
        );
        gammas.push((res.gap_cost, res.gamma, path));
    }

    if gammas.is_empty() {
        bail!("No labels were found in {}", work_dir.display());
    }

    // Sort by cost descending — best (lowest cost) is last.
    gammas.sort_by(|(a, _, _), (b, _, _)| b.total_cmp(a));

    let (best_cost, best_gamma, best_path) = gammas.last().unwrap();
    let (worst_cost, worst_gamma, _) = &gammas[0];
    info!("Best gamma: {}\twith log-gap cost {}", best_gamma, best_cost);
    info!(
        "Worst gamma: {}\twith log-gap cost {}",
        worst_gamma, worst_cost
    );

    // Initialize result_labels from the best gamma's labels.
    {
        let best = unsafe {
            <LabelsStore<Vec<usize>>>::load_mmap(best_path, mmap_flags)
                .context("Could not mmap best gamma labels")?
        };
        let src = best.uncase().labels;
        let dst = result_labels.as_mut();
        dst.par_iter_mut()
            .zip(src.par_iter())
            .for_each(|(d, s)| *d = *s);
    }

    // Combine each gamma's labels into result.
    for (i, (cost, gamma, gamma_path)) in gammas.iter().enumerate() {
        info!(
            "Starting step {} with gamma {} cost {} and labels {:?}...",
            i, gamma, cost, gamma_path
        );
        let labels = unsafe {
            <LabelsStore<Vec<usize>>>::load_mmap(gamma_path, mmap_flags)
                .context("Could not load labels")?
        };

        combine(
            result_labels.as_mut(),
            labels.uncase().labels,
            temp_perm.as_mut(),
        )
        .context("Could not combine labels")?;
        drop(labels);

        // Recombination with best labels (Marco Rosa heuristic from Java LAW).
        info!(
            "Recombining with gamma {} cost {} and labels {:?}...",
            best_gamma, best_cost, best_path
        );
        let best_labels = unsafe {
            <LabelsStore<Vec<usize>>>::load_mmap(best_path, mmap_flags)
                .context("Could not load labels from best gamma")?
        };
        let n = combine(
            result_labels.as_mut(),
            best_labels.uncase().labels,
            temp_perm.as_mut(),
        )?;
        info!("Number of labels: {}", n);
    }

    // Clean up scratch file; result_labels stays alive.
    drop(temp_perm);
    let _ = std::fs::remove_file(perm_path);

    Ok(result_labels)
}

/// Mmap-backed version of [`webgraph_algo::labels_to_ranks`].
///
/// All large arrays (`perm`, `inv_perm`) are backed by memory-mapped files.
/// Returns `(inv_perm_mmap, result_path)` — the caller owns the mmap and is
/// responsible for removing `result_path` and `perm_path` after use.
fn labels_to_ranks_mmap(
    labels: &[usize],
    work_dir: &Path,
) -> Result<MmapHelper<usize, MmapMut>> {
    let num_nodes = labels.len();
    let byte_len = num_nodes * size_of::<usize>();
    let perm_path = work_dir.join("_rank_perm.tmp");
    let inv_path = work_dir.join("_rank_inv.tmp");

    let mut perm = create_mmap(&perm_path, byte_len)
        .context("Could not create rank perm mmap")?;
    let mut inv = create_mmap(&inv_path, byte_len)
        .context("Could not create rank inv mmap")?;

    // Initialize perm to identity and sort by labels.
    let perm_slice = perm.as_mut();
    perm_slice
        .par_iter_mut()
        .enumerate()
        .for_each(|(i, x)| *x = i);
    perm_slice.par_sort_by(|&a, &b| labels[a].cmp(&labels[b]));

    // Invert into inv.
    invert_permutation(perm.as_ref(), inv.as_mut());

    // Clean up perm scratch.
    drop(perm);
    let _ = std::fs::remove_file(perm_path);

    Ok(inv)
}

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
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let start = std::time::Instant::now();
    let temp_dir = tempdir()?;
    let work_dir = args.work_dir.as_deref().unwrap_or(temp_dir.path());
    log::info!("Using workdir: {}", work_dir.display());

    log::info!(
        "Memory-mapping graph {}...",
        args.basename.to_string_lossy()
    );
    let graph = BvGraph::with_basename(&args.basename)
        .mode::<Mmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .endianness::<E>()
        .load()?;

    let num_nodes = graph.num_nodes();

    log::info!("Memory-mapping DCF...");
    let deg_cumul = unsafe {
        DCF::mmap(
            args.basename.with_extension(DEG_CUMUL_EXTENSION),
            Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS,
        )
        .with_context(|| {
            format!(
                "Could not mmap degree cumulative function for basename {}",
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
            // Combine labels and compute the ranking permutation using
            // mmap-backed scratch files — no O(n) heap allocations.
            let combined = combine_labels_mmap(work_dir, num_nodes)
                .context("Could not combine labels")?;
            log::info!("Combined labels...");

            let rank_perm = labels_to_ranks_mmap(combined.as_ref(), work_dir)
                .context("Could not compute ranks")?;
            log::info!("Saving permutation...");

            store_perm(rank_perm.as_ref(), &perm_path, args.fmt)?;

            // Clean up mmap temp files.
            let result_path = work_dir.join("_combine_result.tmp");
            let inv_path = work_dir.join("_rank_inv.tmp");
            drop(combined);
            drop(rank_perm);
            let _ = std::fs::remove_file(result_path);
            let _ = std::fs::remove_file(inv_path);
        }

        Ok(())
    })
}
