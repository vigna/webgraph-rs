/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Layered Label Propagation.
//!
//! An implementation of the _layered label propagation_ algorithm described by
//! Paolo Boldi, Marco Rosa, Massimo Santini, and Sebastiano Vigna in "[Layered
//! Label Propagation: A MultiResolution Coordinate-Free Ordering for
//! Compressing Social Networks][LLP paper]", _Proceedings of the 20th
//! international conference on World Wide Web_, pages 587–596, ACM, 2011.
//!
//! # Requirements
//!
//! The graph provided should be _symmetric_ and _loopless_. If this is not the
//! case, please use [`symmetrize`] to generate a suitable graph.
//!
//! [`symmetrize`]: webgraph::transform::symmetrize
//!
//! # Memory Requirements
//!
//! LLP requires two `usize` and a boolean per node, plus the memory that is
//! necessary to load the graph. There is also some local memory per thread
//! (hash maps for counting neighbor labels), but it is usually negligible
//! compared to the memory for labels and volumes.
//!
//! # Algorithm
//!
//! Label propagation assigns a _label_ to each node and then iteratively
//! updates every label to the one that maximizes an objective function based on
//! the frequency of labels among the node's neighbors and on a resolution
//! parameter ɣ. Low ɣ values produce many small communities, while high ɣ
//! values produce few large ones. _Layered_ label propagation runs label
//! propagation for several values of ɣ and combines the resulting labelings
//! into a single one that captures community structure at multiple resolutions.
//!
//! Nodes of the resulting labeling that share the same label are likely
//! co-located in the graph, so [permuting the graph] in label order will
//! increase locality, yielding better compression.
//!
//! [permuting the graph]: webgraph::transform::permute
//!
//! # Functions
//!
//! - [`layered_label_propagation`]: runs LLP and returns the final combined
//!   labels;
//! - [`layered_label_propagation_labels_only`]: runs LLP and stores
//!   per-ɣ labels to disk, but does not combine them; this is useful when
//!   you want to combine labels in a separate step;
//! - [`combine_labels`]: combines the per-ɣ labels stored on disk by a
//!   previous call to [`layered_label_propagation_labels_only`];
//! - [`labels_to_ranks`]: converts labels to ranks by their natural order,
//!   yielding a permutation that can be passed to [`permute`].
//!
//! [`permute`]: webgraph::transform::permute
//!
//! # Choosing ɣ Values
//!
//! More values improve the resulting combined labeling, but each value needs a
//! full run of the label propagation algorithm, so there is a trade-off between
//! quality and running time. A common choice is a set exponentially-spaced
//! values, for example ɣ ∈ {1, 1/2, 1/4, …} or ɣ ∈ {1, 1/4, 1/16, …}.
//!
//! [LLP paper]: <https://vigna.di.unimi.it/papers.php#BRSLLP>
//!
use anyhow::{Context, Result};
use crossbeam_utils::CachePadded;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use mmap_rs::MmapFlags;
use predicates::Predicate;
use preds::PredParams;

use log::info;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use sux::traits::{IndexedSeq, Succ};
use sync_cell_slice::SyncSlice;
use tempfile::NamedTempFile;
use webgraph::prelude::PermutedGraph;
use webgraph::traits::RandomAccessGraph;
use webgraph::utils::Granularity;
use webgraph::utils::MmapHelper;

pub(crate) mod gap_cost;
pub(crate) mod label_store;
mod mix64;
/// Stopping predicates for LLP.
pub mod preds;

const RAYON_MIN_LEN: usize = 100000;
// This is a bit ugly but prevents from mistakenly interpreting the gap cost
// files as labels files.
const GAP_COST_EXTENSION: &str = "gap";

/// A structure combining labels and the associated ɣ for serialization.
///
/// This structure is used by [`layered_label_propagation_labels_only`] to store
/// the labels for each ɣ on disk using [ε-serde].
///
/// [ε-serde]: epserde
#[derive(Epserde, Debug, Clone)]
pub struct LabelsAndGamma<A> {
    pub gamma: f64,
    pub labels: A,
}

/// Runs layered label propagation on the provided symmetric graph and returns
/// the resulting labels.
///
/// Note that no symmetry check is performed, but in that case the algorithm
/// usually will not give satisfactory results.
///
/// # Arguments
///
/// * `sym_graph` - The symmetric graph to run LLP on.
///
/// * `deg_cumul` - The degree cumulative distribution of the graph, as in
///   [`par_apply`].
///
/// * `gammas` - The ɣ values to use in the LLP algorithm.
///
/// * `chunk_size` - The chunk size used to randomize the permutation. This is
///   an advanced option: see
///   [`par_apply`].
///
/// * `granularity` - The granularity of the parallel processing.
///   This is an advanced option: see
///   [`par_apply`].
///
/// * `seed` - The seed to use for pseudorandom number generation.
///
/// * `predicate` - The stopping criterion for the iterations of the algorithm.
///
/// * `func_perm_gen` - A generator for the functional permutation
///   used in each iteration of the algorithm, given the number of
///   nodes and two random seeds. The typical intended usage is
///
///   ```ignore
///   |n: usize, s0: u64, s1: u64| {
///       let funcperm = funcperm::murmur(n as u64, s0, s1);
///       move |x| funcperm.get(x)
///   }
///   ```
///
///   which will use a MurmurHash-based functional permutation
///   from the [`funcperm`] crate, but you can use other techniques
///   as long as two `u64`s are sufficient for initialization.
///   You can pass
///
///   ```ignore
///   |_: usize, _: u64, _: u64| |x: u64| x
///   ```
///
///   to get the identity permutation (i.e., no permutation at all).
///
/// * `work_dir` - The directory where the labels will be stored.
///
/// [`par_apply`]: webgraph::traits::SequentialLabeling::par_apply
/// [`funcperm`]: funcperm
#[allow(clippy::too_many_arguments)]
pub fn layered_label_propagation<R: RandomAccessGraph + Sync, F: Fn(u64) -> u64 + Send + Sync>(
    sym_graph: R,
    deg_cumul: &(impl for<'a> Succ<Input = u64, Output<'a> = u64> + IndexedSeq + Send + Sync),
    gammas: Vec<f64>,
    granularity: Granularity,
    seed: u64,
    predicate: impl Predicate<preds::PredParams>,
    func_perm_gen: impl Fn(usize, u64, u64) -> F,
    work_dir: impl AsRef<Path>,
) -> Result<Box<[usize]>> {
    // compute the labels
    layered_label_propagation_labels_only(
        sym_graph,
        deg_cumul,
        gammas,
        granularity,
        seed,
        predicate,
        func_perm_gen,
        &work_dir,
    )?;
    // merge them
    combine_labels(work_dir)
}

/// Computes and stores on disk the labels for the given gammas, but
/// does not combine them. For the arguments look at
/// [`layered_label_propagation`].
///
/// Labels are stored with [ε-serde] as a [`LabelsAndGamma`] struct, which contains
/// the gamma and the labels array, with name `labels_{gamma_index}.bin`, where
/// `gamma_index` is the index of the gamma in the `gammas` vector. The log-gap
/// cost of the resulting labels is also computed and stored with [ε-serde] as
/// an `f64` with name `labels_{gamma_index}.gap`.
///
/// # Implementation notes
///
/// The labels and gap costs are stored separately because we reuse the labels
/// as a support to compute the inverse permutation that is necessary to compute
/// the gap cost. This approach saves a large-array allocation but requires to
/// dump the labels before computing the gap cost.
///
/// [ε-serde]: epserde
#[allow(clippy::too_many_arguments)]
pub fn layered_label_propagation_labels_only<
    R: RandomAccessGraph + Sync,
    F: Fn(u64) -> u64 + Send + Sync,
>(
    sym_graph: R,
    deg_cumul: &(impl for<'a> Succ<Input = u64, Output<'a> = u64> + IndexedSeq + Send + Sync),
    gammas: Vec<f64>,
    granularity: Granularity,
    seed: u64,
    predicate: impl Predicate<preds::PredParams>,
    func_perm_gen: impl Fn(usize, u64, u64) -> F,
    work_dir: impl AsRef<Path>,
) -> Result<()> {
    // work-around to make TempDir Live as much as needed but only create it if
    // the user didn't provide a work_dir, which can also be a TempDir.
    let work_path = work_dir.as_ref();
    let labels_path = |gamma_index| work_path.join(format!("labels_{gamma_index}.bin"));
    const IMPROV_WINDOW: usize = 10;
    let num_nodes = sym_graph.num_nodes();
    let num_threads = rayon::current_num_threads();

    let mut can_change = Vec::with_capacity(num_nodes as _);
    can_change.extend((0..num_nodes).map(|_| AtomicBool::new(true)));
    let mut label_store = label_store::LabelStore::new(num_nodes as _);
    // build a thread_pool so we avoid having to re-create the threads
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .context("Could not create thread pool")?;

    // init the gamma progress logger
    let mut gamma_pl = progress_logger![
        display_memory = true,
        item_name = "gamma",
        expected_updates = Some(gammas.len()),
    ];

    // init the iteration progress logger
    let mut iter_pl = progress_logger![item_name = "update"];

    let hash_map_init = Ord::max(sym_graph.num_arcs() / sym_graph.num_nodes() as u64, 16) as usize;

    // init the update progress logger
    let mut update_pl = concurrent_progress_logger![item_name = "node", local_speed = true];

    let mut costs = Vec::with_capacity(gammas.len());

    gamma_pl.start(format!("Running {} threads", num_threads));
    info!("Stopping criterion: {predicate}");

    for (gamma_index, gamma) in gammas.iter().enumerate() {
        // Reset mutable state for the next gamma
        iter_pl.start(format!(
            "Starting iterations with gamma={} ({}/{})...",
            gamma,
            gamma_index + 1,
            gammas.len(),
        ));
        label_store.init();
        thread_pool.install(|| {
            can_change
                .par_iter()
                .with_min_len(RAYON_MIN_LEN)
                .for_each(|c| c.store(true, Ordering::Relaxed));
        });

        let mut obj_func = 0.0;
        let mut prev_gain = f64::MAX;
        let mut improv_window: VecDeque<_> = vec![1.0; IMPROV_WINDOW].into();

        let mut rand = SmallRng::seed_from_u64(seed);

        for update in 0.. {
            update_pl.expected_updates(num_nodes);
            update_pl.start(format!(
                "Starting update {} (for gamma={}, {}/{})...",
                update,
                gamma,
                gamma_index + 1,
                gammas.len()
            ));

            let func_perm = func_perm_gen(num_nodes, rand.random(), rand.random());

            // If this iteration modified anything (early stop)
            let modified = CachePadded::new(AtomicUsize::new(0));

            let delta_obj_func = sym_graph.par_apply(
                |range| {
                    let mut rand = SmallRng::seed_from_u64(range.start as u64);
                    let mut local_obj_func = 0.0;
                    for node in range {
                        let node = func_perm(node as u64) as usize;
                        // Note that here we are using a heuristic optimization:
                        // if no neighbor has changed, the label of a node
                        // cannot change. If gamma != 0, this is not necessarily
                        // true, as a node might need to change its value just
                        // because of a change of volume of the adjacent labels.
                        if !can_change[node].load(Ordering::Relaxed) {
                            continue;
                        }
                        // set that the node can't change by default and we'll unset it later if it can
                        can_change[node].store(false, Ordering::Relaxed);

                        let successors = sym_graph.successors(node);
                        if sym_graph.outdegree(node) == 0 {
                            continue;
                        }

                        // get the label of this node
                        let curr_label = label_store.label(node);

                        // compute the frequency of successor labels
                        let mut map =
                            HashMap::with_capacity_and_hasher(hash_map_init, mix64::Mix64Builder);
                        for succ in successors {
                            map.entry(label_store.label(succ))
                                .and_modify(|counter| *counter += 1)
                                .or_insert(1_usize);
                        }
                        // add the current label to the map
                        map.entry(curr_label).or_insert(0_usize);

                        let mut max = f64::NEG_INFINITY;
                        let mut old = 0.0;
                        let mut majorities = vec![];
                        // compute the most entropic label
                        for (&label, &count) in map.iter() {
                            // For replication of the results of the Java
                            // version, one needs to decrement the volume of
                            // the current value the Java version does
                            // (see the commented code below).
                            //
                            // Note that this is not exactly equivalent to the
                            // behavior of the Java version, as during the
                            // execution of this loop if another thread reads
                            // the volume of the current label it will get a
                            // value larger by one WRT the Java version.
                            let volume = label_store.volume(label); // - (label == curr_label) as usize;
                            let val = (1.0 + gamma) * count as f64 - gamma * (volume + 1) as f64;

                            if max == val {
                                majorities.push(label);
                            }

                            if val > max {
                                majorities.clear();
                                max = val;
                                majorities.push(label);
                            }

                            if label == curr_label {
                                old = val;
                            }
                        }
                        // randomly break ties
                        let next_label = *majorities.choose(&mut rand).unwrap();
                        // if the label changed we need to update the label store
                        // and signal that this could change the neighbor nodes
                        if next_label != curr_label {
                            modified.fetch_add(1, Ordering::Relaxed);
                            for succ in sym_graph.successors(node) {
                                can_change[succ].store(true, Ordering::Relaxed);
                            }
                            label_store.update(node, next_label);
                        }
                        local_obj_func += max - old;
                    }
                    local_obj_func
                },
                |delta_obj_func_0: f64, delta_obj_func_1| delta_obj_func_0 + delta_obj_func_1,
                granularity,
                deg_cumul,
                &mut update_pl,
            );

            update_pl.done_with_count(num_nodes);
            iter_pl.update_and_display();

            obj_func += delta_obj_func;
            let gain = delta_obj_func / obj_func;
            let gain_impr = (prev_gain - gain) / prev_gain;
            prev_gain = gain;
            improv_window.pop_front();
            improv_window.push_back(gain_impr);
            let avg_gain_impr = improv_window.iter().sum::<f64>() / IMPROV_WINDOW as f64;

            info!("Gain: {gain}");
            info!("Gain improvement: {gain_impr}");
            info!("Average gain improvement: {avg_gain_impr}");
            info!("Modified: {}", modified.load(Ordering::Relaxed),);

            if predicate.eval(&PredParams {
                num_nodes: sym_graph.num_nodes(),
                num_arcs: sym_graph.num_arcs(),
                gain,
                avg_gain_impr,
                modified: modified.load(Ordering::Relaxed),
                update,
            }) || modified.load(Ordering::Relaxed) == 0
            {
                break;
            }
        }

        iter_pl.done();

        // Save labels
        let (labels, volumes) = label_store.labels_and_volumes();
        // We use the volume array to compute the sorting permutation of the
        // labels.
        let perm = volumes;

        thread_pool.install(|| {
            perm.par_iter_mut()
                .with_min_len(RAYON_MIN_LEN)
                .enumerate()
                .for_each(|(i, x)| *x = i);
            // Sort by label
            perm.par_sort_unstable_by(|&a, &b| labels[a].cmp(&labels[b]).then_with(|| a.cmp(&b)));
        });

        // store the labels on disk with their cost and gamma
        let labels_store = LabelsAndGamma {
            gamma: *gamma,
            labels: &*labels,
        };

        let labels_path = labels_path(gamma_index);

        // SAFETY: any value is valid
        unsafe {
            labels_store
                .store(&labels_path)
                .context("Could not serialize labels")
        }?;

        let inv_perm = labels;

        // We temporarily use the label array from the label store to compute
        // the inverse permutation. It will be reinitialized at the next
        // iteration anyway.
        thread_pool.install(|| {
            invert_permutation(perm, inv_perm);
        });

        update_pl.expected_updates(num_nodes);
        update_pl.start("Computing log-gap cost...");

        let gap_cost = gap_cost::compute_log_gap_cost(
            PermutedGraph::new(&sym_graph, &inv_perm),
            granularity,
            deg_cumul,
            &mut update_pl,
        );

        update_pl.done();

        info!("Log-gap cost: {}", gap_cost);
        costs.push(gap_cost);

        // SAFETY: any value is valid
        unsafe {
            gap_cost
                .store(labels_path.with_extension(GAP_COST_EXTENSION))
                .context("Could not serialize gap cost")
        }?;

        gamma_pl.update_and_display();
    }

    gamma_pl.done();

    Ok(())
}

/// Combines the labels computed by LLP into a final labels array.
///
/// * `work_dir` - The folder where the labels to combine are.
pub fn combine_labels(work_dir: impl AsRef<Path>) -> Result<Box<[usize]>> {
    let mut gammas = vec![];
    let iter = std::fs::read_dir(work_dir.as_ref())?
        .filter_map(Result::ok)
        .filter(|path| {
            let path_name = path.file_name();
            let path_str = path_name.to_string_lossy();
            path_str.starts_with("labels_")
                && path_str.ends_with(".bin")
                && path.file_type().is_ok_and(|ft| ft.is_file())
        });

    let mut num_nodes = None;
    for path in iter {
        let path = work_dir.as_ref().join(path.file_name());
        // we only need the cost and gamma here, so we mmap it to ignore the
        // actual labels which will be needed only later
        let res = unsafe {
            <LabelsAndGamma<Vec<usize>>>::mmap(&path, Flags::default())
                .with_context(|| format!("Could not load labels from {}", path.to_string_lossy(),))
        }?;

        let gap_cost_path = path.with_extension(GAP_COST_EXTENSION);

        let gap_cost = unsafe {
            f64::load_full(&gap_cost_path).with_context(|| {
                format!(
                    "Could not load gap_cost from {}",
                    gap_cost_path.to_string_lossy(),
                )
            })
        }?;

        let res = res.uncase();

        info!(
            "Found labels from {:?} with gamma {} and num_nodes {}, and cost {} from {:?}",
            path.to_string_lossy(),
            res.gamma,
            res.labels.len(),
            gap_cost,
            gap_cost_path.to_string_lossy(),
        );

        match &mut num_nodes {
            num_nodes @ None => {
                *num_nodes = Some(res.labels.len());
            }
            Some(num_nodes) => {
                if res.labels.len() != *num_nodes {
                    anyhow::bail!(
                        "Labels '{}' have length {} while we expected {}.",
                        path.to_string_lossy(),
                        res.labels.len(),
                        num_nodes
                    );
                }
            }
        }
        gammas.push((gap_cost, res.gamma, path));
    }

    if gammas.is_empty() {
        anyhow::bail!("No labels were found in {}", work_dir.as_ref().display());
    }

    // temp_perm is only sorted unstably or scanned. It should work reasonably
    // even if it doesn't fit in memory.
    let temp_file =
        NamedTempFile::new().context("Could not create temporary file for combination")?;
    let mut temp_perm = MmapHelper::new(&temp_file, MmapFlags::SEQUENTIAL, num_nodes.unwrap())?;

    // compute the indices that sorts the gammas by cost
    // sort in descending order
    gammas.sort_by(|(a, _, _), (b, _, _)| b.total_cmp(a));

    // the best gamma is the last because it has the min cost
    let (best_gamma_cost, best_gamma, best_gamma_path) = gammas.last().unwrap();
    let (worst_gamma_cost, worst_gamma, _worst_gamma_path) = &gammas[0];
    info!(
        "Best gamma: {}\twith log-gap cost {}",
        best_gamma, best_gamma_cost
    );
    info!(
        "Worst gamma: {}\twith log-gap cost {}",
        worst_gamma, worst_gamma_cost
    );

    let mut result_labels = unsafe {
        <LabelsAndGamma<Vec<usize>>>::load_mem(best_gamma_path)
            .context("Could not load labels from best gamma")
    }?
    .uncase()
    .labels
    .to_vec();

    let mmap_flags = Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS;
    for (i, (cost, gamma, gamma_path)) in gammas.iter().enumerate() {
        info!(
            "Starting step {} with gamma {} cost {} and labels {:?}...",
            i, gamma, cost, gamma_path
        );
        let labels = unsafe {
            <LabelsAndGamma<Vec<usize>>>::load_mmap(gamma_path, mmap_flags)
                .context("Could not load labels")
        }?;

        combine(
            &mut result_labels,
            labels.uncase().labels,
            temp_perm.as_mut(),
        )
        .context("Could not combine labels")?;
        drop(labels); // explicit drop so we free labels before loading best_labels

        // This recombination with the best labels does not appear in the paper, but
        // it is not harmful and fixes a few corner cases in which experimentally
        // LLP does not perform well. It was introduced by Marco Rosa in the Java
        // LAW code.
        info!(
            "Recombining with gamma {} cost {} and labels {:?}...",
            best_gamma, best_gamma_cost, best_gamma_path
        );
        let best_labels = unsafe {
            <LabelsAndGamma<Vec<usize>>>::load_mmap(best_gamma_path, mmap_flags)
                .context("Could not load labels from best gamma")
        }?;
        let number_of_labels = combine(
            &mut result_labels,
            best_labels.uncase().labels,
            temp_perm.as_mut(),
        )?;
        info!("Number of labels: {}", number_of_labels);
    }

    Ok(result_labels.into_boxed_slice())
}

/// Combines the labels from two permutations into a single one.
fn combine(result: &mut [usize], labels: &[usize], temp_perm: &mut [usize]) -> Result<usize> {
    // re-init the permutation
    temp_perm
        .par_iter_mut()
        .with_min_len(RAYON_MIN_LEN)
        .enumerate()
        .for_each(|(i, x)| *x = i);
    // permute by the devilish function
    temp_perm.par_sort_unstable_by(|&a, &b| {
        (result[labels[a]].cmp(&result[labels[b]]))
            .then_with(|| labels[a].cmp(&labels[b]))
            .then_with(|| result[a].cmp(&result[b]))
            .then_with(|| a.cmp(&b)) // to make it stable
    });
    let mut prev_labels = (result[temp_perm[0]], labels[temp_perm[0]]);
    let mut curr_label = 0;
    result[temp_perm[0]] = curr_label;

    for i in 1..temp_perm.len() {
        let curr_labels = (result[temp_perm[i]], labels[temp_perm[i]]);
        if prev_labels != curr_labels {
            curr_label += 1;
            prev_labels = curr_labels
        }
        result[temp_perm[i]] = curr_label;
    }

    Ok(curr_label + 1)
}

/// Inverts a permutation.
pub fn invert_permutation(perm: &[usize], inv_perm: &mut [usize]) {
    let sync_slice = inv_perm.as_sync_slice();
    perm.par_iter()
        .with_min_len(RAYON_MIN_LEN)
        .enumerate()
        .for_each(|(i, &x)| {
            // SAFETY: each element x is accessed exactly once, so there are no data races.
            unsafe { sync_slice[x].set(i) };
        });
}

/// Computes the ranks of a slice of labels by their natural order.
pub fn labels_to_ranks(labels: &[usize]) -> Box<[usize]> {
    let mut llp_perm = (0..labels.len()).collect::<Vec<_>>().into_boxed_slice();
    llp_perm.par_sort_by(|&a, &b| labels[a].cmp(&labels[b]));
    let mut llp_inv_perm = vec![0; llp_perm.len()].into_boxed_slice();
    invert_permutation(llp_perm.as_ref(), llp_inv_perm.as_mut());
    llp_inv_perm
}
