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
//! case, please use [`simplify`](webgraph::transform::simplify) to generate a
//! suitable graph.
//!
//! # Memory Requirements
//!
//! LLP requires three `usize` and a boolean per node, plus the memory that is
//! necessary to load the graph.
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
//! co-located in the graph, so [permuting the
//! graph](webgraph::transform::permute) in label order will increase locality,
//! yielding better compression.
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
//!   yielding a permutation that can be passed to
//!   [`permute`](webgraph::transform::permute).
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
use predicates::Predicate;
use preds::PredParams;

use log::info;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use sux::traits::Succ;
use sync_cell_slice::SyncSlice;
use webgraph::prelude::PermutedGraph;
use webgraph::traits::RandomAccessGraph;
use webgraph::utils::Granularity;

pub(crate) mod gap_cost;
pub(crate) mod label_store;
mod mix64;
pub mod preds;

const RAYON_MIN_LEN: usize = 100000;

/// This struct is how the labels and their metadata are stored on disk.
#[derive(Epserde, Debug, Clone)]
pub struct LabelsStore<A> {
    pub gap_cost: f64,
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
///   [par_apply](webgraph::traits::SequentialLabeling::par_apply).
///
/// * `gammas` - The ɣ values to use in the LLP algorithm.
///
/// * `chunk_size` - The chunk size used to randomize the permutation. This is
///   an advanced option: see
///   [par_apply](webgraph::traits::SequentialLabeling::par_apply).
///
/// * `granularity` - The granularity of the parallel processing.
///   This is an advanced option: see
///   [par_apply](webgraph::traits::SequentialLabeling::par_apply).
///
/// * `seed` - The seed to use for pseudorandom number generation.
///
/// * `predicate` - The stopping criterion for the iterations of the algorithm.
///
/// * `work_dir` - The directory where the labels will be stored.
#[allow(clippy::too_many_arguments)]
pub fn layered_label_propagation<R: RandomAccessGraph + Sync>(
    sym_graph: R,
    deg_cumul: &(impl for<'a> Succ<Input = usize, Output<'a> = usize> + Send + Sync),
    gammas: Vec<f64>,
    chunk_size: Option<usize>,
    granularity: Granularity,
    seed: u64,
    predicate: impl Predicate<preds::PredParams>,
    work_dir: impl AsRef<Path>,
) -> Result<Box<[usize]>> {
    // compute the labels
    layered_label_propagation_labels_only(
        sym_graph,
        deg_cumul,
        gammas,
        chunk_size,
        granularity,
        seed,
        predicate,
        &work_dir,
    )?;
    // merge them
    combine_labels(work_dir)
}

/// Computes and stores on disk the labels for the given gammas, but
/// does not combine them. For the arguments look at
/// [`layered_label_propagation`].
#[allow(clippy::too_many_arguments)]
pub fn layered_label_propagation_labels_only<R: RandomAccessGraph + Sync>(
    sym_graph: R,
    deg_cumul: &(impl for<'a> Succ<Input = usize, Output<'a> = usize> + Send + Sync),
    gammas: Vec<f64>,
    chunk_size: Option<usize>,
    granularity: Granularity,
    seed: u64,
    predicate: impl Predicate<preds::PredParams>,
    work_dir: impl AsRef<Path>,
) -> Result<()> {
    // work-around to make TempDir Live as much as needed but only create it if
    // the user didn't provide a work_dir, which can also be a TempDir.
    let work_path = work_dir.as_ref();
    let labels_path = |gamma_index| work_path.join(format!("labels_{gamma_index}.bin"));
    const IMPROV_WINDOW: usize = 10;
    let num_nodes = sym_graph.num_nodes();
    let chunk_size = chunk_size.unwrap_or(1_000_000);
    let num_threads = rayon::current_num_threads();

    // init the permutation with the indices
    let mut update_perm = (0..num_nodes).collect::<Vec<_>>();

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

    let seed = CachePadded::new(AtomicU64::new(seed));
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

        for update in 0.. {
            update_pl.expected_updates(Some(num_nodes));
            update_pl.start(format!(
                "Starting update {} (for gamma={}, {}/{})...",
                update,
                gamma,
                gamma_index + 1,
                gammas.len()
            ));

            update_perm.iter_mut().enumerate().for_each(|(i, x)| *x = i);
            thread_pool.install(|| {
                // parallel shuffle
                update_perm.par_chunks_mut(chunk_size).for_each(|chunk| {
                    let seed = seed.fetch_add(1, Ordering::Relaxed);
                    let mut rand = SmallRng::seed_from_u64(seed);
                    chunk.shuffle(&mut rand);
                });
            });

            // If this iteration modified anything (early stop)
            let modified = CachePadded::new(AtomicUsize::new(0));

            let delta_obj_func = sym_graph.par_apply(
                |range| {
                    let mut rand = SmallRng::seed_from_u64(range.start as u64);
                    let mut local_obj_func = 0.0;
                    for &node in &update_perm[range] {
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

        // We temporarily use the update permutation to compute the sorting
        // permutation of the labels.
        let perm = &mut update_perm;
        thread_pool.install(|| {
            perm.par_iter_mut()
                .with_min_len(RAYON_MIN_LEN)
                .enumerate()
                .for_each(|(i, x)| *x = i);
            // Sort by label
            perm.par_sort_unstable_by(|&a, &b| {
                label_store
                    .label(a as _)
                    .cmp(&label_store.label(b as _))
                    .then_with(|| a.cmp(&b))
            });
        });

        // Save labels
        let (labels, volumes) = label_store.labels_and_volumes();

        // We temporarily use the label array from the label store to compute
        // the inverse permutation. It will be reinitialized at the next
        // iteration anyway.
        thread_pool.install(|| {
            invert_permutation(perm, volumes);
        });

        update_pl.expected_updates(Some(num_nodes));
        update_pl.start("Computing log-gap cost...");

        let gap_cost = gap_cost::compute_log_gap_cost(
            &PermutedGraph {
                graph: &sym_graph,
                perm: &volumes,
            },
            granularity,
            deg_cumul,
            &mut update_pl,
        );

        update_pl.done();

        info!("Log-gap cost: {}", gap_cost);
        costs.push(gap_cost);

        // store the labels on disk with their cost and gamma
        let labels_store = LabelsStore {
            labels: &*labels,
            gap_cost,
            gamma: *gamma,
        };
        // SAFETY: the type is ε-serde serializable and the path is valid.
        unsafe {
            labels_store
                .store(labels_path(gamma_index))
                .context("Could not serialize labels")
        }?;

        gamma_pl.update_and_display();
    }

    gamma_pl.done();

    Ok(())
}

/// Combines the labels computed by LLP into a final labels array.
///
/// * `work_dir`: The folder where the labels to combine are.
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
            <LabelsStore<Vec<usize>>>::mmap(&path, Flags::default())
                .with_context(|| format!("Could not load labels from {}", path.to_string_lossy(),))
        }?;

        let res = res.uncase();

        info!(
            "Found labels from {:?} with gamma {} and cost {} and num_nodes {}",
            path.to_string_lossy(),
            res.gamma,
            res.gap_cost,
            res.labels.len()
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
        gammas.push((res.gap_cost, res.gamma, path));
    }

    if gammas.is_empty() {
        anyhow::bail!("No labels were found in {}", work_dir.as_ref().display());
    }
    let mut temp_perm = vec![0; num_nodes.unwrap()];

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
        <LabelsStore<Vec<usize>>>::load_mem(best_gamma_path)
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
            <LabelsStore<Vec<usize>>>::load_mmap(gamma_path, mmap_flags)
                .context("Could not load labels")
        }?;

        combine(&mut result_labels, labels.uncase().labels, &mut temp_perm)
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
            <LabelsStore<Vec<usize>>>::load_mmap(best_gamma_path, mmap_flags)
                .context("Could not load labels from best gamma")
        }?;
        let number_of_labels = combine(
            &mut result_labels,
            best_labels.uncase().labels,
            &mut temp_perm,
        )?;
        info!("Number of labels: {}", number_of_labels);
    }

    Ok(result_labels.into_boxed_slice())
}

/// Combines the labels from two permutations into a single one.
fn combine(result: &mut [usize], labels: &[usize], temp_perm: &mut [usize]) -> Result<usize> {
    // re-init the permutation
    temp_perm.iter_mut().enumerate().for_each(|(i, x)| *x = i);
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
