/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Layered label propagation.
//!
//! An implementation of the _layered label propagation_ algorithm described by
//! Paolo Boldi, Sebastiano Vigna, Marco Rosa, Massimo Santini, and Sebastiano
//! Vigna in “Layered label propagation: A multiresolution coordinate-free
//! ordering for compressing social networks”, _Proceedings of the 20th
//! international conference on World Wide Web_, pages 587–596, ACM, 2011.
//!
//! The function [`layered_label_propagation`] returns a permutation of the
//! provided symmetric graph which will (hopefully) increase locality (see the
//! paper). Usually, the permutation is fed to [`perm`] to permute the original
//! graph.
//!
//! Note that the graph provided should be _symmetric_ and _loopless_. If this
//! is not the case, please use [crate::transform::simplify] to generate a
//! suitable graph.
//!
//! # Memory requirements
//!
//! LLP requires three `usize` and a boolean per node, plus the memory that is
//! necessary to load the graph.
//!
use crate::prelude::*;
use crate::traits::*;
use anyhow::{Context, Result};
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use llp::preds::PredParams;
use predicates::Predicate;

use common_traits::UnsignedInt;
use log::info;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::env::temp_dir;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use sux::traits::IndexedDict;
use sux::traits::Succ;

pub(crate) mod gap_cost;
pub(crate) mod label_store;
mod mix64;
pub mod preds;

fn labels_path(gamma_index: usize) -> PathBuf {
    [temp_dir(), format!("labels_{}.bin", gamma_index).into()]
        .iter()
        .collect()
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
/// * `deg_cumul` - The degree cumulative distribution of the graph, as in
///   [par_apply](crate::traits::SequentialLabeling::par_apply).
/// * `gammas` - The ɣ values to use in the LLP algorithm.
/// * `num_threads` - The number of threads to use. If `None`, the number of
/// threads is set to [`num_cpus::get`].
/// * `chunk_size` - The chunk size used to randomize the permutation. This is
/// an advanced option: see
///   [par_apply](crate::traits::SequentialLabeling::par_apply).
/// * `granularity` - The granularity of the parallel processing expressed as
///   the number of arcs to process at a time. If `None`, the granularity is
///   computed adaptively. This is an advanced option: see
///   [par_apply](crate::traits::SequentialLabeling::par_apply).
/// * `seed` - The seed to use for pseudorandom number generation.
#[allow(clippy::type_complexity)]
#[allow(clippy::too_many_arguments)]
pub fn layered_label_propagation<R: RandomAccessGraph + Sync>(
    sym_graph: &R,
    deg_cumul: &(impl Succ<Input = usize, Output = usize> + Send + Sync),
    gammas: Vec<f64>,
    num_threads: Option<usize>,
    chunk_size: Option<usize>,
    granularity: Option<usize>,
    seed: u64,
    predicate: impl Predicate<preds::PredParams>,
) -> Result<Box<[usize]>> {
    const IMPROV_WINDOW: usize = 10;
    let num_nodes = sym_graph.num_nodes();
    let chunk_size = chunk_size.unwrap_or(1_000_000);
    let granularity = granularity.unwrap_or(((sym_graph.num_arcs() >> 9) as usize).max(1024));

    // init the permutation with the indices
    let mut update_perm = (0..num_nodes).collect::<Vec<_>>();

    let mut can_change = Vec::with_capacity(num_nodes as _);
    can_change.extend((0..num_nodes).map(|_| AtomicBool::new(true)));
    let mut label_store = label_store::LabelStore::new(num_nodes as _);
    let stack_size = std::env::var("RUST_MIN_STACK")
        .map(|value| value.parse().unwrap())
        .unwrap_or(1024 * num_nodes.ilog2_ceil() as usize);
    // build a thread_pool so we avoid having to re-create the threads
    let num_threads = num_threads.unwrap_or_else(num_cpus::get);
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .stack_size(stack_size)
        .build()
        .context("Could not create thread pool")?;

    // init the gamma progress logger
    let mut gamma_pl = progress_logger!(
        display_memory = true,
        item_name = "gamma",
        expected_updates = Some(gammas.len()),
    );

    // init the iteration progress logger
    let mut iter_pl = progress_logger!(item_name = "update");

    let hash_map_init = (sym_graph.num_arcs() / sym_graph.num_nodes() as u64).max(16) as usize;

    // init the update progress logger
    let mut update_pl = progress_logger!(item_name = "node", local_speed = true);

    let seed = AtomicU64::new(seed);
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
        can_change
            .par_iter()
            .with_min_len(1024)
            .for_each(|c| c.store(true, Ordering::Relaxed));

        let mut obj_func = 0.0;
        let mut prev_gain = f64::MAX;
        let mut improv_window: VecDeque<_> = vec![1.0; IMPROV_WINDOW].into();

        for update in 0.. {
            update_pl.expected_updates(Some(num_nodes));
            update_pl.start(format!("Starting update {}...", update));

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
            let modified = AtomicUsize::new(0);

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
                        // set that the node can't change by default and we'll unset later it if it can
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
                        // and signal that this could change the neighbour nodes
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
                &thread_pool,
                Some(&mut update_pl),
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
        perm.par_iter_mut()
            .enumerate()
            .with_min_len(1024)
            .for_each(|(i, x)| *x = i);
        // Sort by label
        perm.par_sort_by(|&a, &b| label_store.label(a as _).cmp(&label_store.label(b as _)));

        // Save labels
        let labels = label_store.labels();
        let mut file =
            std::fs::File::create(labels_path(gamma_index)).context("Could not write labels")?;
        labels
            .serialize(&mut file)
            .context("Could not serialize labels")?;

        // We temporarily use the label array from the label store to compute
        // the inverse permutation. It will be reinitialized at the next
        // iteration anyway.
        let inv_perm = labels;
        invert_permutation(perm, inv_perm);

        update_pl.expected_updates(Some(num_nodes));
        update_pl.start("Computing log-gap cost...");

        let cost = gap_cost::compute_log_gap_cost(
            &PermutedGraph {
                graph: sym_graph,
                perm: &inv_perm,
            },
            granularity,
            deg_cumul,
            &thread_pool,
            Some(&mut update_pl),
        );

        update_pl.done();

        info!("Log-gap cost: {}", cost);
        costs.push(cost);

        gamma_pl.update_and_display();
    }

    gamma_pl.done();

    // compute the indices that sorts the gammas by cost
    let mut gamma_indices = (0..costs.len()).collect::<Vec<_>>();
    // sort in descending order
    gamma_indices.sort_by(|a, b| costs[*b].total_cmp(&costs[*a]));

    // the best gamma is the last because it has the min cost
    let best_gamma_index = *gamma_indices.last().unwrap();
    let worst_gamma_index = gamma_indices[0];
    let best_gamma = gammas[best_gamma_index];
    let worst_gamma = gammas[worst_gamma_index];
    info!(
        "Best gamma: {}\twith log-gap cost {}",
        best_gamma, costs[best_gamma_index]
    );
    info!(
        "Worst gamma: {}\twith log-gap cost {}",
        worst_gamma, costs[worst_gamma_index]
    );
    // reuse the update_perm to store the final permutation
    let mut temp_perm = update_perm;

    let mut result_labels = <Vec<usize>>::load_mem(labels_path(best_gamma_index))
        .context("Could not load labels from best gammar")?
        .to_vec();

    for (i, gamma_index) in gamma_indices.iter().enumerate() {
        info!("Starting step {}...", i);
        let labels =
            <Vec<usize>>::load_mem(labels_path(*gamma_index)).context("Could not load labels")?;
        combine(&mut result_labels, *labels, &mut temp_perm).context("Could not combine labels")?;
        // This recombination with the best labels does not appear in the paper, but
        // it is not harmful and fixes a few corner cases in which experimentally
        // LLP does not perform well. It was introduced by Marco Rosa in the Java
        // LAW code.
        let best_labels = <Vec<usize>>::load_mem(labels_path(best_gamma_index))
            .context("Could not load labels from best gamma")?;
        let number_of_labels = combine(&mut result_labels, *best_labels, &mut temp_perm)?;
        info!("Number of labels: {}", number_of_labels);
        info!("Finished step {}.", i);
    }

    Ok(result_labels.into_boxed_slice())
}

/// combine the labels from two permutations into a single one
fn combine(result: &mut [usize], labels: &[usize], temp_perm: &mut [usize]) -> Result<usize> {
    // re-init the permutation
    temp_perm.iter_mut().enumerate().for_each(|(i, x)| *x = i);
    // permute by the devilish function
    temp_perm.par_sort_by(|&a, &b| {
        (result[labels[a]].cmp(&result[labels[b]]))
            .then_with(|| labels[a].cmp(&labels[b]))
            .then_with(|| result[a].cmp(&result[b]))
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

use std::cell::UnsafeCell;

#[derive(Copy, Clone)]
struct UnsafeSlice<'a, T>(&'a [UnsafeCell<T>]);
unsafe impl<'a, T: Send + Sync> Send for UnsafeSlice<'a, T> {}
unsafe impl<'a, T: Send + Sync> Sync for UnsafeSlice<'a, T> {}

impl<'a, T> UnsafeSlice<'a, T> {
    fn new(slice: &'a mut [T]) -> Self {
        #![allow(trivial_casts)]
        Self(unsafe { &*(slice as *mut [T] as *const [UnsafeCell<T>]) })
    }

    /// Writes a value to the slice at the given index.
    ///
    /// This method makes it possible to write in the slice
    /// without borrowing the slice mutably.
    ///
    /// # Safety
    ///
    /// It is UB if two threads write to the same index without
    /// synchronization.
    unsafe fn write(&self, i: usize, value: T) {
        let ptr = self.0[i].get();
        *ptr = value;
    }
}
pub fn invert_permutation(perm: &[usize], inv_perm: &mut [usize]) {
    let unsafe_slice = UnsafeSlice::new(inv_perm);
    perm.par_iter()
        .enumerate()
        .with_min_len(1024)
        .for_each(|(i, &x)| unsafe {
            unsafe_slice.write(x, i);
        });
}
