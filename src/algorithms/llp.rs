use crate::prelude::PermutedGraph;
use crate::{invert_in_place, traits::*};
use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
use epserde::Serialize;
use log::info;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rayon::prelude::*;
use rayon::slice::ParallelSliceMut;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

/// Write the permutation computed by the LLP algorithm inside `perm`,
/// and return the labels of said permutation.
///
/// # References
/// [Layered Label Propagation: A MultiResolution Coordinate-Free Ordering for Compressing Social Networks](https://arxiv.org/pdf/1011.5425.pdf>)
#[allow(clippy::type_complexity)]
#[allow(clippy::too_many_arguments)]
pub fn layered_label_propagation<G>(
    graph: &G,
    gammas: Vec<f64>,
    num_threads: Option<usize>,
    max_iters: usize,
    chunk_size: usize,
    granularity: usize,
    seed: u64,
) -> Result<Box<[usize]>>
where
    G: RandomAccessGraph + Sync,
{
    let num_nodes = graph.num_nodes();

    // init the permutation with the indices
    let mut update_perm = (0..num_nodes).collect::<Vec<_>>();

    let mut can_change = Vec::with_capacity(num_nodes as _);
    can_change.extend((0..num_nodes).map(|_| AtomicBool::new(true)));
    let label_store = LabelStore::new(num_nodes as _);

    // build a thread_pool so we avoid having to re-create the threads
    let num_threads = num_threads.unwrap_or_else(num_cpus::get);
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()?;

    // init the gamma progress logger
    let mut gamma_pr = ProgressLogger::default().display_memory();
    gamma_pr.item_name = "gamma";
    gamma_pr.expected_updates = Some(gammas.len());

    // init the iteration progress logger
    let mut iter_pr = ProgressLogger::default();
    iter_pr.item_name = "update";

    // init the update progress logger
    let mut update_pr = ProgressLogger::default();
    update_pr.item_name = "node";
    update_pr.local_speed = true;
    update_pr.expected_updates = Some(num_nodes);

    let seed = AtomicU64::new(seed);
    let mut costs = Vec::with_capacity(gammas.len());

    gamma_pr.start(format!("Running {} threads", num_threads));

    for (gamma_index, gamma) in gammas.iter().enumerate() {
        // Reset mutable state for the next gamma
        iter_pr.start(format!(
            "Starting iterations with gamma={} ({}/{})...",
            gamma,
            gamma_index + 1,
            gammas.len(),
        ));
        let mut obj_func = 0.0;
        label_store.init();
        can_change
            .iter()
            .for_each(|x| x.store(true, Ordering::Relaxed));

        for i in 0..max_iters {
            update_pr.start(format!("Starting update {}...", i));

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

            let delta_obj_func = crate::graph::par_graph_apply(
                graph,
                |range| {
                    let mut map = HashMap::with_capacity(1024);
                    let mut rand = SmallRng::seed_from_u64(range.start as u64);
                    let mut local_obj_func = 0.0;
                    for &node in &update_perm[range] {
                        // if the node can't change we can skip it
                        if !can_change[node].load(Ordering::Relaxed) {
                            continue;
                        }
                        // set that the node can't change by default and we'll unset later it if it can
                        can_change[node].store(false, Ordering::Relaxed);

                        let successors = graph.successors(node);
                        if successors.len() == 0 {
                            continue;
                        }

                        // get the label of this node
                        let curr_label = label_store.label(node);
                        // get the count of how many times a
                        // label appears in the successors
                        map.clear();
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
                            let volume = label_store.volume(label);
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
                            for succ in graph.successors(node) {
                                can_change[succ].store(true, Ordering::Relaxed);
                            }
                            label_store.set(node, next_label);
                        }
                        local_obj_func += max - old;
                    }
                    local_obj_func
                },
                |delta_obj_func_0, delta_obj_func_1| delta_obj_func_0 + delta_obj_func_1,
                &thread_pool,
                granularity,
                Some(&mut update_pr),
            );

            update_pr.done_with_count(num_nodes);
            iter_pr.update_and_display();

            obj_func += delta_obj_func;
            let gain = delta_obj_func / obj_func;

            info!("Gain: {}", gain);
            info!("Modified: {}", modified.load(Ordering::Relaxed),);

            if gain < 0.001 || modified.load(Ordering::Relaxed) == 0 {
                break;
            }
        }

        iter_pr.done();

        update_perm.iter_mut().enumerate().for_each(|(i, x)| *x = i);
        // create sorted clusters by contiguous labels
        update_perm.par_sort_unstable_by(|&a, &b| {
            label_store.label(a as _).cmp(&label_store.label(b as _))
        });
        invert_in_place(&mut update_perm);

        let labels =
            unsafe { std::mem::transmute::<&[AtomicUsize], &[usize]>(&label_store.labels) };

        let pgraph = PermutedGraph {
            graph,
            perm: &update_perm,
        };
        let cost = compute_log_gap_cost(&thread_pool, &pgraph, None);
        info!("Log-gap cost: {}", cost);
        costs.push(cost);

        // storing the perms
        let mut file = std::fs::File::create(format!("labels_{}.bin", gamma_index))?;
        labels.serialize(&mut file)?;

        gamma_pr.update_and_display();
    }

    gamma_pr.done();

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

    let mut result_labels =
        epserde::load_mem::<Vec<usize>>(format!("labels_{}.bin", best_gamma_index))?.to_vec();

    for (i, gamma_index) in gamma_indices
        .iter()
        .enumerate()
        .take(gamma_indices.len() - 1)
    {
        info!("Starting step {}...", i);
        let labels = epserde::load_mem::<Vec<usize>>(format!("labels_{}.bin", gamma_index))?;
        combine(&mut result_labels, *labels, &mut temp_perm)?;
        // This recombination with the best labels does not appear in the paper, but
        // it is not harmful and fixes a few corner cases in which experimentally
        // LLP does not perform well. It was introduced by Marco Rosa in the Java
        // LAW code.
        let best_labels =
            epserde::load_mem::<Vec<usize>>(format!("labels_{}.bin", best_gamma_index))?;
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
    temp_perm[0] = curr_label;

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

struct LabelStore {
    labels: Box<[AtomicUsize]>,
    volumes: Box<[AtomicUsize]>,
}

impl LabelStore {
    fn new(n: usize) -> Self {
        let mut labels = Vec::with_capacity(n);
        labels.extend((0..n).map(|_| AtomicUsize::new(0)));
        let mut volumes = Vec::with_capacity(n);
        volumes.extend((0..n).map(|_| AtomicUsize::new(0)));

        Self {
            labels: labels.into_boxed_slice(),
            volumes: volumes.into_boxed_slice(),
        }
    }

    fn init(&self) {
        for l in 0..self.labels.len() {
            self.labels[l].store(l, Ordering::Relaxed);
            self.volumes[l].store(1, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    fn set(&self, node: usize, new_label: usize) {
        let old_label = self.labels[node].swap(new_label, Ordering::Relaxed);
        self.volumes[old_label].fetch_sub(1, Ordering::Relaxed);
        self.volumes[new_label].fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    fn label(&self, node: usize) -> usize {
        self.labels[node].load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn volume(&self, label: usize) -> usize {
        self.volumes[label].load(Ordering::Relaxed)
    }
}

unsafe impl Send for LabelStore {}
unsafe impl Sync for LabelStore {}

fn compute_log_gap_cost<G: SequentialGraph + Sync>(
    thread_pool: &rayon::ThreadPool,
    graph: &G,
    pr: Option<&mut ProgressLogger>,
) -> f64 {
    crate::graph::par_graph_apply(
        graph,
        |range| {
            graph
                .iter_nodes_from(range.start)
                .take(range.len())
                .map(|(x, succ)| {
                    let mut cost = 0;
                    let mut sorted: Vec<_> = succ.collect();
                    if !sorted.is_empty() {
                        sorted.sort();
                        cost +=
                            ((x as isize - sorted[0] as isize).unsigned_abs() + 1).ilog2() as usize;
                        cost += sorted
                            .windows(2)
                            .map(|w| (w[1] - w[0]).ilog2() as usize)
                            .sum::<usize>();
                    }
                    cost
                })
                .sum::<usize>() as f64
        },
        |a, b| a + b,
        thread_pool,
        1_000,
        pr,
    )
}
