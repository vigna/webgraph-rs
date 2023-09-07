use crate::prelude::PermutedGraph;
use crate::traits::*;
use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
use epserde::*;
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
    num_cpus: Option<usize>,
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
    let num_cpus = num_cpus.unwrap_or_else(num_cpus::get);
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus)
        .build()?;

    // init the progress logger
    let mut iter_pr = ProgressLogger::default().display_memory();
    iter_pr.item_name = "update";
    iter_pr.start("Starting updates...");

    let mut graph_pr = ProgressLogger::default();
    graph_pr.item_name = "node";
    graph_pr.local_speed = true;
    graph_pr.expected_updates = Some(num_nodes);

    let seed = AtomicU64::new(seed);
    let mut costs = Vec::with_capacity(gammas.len());
    for (gamma_index, gamma) in gammas.iter().enumerate() {
        let mut prev_obj_func = 0.0;
        for _ in 0..max_iters {
            thread_pool.install(|| {
                // parallel shuffle using the num_cpus
                update_perm.par_chunks_mut(chunk_size).for_each(|chunk| {
                    let seed = seed.fetch_add(1, Ordering::Relaxed);
                    let mut rand = SmallRng::seed_from_u64(seed);
                    chunk.shuffle(&mut rand);
                });
            });

            graph_pr.start("Updating...");

            // If this iteration modified anything (early stop)
            let modified = AtomicUsize::new(0);

            let obj_func = crate::graph::par_graph_apply(
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
                |local_obj_func_0, local_obj_func_1| local_obj_func_0 + local_obj_func_1,
                &thread_pool,
                granularity,
                Some(&mut graph_pr),
            );

            let gain = 1.0 - (prev_obj_func / (prev_obj_func + obj_func));
            info!(
                "Modified: {} Gain: {} PObjFunc: {} ObjFunc: {}",
                modified.load(Ordering::Relaxed),
                gain,
                prev_obj_func,
                obj_func,
            );
            prev_obj_func += obj_func;
            graph_pr.done_with_count(num_nodes);
            iter_pr.update_and_display();

            if modified.load(Ordering::Relaxed) == 0 {
                break;
            }
            if gain < 0.001 {
                break;
            }
        }

        iter_pr.done();

        // create sorted clusters by contiguous labels
        update_perm.par_sort_unstable_by(|&a, &b| {
            label_store.label(a as _).cmp(&label_store.label(b as _))
        });

        let labels =
            unsafe { std::mem::transmute::<&[AtomicUsize], &[usize]>(&label_store.labels) };

        let pgraph = PermutedGraph {
            graph: graph,
            perm: &update_perm,
        };
        let cost = compute_log_gap_cost(&thread_pool, &pgraph, None);
        info!("Gamma: {} Log gap cost: {}", gamma, cost);
        costs.push(cost);

        // storing the perms
        let mut file = std::fs::File::create(format!("labels_{}.bin", gamma_index))?;
        labels.to_vec().serialize(&mut file)?; // TODO!: REMOVE to_vec
    }

    // compute the indices that sorts the gammas by cost
    let mut indices = (0..costs.len()).collect::<Vec<_>>();
    // sort in descending order
    indices.sort_by(|a, b| costs[*b].total_cmp(&costs[*a]));

    // the best gamma is the last because it has the min cost
    let best_gamma_index = *indices.last().unwrap();
    let best_gamma = gammas[best_gamma_index];
    info!("Best gamma: {}", best_gamma);
    // reuse the update_perm to store the final permutation
    let mut temp_perm = update_perm;

    let mut result_labels =
        load::<Vec<usize>>(format!("labels_{}.bin", best_gamma_index))?.to_vec();
    for index in indices {
        let labels = load::<Vec<usize>>(format!("labels_{}.bin", index))?;
        combine(&mut result_labels, *labels, &mut temp_perm)?;
        let best_labels = load::<Vec<usize>>(format!("labels_{}.bin", best_gamma_index))?;
        combine(&mut result_labels, *best_labels, &mut temp_perm)?;
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
        let mut volumes = Vec::with_capacity(n);
        for l in 0..n {
            labels.push(AtomicUsize::new(l));
            volumes.push(AtomicUsize::new(1));
        }
        Self {
            labels: labels.into_boxed_slice(),
            volumes: volumes.into_boxed_slice(),
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
    let cost = crate::graph::par_graph_apply(
        graph,
        |range| {
            let res = graph
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
                .sum::<usize>() as f64;
            res
        },
        |a, b| a + b,
        thread_pool,
        1_000,
        pr,
    );
    cost
}
