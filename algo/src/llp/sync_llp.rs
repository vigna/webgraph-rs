use std::{collections::{HashMap, VecDeque}, mem::size_of, path::Path};

use anyhow::{Context, Result};
use dsi_progress_logger::{ProgressLog, concurrent_progress_logger, progress_logger};
use epserde::{Epserde, ser::Serialize};
use lender::for_;
use log::info;
use mmap_rs::{MmapFlags, MmapMut};
use predicates::Predicate;
use rand::{SeedableRng, rngs::SmallRng, seq::IndexedRandom};
use rayon::prelude::*;
use sux::traits::Succ;
use sync_cell_slice::SyncSlice;
use webgraph::{
    prelude::PermutedGraph,
    traits::RandomAccessGraph,
    utils::{Granularity, MmapHelper},
};

use crate::{gap_cost, invert_permutation, llp::mix64, preds::{self, PredParams}};

const RAYON_MIN_LEN: usize = 100000;

/// Apply `madvise` to the memory region backing an mmap-ed `usize` slice.
///
/// Failure is silently ignored — `madvise` is advisory and the kernel may
/// disregard the hint.
#[cfg(unix)]
unsafe fn madvise_slice(slice: &[usize], advice: libc::c_int) {
    unsafe {
        libc::madvise(
            slice.as_ptr() as *mut libc::c_void,
            slice.len() * size_of::<usize>(),
            advice,
        );
    }
}

/// This struct is how the labels and their metadata are stored on disk.
#[derive(Epserde, Debug, Clone)]
pub struct LabelsStore<A> {
    pub gap_cost: f64,
    pub gamma: f64,
    pub labels: A,
}

/// A synchronous version of layered label propagation.
///
/// "Synchronous" means all updates in an iteration are computed from the same
/// snapshot of labels, then applied at once (double-buffering). The three large
/// arrays (`prev_labels`, `next_labels`, `prev_volumes`) are backed by
/// memory-mapped files in `work_dir`, so the OS can page them to disk when
/// physical RAM is scarce. This allows processing graphs whose node-level
/// metadata exceeds available memory.
pub fn sync_layered_label_propagation(
    sym_graph: impl RandomAccessGraph + Sync,
    deg_cumul: &(impl for<'a> Succ<Input = u64, Output<'a> = u64> + Send + Sync),
    gammas: Vec<f64>,
    granularity: Granularity,
    predicate: impl Predicate<preds::PredParams>,
    work_dir: impl AsRef<Path>,
) -> Result<()> {
    const IMPROV_WINDOW: usize = 10;

    let work_path = work_dir.as_ref();
    let labels_path = |gamma_index| work_path.join(format!("labels_{gamma_index}.bin"));
    let num_nodes = sym_graph.num_nodes();
    let num_threads = rayon::current_num_threads();

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .with_context(|| "Could not create thread pool")?;

    let mut gamma_pl = progress_logger![
        display_memory = true,
        item_name = "gamma",
        expected_updates = Some(gammas.len()),
    ];
    let mut iter_pl = progress_logger![item_name = "update"];
    let hash_map_init = Ord::max(sym_graph.num_arcs() / sym_graph.num_nodes() as u64, 16) as usize;
    let mut update_pl = concurrent_progress_logger![item_name = "node", local_speed = true];

    // Allocate the three large arrays as mmap-backed files so the OS can
    // page them out under memory pressure.
    let labels_a_path = work_path.join("_labels_a.tmp");
    let labels_b_path = work_path.join("_labels_b.tmp");
    let volumes_path = work_path.join("_volumes.tmp");

    let create_mmap = |path: &Path| -> Result<MmapHelper<usize, MmapMut>> {
        let byte_len = num_nodes * size_of::<usize>();
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
    };

    let mut prev_labels = create_mmap(&labels_a_path)?;
    let mut next_labels = create_mmap(&labels_b_path)?;
    let mut prev_volumes = create_mmap(&volumes_path)?;

    let mut costs = Vec::with_capacity(gammas.len());

    gamma_pl.start(format!("Running {} threads", num_threads));
    info!("Stopping criterion: {predicate}");

    for (gamma_index, gamma) in gammas.iter().enumerate() {
        iter_pl.start(format!(
            "Starting iterations with gamma={} ({}/{})...",
            gamma,
            gamma_index + 1,
            gammas.len(),
        ));

        let mut obj_func = 0.0;
        let mut prev_gain = f64::MAX;
        let mut improv_window: VecDeque<_> = vec![1.0; IMPROV_WINDOW].into();

        // Reset labels to identity and volumes to 1 for each new gamma.
        prev_labels
            .as_mut()
            .par_iter_mut()
            .with_min_len(RAYON_MIN_LEN)
            .enumerate()
            .for_each(|(i, x)| *x = i);
        prev_volumes
            .as_mut()
            .par_iter_mut()
            .with_min_len(RAYON_MIN_LEN)
            .for_each(|x| *x = 1);

        for update in 0.. {
            update_pl.expected_updates(Some(num_nodes));
            update_pl.start(format!(
                "Starting update {} (for gamma={}, {}/{})...",
                update,
                gamma,
                gamma_index + 1,
                gammas.len()
            ));

            // Hint the kernel about access patterns for this iteration:
            //  - prev_labels/prev_volumes: random reads (indexed by successor
            //    node ids and label values respectively)
            //  - next_labels: sequential writes in chunks (one per node, in
            //    order within each par_apply range)
            #[cfg(unix)]
            unsafe {
                madvise_slice(prev_labels.as_ref(), libc::MADV_RANDOM);
                madvise_slice(prev_volumes.as_ref(), libc::MADV_RANDOM);
                madvise_slice(next_labels.as_ref(), libc::MADV_SEQUENTIAL);
            }

            // Obtain slice references for the parallel closure. prev_* are
            // read-only; next_labels is written via SyncSlice interior
            // mutability.
            let prev_labels_ref = prev_labels.as_ref();
            let prev_volumes_ref = prev_volumes.as_ref();
            let next_labels_sync = next_labels.as_mut().as_sync_slice();

            let (delta_obj_func, modified) = sym_graph.par_apply(
                |range| {
                    let mut rand = SmallRng::seed_from_u64(range.start as u64);
                    let mut local_obj_func = 0.0;
                    let mut modified = 0_usize;

                    let mut map =
                        HashMap::with_capacity_and_hasher(hash_map_init, mix64::Mix64Builder);
                    let mut majorities = vec![];

                    for_![(node, successors) in sym_graph.iter_from(range.start).take(range.len() as usize) {
                        let curr_label = prev_labels_ref[node];
                        for succ in successors {
                            map.entry(prev_labels_ref[succ])
                                .and_modify(|counter| *counter += 1)
                                .or_insert(1_usize);
                        }
                        map.entry(curr_label).or_insert(0_usize);

                        let mut max = f64::NEG_INFINITY;
                        let mut old = 0.0;
                        for (&label, &count) in map.iter() {
                            let volume = prev_volumes_ref[label];
                            let val =
                                (1.0 + gamma) * count as f64 - gamma * (volume + 1) as f64;

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

                        let next_label = *majorities.choose(&mut rand).unwrap();
                        // SAFETY: each node is processed by exactly one thread,
                        // so disjoint indices guarantee no data races.
                        unsafe { next_labels_sync[node].set(next_label) };
                        if next_label != curr_label {
                            modified += 1;
                        }
                        local_obj_func += max - old;
                        map.clear();
                        majorities.clear();
                    }];

                    (local_obj_func, modified)
                },
                |a, b| (a.0 + b.0, a.1 + b.1),
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
            info!("Modified: {modified}");

            if predicate.eval(&PredParams {
                num_nodes: sym_graph.num_nodes(),
                num_arcs: sym_graph.num_arcs(),
                gain,
                avg_gain_impr,
                modified,
                update,
            }) || modified == 0
            {
                break;
            }

            // Apply: swap label buffers and recompute volumes from the new
            // labels. The swap exchanges the MmapHelper structs (pointer +
            // metadata), not the underlying data — O(1).
            std::mem::swap(&mut prev_labels, &mut next_labels);
            let vols = prev_volumes.as_mut();
            vols.fill(0);
            for &label in prev_labels.as_ref().iter() {
                vols[label] += 1;
            }
        }

        iter_pl.done();

        // Compute the sorting permutation of the labels. We repurpose the
        // volumes array as scratch space for the permutation.
        //   prev_volumes (perm): sequential init then random r/w during sort
        //   next_labels: random reads (indexed by arbitrary perm values)
        #[cfg(unix)]
        unsafe {
            madvise_slice(prev_volumes.as_ref(), libc::MADV_RANDOM);
            madvise_slice(next_labels.as_ref(), libc::MADV_RANDOM);
        }
        {
            let next_labels_ref = next_labels.as_ref();
            let perm = prev_volumes.as_mut();
            thread_pool.install(|| {
                perm.par_iter_mut()
                    .with_min_len(RAYON_MIN_LEN)
                    .enumerate()
                    .for_each(|(i, x)| *x = i);
                perm.par_sort_unstable_by(|&a, &b| {
                    next_labels_ref[a]
                        .cmp(&next_labels_ref[b])
                        .then_with(|| a.cmp(&b))
                });
            });
        }

        // Compute the inverse permutation into prev_labels (which will be
        // reinitialized at the start of the next gamma anyway).
        //   prev_volumes (perm): sequential read
        //   prev_labels (inv): random writes at arbitrary indices
        #[cfg(unix)]
        unsafe {
            madvise_slice(prev_volumes.as_ref(), libc::MADV_SEQUENTIAL);
            madvise_slice(prev_labels.as_ref(), libc::MADV_RANDOM);
        }
        thread_pool.install(|| {
            invert_permutation(prev_volumes.as_ref(), prev_labels.as_mut());
        });

        update_pl.expected_updates(Some(num_nodes));
        update_pl.start("Computing log-gap cost...");

        // prev_labels is now the inverse permutation — read randomly
        // during the permuted graph traversal.
        #[cfg(unix)]
        unsafe {
            madvise_slice(prev_labels.as_ref(), libc::MADV_RANDOM);
        }
        let perm_slice = prev_labels.as_ref();
        let gap_cost = gap_cost::compute_log_gap_cost(
            &PermutedGraph {
                graph: &sym_graph,
                perm: &perm_slice,
            },
            granularity,
            deg_cumul,
            &mut update_pl,
        );

        update_pl.done();

        info!("Log-gap cost: {}", gap_cost);
        costs.push(gap_cost);

        // Serialize next_labels — sequential read.
        #[cfg(unix)]
        unsafe {
            madvise_slice(next_labels.as_ref(), libc::MADV_SEQUENTIAL);
        }
        let labels_store = LabelsStore {
            labels: next_labels.as_ref(),
            gamma: *gamma,
            gap_cost,
        };
        // SAFETY: the type is ε-serde serializable and the path is valid.
        unsafe {
            labels_store
                .store(&labels_path(gamma_index))
                .context("Could not serialize labels")
        }?;

        gamma_pl.update_and_display();
    }

    gamma_pl.done();

    // Drop the mmaps before removing their backing files.
    drop(prev_labels);
    drop(next_labels);
    drop(prev_volumes);
    let _ = std::fs::remove_file(labels_a_path);
    let _ = std::fs::remove_file(labels_b_path);
    let _ = std::fs::remove_file(volumes_path);

    Ok(())
}
