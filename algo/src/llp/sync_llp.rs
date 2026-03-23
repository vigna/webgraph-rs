use std::{collections::{HashMap, VecDeque}, fs::File, mem::size_of, path::Path};

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

/// Flush all dirty pages in a MAP_SHARED mmap to their backing file, then
/// drop them from the process's page tables.
///
/// After this call the pages are clean on disk.  Future reads trigger a
/// page fault that loads the data from the file on demand.  This is
/// essential on memory-constrained systems: without it, the kernel's
/// `MemAvailable` metric drops (dirty+mapped pages are discounted), which
/// can trip earlyoom or the kernel OOM killer even though the pages are
/// theoretically reclaimable.
#[cfg(unix)]
unsafe fn flush_and_evict_mmap(slice: &[usize]) {
    let ptr = slice.as_ptr() as *mut libc::c_void;
    let len = slice.len() * size_of::<usize>();
    unsafe {
        // Block until every dirty page is written to disk.
        libc::msync(ptr, len, libc::MS_SYNC);
        // Drop the now-clean pages from the page tables.
        libc::madvise(ptr, len, libc::MADV_DONTNEED);
    }
}

const FORCE_EVICT: bool = false;

/// Write a chunk of labels to the backing file using positional I/O
/// (`pwrite`), then immediately kick off background writeback and mark the
/// pages for eviction so they don't accumulate in the page cache.
///
/// Positional writes are thread-safe: each call specifies its own offset,
/// so multiple threads can write disjoint regions concurrently without
/// locking.
fn write_label_chunk(file: &File, labels: &[usize], node_offset: usize) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    let byte_offset = (node_offset * size_of::<usize>()) as u64;
    let byte_len = (labels.len() * size_of::<usize>()) as u64;
    let bytes = unsafe {
        std::slice::from_raw_parts(
            labels.as_ptr() as *const u8,
            byte_len as usize,
        )
    };
    file.write_all_at(bytes, byte_offset)?;

    // Evict the just-written pages from the page cache immediately.
    // On Linux: start async writeback for this byte range, then tell the
    // kernel to drop the pages once they're clean.  On other Unix:
    // fadvise alone (the kernel will write back dirty pages before
    // evicting).  Without per-chunk eviction a 32 GB write-only array
    // accumulates entirely in the page cache and causes OOM on machines
    // with less RAM than the working set.
    #[cfg(unix)]
    {
        if FORCE_EVICT {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            #[cfg(target_os = "linux")]
            unsafe {
                libc::sync_file_range(
                    fd,
                    byte_offset as libc::off_t,
                    byte_len as libc::off_t,
                    libc::SYNC_FILE_RANGE_WRITE,
                );
            }
            unsafe {
                libc::posix_fadvise(
                    fd,
                    byte_offset as libc::off_t,
                    byte_len as libc::off_t,
                    libc::POSIX_FADV_DONTNEED,
                );
            }
        }
    }

    Ok(())
}

/// Kick off asynchronous writeback of dirty pages. Returns immediately.
///
/// On Linux this uses `sync_file_range(SYNC_FILE_RANGE_WRITE)` to start
/// background writeback without blocking. On other systems this is a no-op;
/// [`await_writeback_and_evict`] will fall back to a synchronous flush.
fn initiate_writeback(file: &File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::sync_file_range(
                file.as_raw_fd(),
                0,
                0,
                libc::SYNC_FILE_RANGE_WRITE,
            );
        }
    }
}

/// Wait for all writeback to complete and evict pages from the page cache.
///
/// On Linux this waits for the async writeback kicked off by
/// [`initiate_writeback`], then calls `posix_fadvise(DONTNEED)` to evict
/// the now-clean pages. On other Unix systems it falls back to a
/// synchronous `sync_data()` + `posix_fadvise(DONTNEED)`.
fn await_writeback_and_evict(file: &File) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let ret = unsafe {
            libc::sync_file_range(
                fd,
                0,
                0,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE
                    | libc::SYNC_FILE_RANGE_WRITE
                    | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        unsafe {
            libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED);
        }
        return Ok(());
    }

    // Fallback for non-Linux: synchronous flush + evict.
    #[allow(unreachable_code)]
    {
        file.sync_data()?;
        #[cfg(unix)]
        unsafe {
            use std::os::unix::io::AsRawFd;
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
        Ok(())
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
/// snapshot of labels, then applied at once (double-buffering). The read-only
/// arrays (`prev_labels`, `prev_volumes`) are backed by memory-mapped files so
/// the OS can page them to disk when physical RAM is scarce. The write-only
/// array (`next_labels`) is written via positional file I/O (`pwrite`), with an
/// explicit flush-and-evict after each iteration so the kernel does not keep
/// written pages in the page cache. This allows processing graphs whose
/// node-level metadata exceeds available memory without polluting the cache with
/// write-only data.
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

    // Allocate the backing files. prev_labels and prev_volumes are mmap-ed
    // (read randomly during iteration); next_labels is written via pwrite and
    // flushed+evicted so the kernel can free those pages immediately.
    let labels_a_path = work_path.join("_labels_a.tmp");
    let labels_b_path = work_path.join("_labels_b.tmp");
    let volumes_path = work_path.join("_volumes.tmp");
    let byte_len = num_nodes * size_of::<usize>();

    let create_sized_file = |path: &Path| -> Result<()> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("Could not create {}", path.display()))?
            .set_len(byte_len as u64)
            .with_context(|| format!("Could not extend {}", path.display()))?;
        Ok(())
    };

    let mmap_file = |path: &Path| -> Result<MmapHelper<usize, MmapMut>> {
        MmapHelper::<usize, MmapMut>::mmap_mut(path, MmapFlags::SHARED)
            .with_context(|| format!("Could not mmap {}", path.display()))
    };

    let open_rw = |path: &Path| -> Result<File> {
        File::options()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("Could not open {}", path.display()))
    };

    // Create all three backing files.
    create_sized_file(&labels_a_path)?;
    create_sized_file(&labels_b_path)?;
    create_sized_file(&volumes_path)?;

    // prev_labels: mmap (random reads during iteration)
    // next_file:   File  (pwrite during iteration, flushed+evicted after)
    let mut prev_labels = mmap_file(&labels_a_path)?;
    let mut next_file = open_rw(&labels_b_path)?;
    let mut prev_path = labels_a_path.clone();
    let mut next_path = labels_b_path.clone();
    let mut prev_volumes = mmap_file(&volumes_path)?;

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
        // Flush and evict the dirty pages afterwards so the kernel sees
        // them as reclaimable — without this, 64 GB of dirty+mapped shared
        // pages drain MemAvailable and trip earlyoom/OOM.
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
        #[cfg(unix)]
        unsafe {
            flush_and_evict_mmap(prev_labels.as_ref());
            flush_and_evict_mmap(prev_volumes.as_ref());
        }

        for update in 0.. {
            update_pl.expected_updates(Some(num_nodes));
            update_pl.start(format!(
                "Starting update {} (for gamma={}, {}/{})...",
                update,
                gamma,
                gamma_index + 1,
                gammas.len()
            ));

            // Hint the kernel about access patterns for the mmap-backed
            // arrays. next_labels is file-backed (not mmap), so no hint.
            #[cfg(unix)]
            unsafe {
                madvise_slice(prev_labels.as_ref(), libc::MADV_RANDOM);
                madvise_slice(prev_volumes.as_ref(), libc::MADV_RANDOM);
            }

            let prev_labels_ref = prev_labels.as_ref();
            let prev_volumes_ref = prev_volumes.as_ref();

            let (delta_obj_func, modified) = sym_graph.par_apply(
                |range| {
                    let mut rand = SmallRng::seed_from_u64(range.start as u64);
                    let mut local_obj_func = 0.0;
                    let mut modified = 0_usize;

                    let mut map =
                        HashMap::with_capacity_and_hasher(hash_map_init, mix64::Mix64Builder);
                    let mut majorities = vec![];
                    let mut label_buf: Vec<usize> = Vec::with_capacity(range.len());

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
                        label_buf.push(next_label);
                        if next_label != curr_label {
                            modified += 1;
                        }
                        local_obj_func += max - old;
                        map.clear();
                        majorities.clear();
                    }];

                    // Flush the chunk to disk via a single pwrite (thread-safe).
                    write_label_chunk(&next_file, &label_buf, range.start)
                        .expect("Could not write labels chunk");

                    (local_obj_func, modified)
                },
                |a, b| (a.0 + b.0, a.1 + b.1),
                granularity,
                deg_cumul,
                &mut update_pl,
            );

            // Kick off async writeback so pages start flushing to disk
            // while we compute convergence statistics below.
            initiate_writeback(&next_file);

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

            // Barrier: wait for async writeback to complete and evict
            // pages before we drop the file handle and mmap it.
            await_writeback_and_evict(&next_file)
                .context("Could not flush next_labels for swap")?;

            // Swap: the file that was next_labels becomes prev_labels (mmap it
            // for random reads) and the old prev_labels file becomes the new
            // next_labels (open it for pwrite).
            drop(prev_labels);
            drop(next_file);
            prev_labels = mmap_file(&next_path)
                .context("Could not mmap labels for swap")?;
            next_file = open_rw(&prev_path)
                .context("Could not re-open labels file for swap")?;
            std::mem::swap(&mut prev_path, &mut next_path);

            // Recompute volumes: sequential scan of prev_labels, random
            // writes to prev_volumes.  Hints help the kernel readahead
            // prev_labels and avoid useless readahead on prev_volumes.
            #[cfg(unix)]
            unsafe {
                madvise_slice(prev_labels.as_ref(), libc::MADV_SEQUENTIAL);
                madvise_slice(prev_volumes.as_ref(), libc::MADV_RANDOM);
            }
            let vols = prev_volumes.as_mut();
            vols.fill(0);
            for &label in prev_labels.as_ref().iter() {
                vols[label] += 1;
            }
            // Flush dirty volume pages so MemAvailable stays healthy.
            #[cfg(unix)]
            unsafe {
                flush_and_evict_mmap(prev_volumes.as_ref());
            }
        }

        iter_pl.done();

        // --- Post-convergence: mmap next_labels for read access ---
        //
        // The iteration loop wrote next_labels via pwrite and evicted it.
        // Now we need random reads (sort) and sequential reads (serialize),
        // so mmap the file.
        await_writeback_and_evict(&next_file).context("Could not flush next_labels for post-convergence")?;
        drop(next_file);
        let next_labels_mmap = mmap_file(&next_path)
            .context("Could not mmap next_labels for post-convergence")?;

        // Compute the sorting permutation of the labels. We repurpose the
        // volumes array as scratch space for the permutation.
        //   prev_volumes (perm): sequential init then random r/w during sort
        //   next_labels: random reads (indexed by arbitrary perm values)
        #[cfg(unix)]
        unsafe {
            madvise_slice(prev_volumes.as_ref(), libc::MADV_RANDOM);
            madvise_slice(next_labels_mmap.as_ref(), libc::MADV_RANDOM);
        }
        {
            let next_labels_ref = next_labels_mmap.as_ref();
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
            madvise_slice(next_labels_mmap.as_ref(), libc::MADV_SEQUENTIAL);
        }
        let labels_store = LabelsStore {
            labels: next_labels_mmap.as_ref(),
            gamma: *gamma,
            gap_cost,
        };
        // SAFETY: the type is ε-serde serializable and the path is valid.
        unsafe {
            labels_store
                .store(&labels_path(gamma_index))
                .context("Could not serialize labels")
        }?;

        // Done with post-convergence reads. Re-open the file for the next
        // gamma's pwrite iterations.
        drop(next_labels_mmap);
        next_file = open_rw(&next_path)
            .context("Could not re-open next_labels for next gamma")?;

        gamma_pl.update_and_display();
    }

    gamma_pl.done();

    // Clean up backing files.
    drop(prev_labels);
    drop(next_file);
    drop(prev_volumes);
    let _ = std::fs::remove_file(labels_a_path);
    let _ = std::fs::remove_file(labels_b_path);
    let _ = std::fs::remove_file(volumes_path);

    Ok(())
}
