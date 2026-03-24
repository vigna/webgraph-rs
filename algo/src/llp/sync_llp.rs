use std::{collections::VecDeque, fs::File, mem::size_of, path::Path};

use anyhow::{Context, Result};
use dsi_progress_logger::{ProgressLog, concurrent_progress_logger, progress_logger};
use epserde::{Epserde, ser::Serialize};
use lender::for_;
use log::info;
use mmap_rs::MmapFlags;
use predicates::Predicate;
use rand::{RngExt, SeedableRng, rngs::SmallRng};
use rayon::prelude::*;
use sux::traits::Succ;
use webgraph::{
    prelude::PermutedGraph,
    traits::RandomAccessGraph,
    utils::{Granularity, MmapHelper},
};

use crate::{gap_cost, invert_permutation, preds::{self, PredParams}};

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

// ---------------------------------------------------------------------------
// Bulk pwrite helpers — bypass btrfs page_mkwrite entirely
// ---------------------------------------------------------------------------

/// Chunk size for parallel pwrite operations (1M elements = 8 MB per chunk).
const PWRITE_CHUNK: usize = 1 << 20;

/// Write a contiguous `&[usize]` slice to `file` at the given element offset
/// using positional I/O (`pwrite`).  Thread-safe: each call specifies its own
/// file offset, so callers may invoke this concurrently on disjoint regions.
fn pwrite_chunk(file: &File, data: &[usize], element_offset: usize) -> Result<()> {
    use std::os::unix::fs::FileExt;
    let byte_offset = (element_offset * size_of::<usize>()) as u64;
    let bytes = unsafe {
        std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * size_of::<usize>())
    };
    file.write_all_at(bytes, byte_offset)
        .context("pwrite_chunk failed")?;
    Ok(())
}

/// Write the identity sequence `0..num_nodes` to `file` in parallel chunks,
/// then flush and evict all written pages.
fn write_identity_to_file(file: &File, num_nodes: usize) -> Result<()> {
    let n_chunks = num_nodes.div_ceil(PWRITE_CHUNK);
    (0..n_chunks).into_par_iter().try_for_each(|ci| {
        let start = ci * PWRITE_CHUNK;
        let end = (start + PWRITE_CHUNK).min(num_nodes);
        let buf: Vec<usize> = (start..end).collect();
        pwrite_chunk(file, &buf, start)
    })?;
    await_writeback_and_evict(file).context("flush after identity write")?;
    Ok(())
}

/// Fill `file` with `num_nodes` copies of `value` in parallel chunks,
/// then flush and evict all written pages.
fn write_fill_to_file(file: &File, num_nodes: usize, value: usize) -> Result<()> {
    let n_chunks = num_nodes.div_ceil(PWRITE_CHUNK);
    (0..n_chunks).into_par_iter().try_for_each(|ci| {
        let start = ci * PWRITE_CHUNK;
        let end = (start + PWRITE_CHUNK).min(num_nodes);
        let buf = vec![value; end - start];
        pwrite_chunk(file, &buf, start)
    })?;
    await_writeback_and_evict(file).context("flush after fill write")?;
    Ok(())
}

/// Write an existing `&[usize]` slice to `file` in parallel chunks,
/// then flush and evict all written pages.
fn write_slice_to_file(file: &File, data: &[usize]) -> Result<()> {
    let n_chunks = data.len().div_ceil(PWRITE_CHUNK);
    (0..n_chunks).into_par_iter().try_for_each(|ci| {
        let start = ci * PWRITE_CHUNK;
        let end = (start + PWRITE_CHUNK).min(data.len());
        pwrite_chunk(file, &data[start..end], start)
    })?;
    await_writeback_and_evict(file).context("flush after slice write")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-chunk label writer used inside the iteration inner loop
// ---------------------------------------------------------------------------

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
    Ok(())
}

// ---------------------------------------------------------------------------
// Writeback helpers
// ---------------------------------------------------------------------------

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
/// arrays (`prev_labels`, `prev_volumes`) are backed by read-only memory-mapped
/// files so the OS can page them to disk when physical RAM is scarce.  The
/// write-only array (`next_labels`) is written via positional file I/O
/// (`pwrite`), with an explicit flush-and-evict after each iteration so the
/// kernel does not keep written pages in the page cache.
///
/// All mutations to the label/volume backing files go through `pwrite`,
/// never through the mmap.  This avoids filesystem `page_mkwrite` overhead
/// (a severe bottleneck on btrfs) and keeps dirty-page accounting clean.
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
    iter_pl.display_memory(true);
    let mut update_pl = concurrent_progress_logger![item_name = "node", local_speed = true];
    update_pl.display_memory(true);

    // Allocate the backing files.  Both label files and the volumes file are
    // accessed through read-only mmaps; all writes go through pwrite.
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

    // Read-only mmap — no PROT_WRITE, no btrfs page_mkwrite overhead.
    let mmap_ro = |path: &Path| -> Result<MmapHelper<usize>> {
        MmapHelper::<usize>::mmap(path, MmapFlags::empty())
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

    // prev_labels / prev_volumes: read-only mmap (random reads during iteration)
    // next_file: File handle (pwrite during iteration, flushed+evicted after)
    let mut prev_labels = mmap_ro(&labels_a_path)?;
    let mut next_file = open_rw(&labels_b_path)?;
    let mut prev_path = labels_a_path.clone();
    let mut next_path = labels_b_path.clone();
    let mut prev_volumes = mmap_ro(&volumes_path)?;

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
        // All writes go through pwrite to avoid btrfs page_mkwrite overhead.
        info!("Resetting labels to identity...");
        let t_reset = std::time::Instant::now();
        drop(prev_labels);
        {
            let f = open_rw(&prev_path)?;
            write_identity_to_file(&f, num_nodes)
                .context("Could not write identity labels")?;
        }
        prev_labels = mmap_ro(&prev_path)?;
        info!("Reset labels in {:.1}s", t_reset.elapsed().as_secs_f64());

        info!("Resetting volumes to 1...");
        let t_reset = std::time::Instant::now();
        drop(prev_volumes);
        {
            let f = open_rw(&volumes_path)?;
            write_fill_to_file(&f, num_nodes, 1)
                .context("Could not write unit volumes")?;
        }
        prev_volumes = mmap_ro(&volumes_path)?;
        info!("Reset volumes in {:.1}s", t_reset.elapsed().as_secs_f64());

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

            let is_first_update = update == 0;
            let (delta_obj_func, modified) = sym_graph.par_apply(
                |range| {
                    let mut rand = SmallRng::seed_from_u64(range.start as u64);
                    let mut local_obj_func = 0.0;
                    let mut modified = 0_usize;

                    // Small fixed-size buffer for batching pwrite calls.
                    const WRITE_BUF_CAP: usize = 8192;
                    let mut write_buf: Vec<usize> = Vec::with_capacity(WRITE_BUF_CAP);
                    let mut write_offset = range.start;

                    // label_buf is only needed for updates > 0 where
                    // labels have clustered.  On update 0 (identity
                    // labels, volumes all 1) every neighbor scores
                    // identically so we reservoir-sample with O(1) memory.
                    let mut label_buf: Vec<usize> = Vec::new();

                    for_![(node, successors) in sym_graph.iter_from(range.start).take(range.len() as usize) {
                        let curr_label = prev_labels_ref[node];

                        let (next_label, delta) = if is_first_update {
                            // All labels are identity, all volumes are 1.
                            // Every neighbor's score = (1+γ)·1 − γ·(1+1) = 1−γ.
                            // Current label (not a neighbor in loopless
                            // graphs) scores (1+γ)·0 − γ·2 = −2γ, or (1−γ)
                            // if it is a neighbor (self-loop).
                            //
                            // Reservoir-sample a random successor.  O(1)
                            // memory — no label_buf, no sorting.
                            let mut best = curr_label;
                            let mut n_seen: u32 = 0;
                            let mut curr_is_neighbor = false;
                            for succ in successors {
                                n_seen += 1;
                                if succ == node { curr_is_neighbor = true; }
                                if rand.random_range(0..n_seen) == 0 {
                                    best = prev_labels_ref[succ];
                                }
                            }
                            if n_seen == 0 {
                                // Isolated node — keep current label.
                                (curr_label, 0.0)
                            } else {
                                // old = score of current label
                                let old = if curr_is_neighbor {
                                    1.0 - gamma  // count=1, volume=1
                                } else {
                                    -2.0 * gamma  // count=0, volume=1
                                };
                                (best, (1.0 - gamma) - old)
                            }
                        } else {
                            // Labels have clustered — use sorted Vec to
                            // count distinct labels.  Vec uses 8 bytes/entry
                            // vs HashMap's ~50.
                            label_buf.clear();
                            for succ in successors {
                                label_buf.push(prev_labels_ref[succ]);
                            }
                            label_buf.sort_unstable();

                            let mut max = f64::NEG_INFINITY;
                            let mut old = 0.0;
                            let mut best_label: usize = curr_label;
                            let mut tie_count: u32 = 0;
                            let mut curr_seen = false;

                            let mut i = 0;
                            while i < label_buf.len() {
                                let label = label_buf[i];
                                let mut count: usize = 0;
                                while i < label_buf.len() && label_buf[i] == label {
                                    count += 1;
                                    i += 1;
                                }
                                if label == curr_label { curr_seen = true; }
                                let volume = prev_volumes_ref[label];
                                let val =
                                    (1.0 + gamma) * count as f64 - gamma * (volume + 1) as f64;
                                if val > max {
                                    max = val;
                                    best_label = label;
                                    tie_count = 1;
                                } else if val == max {
                                    tie_count += 1;
                                    if rand.random_range(0..tie_count) == 0 {
                                        best_label = label;
                                    }
                                }
                                if label == curr_label { old = val; }
                            }

                            // Ensure the current label is considered even when
                            // no neighbor carries it (count = 0).
                            if !curr_seen {
                                let volume = prev_volumes_ref[curr_label];
                                let val = -gamma * (volume + 1) as f64;
                                if val > max {
                                    max = val;
                                    best_label = curr_label;
                                } else if val == max {
                                    tie_count += 1;
                                    if rand.random_range(0..tie_count) == 0 {
                                        best_label = curr_label;
                                    }
                                }
                                old = val;
                            }

                            // Release inflated buffers from hub nodes.
                            const LABEL_BUF_SHRINK: usize = 1 << 20;
                            if label_buf.capacity() > LABEL_BUF_SHRINK {
                                label_buf.shrink_to(0);
                            }

                            (best_label, max - old)
                        };

                        write_buf.push(next_label);
                        if write_buf.len() == WRITE_BUF_CAP {
                            write_label_chunk(&next_file, &write_buf, write_offset)
                                .expect("Could not write labels chunk");
                            write_offset += WRITE_BUF_CAP;
                            write_buf.clear();
                        }
                        if next_label != curr_label {
                            modified += 1;
                        }
                        local_obj_func += delta;
                    }];

                    // Flush remaining labels.
                    if !write_buf.is_empty() {
                        write_label_chunk(&next_file, &write_buf, write_offset)
                            .expect("Could not write labels chunk");
                    }

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
            info!("Flushing next_labels to disk...");
            let t_flush = std::time::Instant::now();
            await_writeback_and_evict(&next_file)
                .context("Could not flush next_labels for swap")?;
            info!("Flushed next_labels in {:.1}s", t_flush.elapsed().as_secs_f64());

            // Swap: the file that was next_labels becomes prev_labels (mmap it
            // for random reads) and the old prev_labels file becomes the new
            // next_labels (open it for pwrite).
            drop(prev_labels);
            drop(next_file);
            prev_labels = mmap_ro(&next_path)
                .context("Could not mmap labels for swap")?;
            next_file = open_rw(&prev_path)
                .context("Could not re-open labels file for swap")?;
            std::mem::swap(&mut prev_path, &mut next_path);

            // Recompute volumes: sequential scan of prev_labels into a heap
            // buffer, then pwrite the result to the volumes file.  This
            // avoids btrfs page_mkwrite overhead from mmap writes.
            info!("Recomputing volumes...");
            let t_vol = std::time::Instant::now();
            #[cfg(unix)]
            unsafe {
                madvise_slice(prev_labels.as_ref(), libc::MADV_SEQUENTIAL);
            }
            let mut vol_buf = vec![0usize; num_nodes];
            for &label in prev_labels.as_ref().iter() {
                vol_buf[label] += 1;
            }
            drop(prev_volumes);
            {
                let f = open_rw(&volumes_path)?;
                write_slice_to_file(&f, &vol_buf)
                    .context("Could not write recomputed volumes")?;
            }
            drop(vol_buf);
            prev_volumes = mmap_ro(&volumes_path)?;
            info!("Recomputed volumes in {:.1}s", t_vol.elapsed().as_secs_f64());
        }

        iter_pl.done();

        // --- Post-convergence: flush and mmap next_labels for read access ---
        info!("Flushing next_labels for post-convergence...");
        let t_flush = std::time::Instant::now();
        await_writeback_and_evict(&next_file)
            .context("Could not flush next_labels for post-convergence")?;
        drop(next_file);
        let next_labels_mmap = mmap_ro(&next_path)
            .context("Could not mmap next_labels for post-convergence")?;
        info!("Flushed in {:.1}s", t_flush.elapsed().as_secs_f64());

        // --- Sort permutation ---
        //
        // Compute a permutation that sorts nodes by their label.  This is
        // done in a heap Vec to avoid writing through the mmap.  The result
        // is written to the volumes file (repurposed as scratch) via pwrite.
        info!("Computing sort permutation...");
        let t_sort = std::time::Instant::now();
        #[cfg(unix)]
        unsafe {
            madvise_slice(next_labels_mmap.as_ref(), libc::MADV_RANDOM);
        }
        let mut perm = vec![0usize; num_nodes];
        let next_labels_ref = next_labels_mmap.as_ref();
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
        info!("Sorted permutation in {:.1}s", t_sort.elapsed().as_secs_f64());

        // Persist the permutation to the volumes file.
        info!("Writing permutation to disk...");
        let t_write = std::time::Instant::now();
        drop(prev_volumes);
        {
            let f = open_rw(&volumes_path)?;
            write_slice_to_file(&f, &perm)
                .context("Could not write sort permutation")?;
        }
        drop(perm);
        prev_volumes = mmap_ro(&volumes_path)?;
        info!("Wrote permutation in {:.1}s", t_write.elapsed().as_secs_f64());

        // --- Inverse permutation ---
        //
        // Compute the inverse of the sorting permutation into a heap Vec,
        // then write it to the prev_labels file via pwrite.
        info!("Computing inverse permutation...");
        let t_inv = std::time::Instant::now();
        #[cfg(unix)]
        unsafe {
            madvise_slice(prev_volumes.as_ref(), libc::MADV_SEQUENTIAL);
        }
        let perm_ref = prev_volumes.as_ref();
        let mut inv = vec![0usize; num_nodes];
        thread_pool.install(|| {
            invert_permutation(perm_ref, &mut inv);
        });
        // Write inverse perm to prev_labels file (will be reinitialized
        // at the start of the next gamma anyway).
        drop(prev_labels);
        {
            let f = open_rw(&prev_path)?;
            write_slice_to_file(&f, &inv)
                .context("Could not write inverse permutation")?;
        }
        drop(inv);
        prev_labels = mmap_ro(&prev_path)?;
        info!("Computed inverse permutation in {:.1}s", t_inv.elapsed().as_secs_f64());

        // --- Log-gap cost ---
        info!("Computing log-gap cost...");
        let t_gap = std::time::Instant::now();
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
        info!("Log-gap cost: {} ({:.1}s)", gap_cost, t_gap.elapsed().as_secs_f64());
        costs.push(gap_cost);

        // --- Serialize labels ---
        info!("Serializing labels for gamma {}...", gamma);
        let t_ser = std::time::Instant::now();
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
        info!("Serialized labels in {:.1}s", t_ser.elapsed().as_secs_f64());

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
