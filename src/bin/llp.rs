use anyhow::Result;
use clap::Parser;

use dsi_progress_logger::ProgressLogger;
use log::info;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rayon::slice::ParallelSliceMut;
use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;
use std::thread;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Performs an LLP round", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
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

    fn set(&self, node: usize, new_label: usize) {
        let old_label = self.labels[node].swap(new_label, Relaxed);
        self.volumes[old_label].fetch_sub(1, Relaxed);
        self.volumes[new_label].fetch_add(1, Relaxed);
    }

    fn label(&self, node: usize) -> usize {
        self.labels[node].load(Relaxed)
    }

    fn volume(&self, label: usize) -> usize {
        self.volumes[label].load(Relaxed)
    }

    fn labels(&self) -> &[AtomicUsize] {
        &self.labels
    }
}

unsafe impl Send for LabelStore {}
unsafe impl Sync for LabelStore {}

fn ceil_log2(x: usize) -> usize {
    if x <= 2 {
        x - 1
    } else {
        64 - (x - 1).leading_zeros() as usize
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let graph = webgraph::webgraph::load(&args.basename)?;
    let num_nodes = graph.num_nodes();
    let mut glob_pr = ProgressLogger::default().display_memory();
    glob_pr.item_name = "update";

    let mut can_change = Vec::with_capacity(num_nodes as _);
    can_change.extend((0..num_nodes).map(|_| AtomicBool::new(true)));

    let gamma = 0.0;
    let label_store = LabelStore::new(num_nodes as _);
    let mut rand = SmallRng::seed_from_u64(0);
    let mut perm = (0..num_nodes).collect::<Vec<_>>();

    glob_pr.start("Starting updates...");

    for _ in 0..100 {
        perm.chunks_mut(100000)
            .for_each(|chunk| chunk.shuffle(&mut rand));
        let mut pr = ProgressLogger::default();
        pr.item_name = "node";
        pr.local_speed = true;
        pr.expected_updates = Some(num_nodes);
        pr.start("Updating...");
        let prlock = Mutex::new(&mut pr);

        let delta = Mutex::new(0.0);
        let modified = AtomicUsize::new(0);
        let pos = AtomicUsize::new(0);
        const GRANULARITY: usize = 1000;

        thread::scope(|scope| {
            for _ in 0..num_cpus::get() {
                scope.spawn(|| {
                    let mut local_delta = 0.0;
                    let mut rand = SmallRng::seed_from_u64(0);
                    loop {
                        let next_pos = pos.fetch_add(GRANULARITY, Relaxed);
                        if next_pos >= num_nodes {
                            let mut delta = delta.lock().unwrap();
                            *delta += local_delta;
                            break;
                        }
                        let end_pos = (next_pos + GRANULARITY).min(perm.len());
                        for &node in &perm[next_pos..end_pos] {
                            if !can_change[node].load(Relaxed) {
                                continue;
                            }

                            can_change[node].store(false, Relaxed);

                            let successors = graph.successors(node);

                            if successors.len() == 0 {
                                continue;
                            }

                            let mut map = HashMap::<usize, usize>::with_capacity(successors.len());

                            let curr_label = label_store.label(node as _);

                            for succ in successors {
                                map.entry(label_store.label(succ))
                                    .and_modify(|counter| *counter += 1)
                                    .or_insert(1);
                            }

                            //map.entry(curr_label).or_insert(0);

                            let mut max = f64::MIN;
                            let mut old = 0.0;
                            let mut majorities = vec![];

                            for (&label, &freq) in map.iter() {
                                let volume = label_store.volume(label);
                                let val = (1.0 + gamma) * freq as f64 - gamma * (volume + 1) as f64;

                                if max == val {
                                    majorities.push(label);
                                }

                                if max < val {
                                    majorities.clear();
                                    max = val;
                                    majorities.push(label);
                                }

                                if label == curr_label {
                                    old = val;
                                }
                            }

                            debug_assert!(!majorities.is_empty());
                            let next_label = *majorities.choose(&mut rand).unwrap();
                            if next_label != curr_label {
                                modified.fetch_add(1, Relaxed);
                                for succ in graph.successors(node) {
                                    can_change[succ].store(true, Relaxed);
                                }

                                label_store.set(node as _, next_label);
                            }

                            local_delta += max - old;
                        }
                        prlock.lock().unwrap().update_with_count(end_pos - next_pos);
                    }
                });
            }
        });

        pr.done_with_count(num_nodes as _);
        info!(
            "Modified: {} Delta: {}",
            modified.load(Relaxed),
            delta.lock().unwrap()
        );
        glob_pr.update_and_display();
        if modified.load(Relaxed) == 0 {
            break;
        }
    }
    glob_pr.done();

    let labels = unsafe { std::mem::transmute::<&[AtomicUsize], &[u8]>(label_store.labels()) };
    std::fs::File::create(format!("{}-{}.labels", args.basename, 0))?.write_all(labels)?;

    let mut perm = (0..num_nodes).collect::<Vec<_>>();
    perm.par_sort_unstable_by(|&a, &b| label_store.label(a as _).cmp(&label_store.label(b as _)));

    PermutedGraph {
        graph: &graph,
        perm: &perm,
    }
    .iter_nodes()
    .map(|(x, succ)| {
        let mut cost = 0;
        if !succ.len() != 0 {
            let mut sorted: Vec<_> = succ.collect();
            sorted.sort();
            cost += ceil_log2((x as isize - sorted[0] as isize).unsigned_abs());
            cost += sorted
                .windows(2)
                .map(|w| ceil_log2(w[1] - w[0]))
                .sum::<usize>();
        }
        cost
    })
    .sum::<usize>();

    Ok(())
}
