use crate::traits::SequentialGraph;

pub mod bvgraph;
pub mod permuted_graph;
pub mod vec_graph;

pub mod prelude {
    pub use super::bvgraph::*;
    pub use super::permuted_graph::*;
    pub use super::vec_graph::*;
}
use core::ops::Range;
use core::sync::atomic::{AtomicUsize, Ordering};
use dsi_progress_logger::ProgressLogger;
use std::sync::Mutex;

/// Given a graph, apply `func` to each chunk of nodes of size `granularity`
/// in parallel, and reduce the results using `reduce`.
pub fn par_graph_apply<F, G, R, T>(
    graph: &G,
    func: F,
    reduce: R,
    thread_pool: &rayon::ThreadPool,
    granularity: usize,
    pr: Option<&mut ProgressLogger>,
) -> T
where
    G: SequentialGraph,
    F: Fn(Range<usize>) -> T + Send + Sync,
    R: Fn(T, T) -> T + Send + Sync,
    T: Send + Default,
{
    let pr_lock = pr.map(Mutex::new);
    let num_nodes = graph.num_nodes();
    let num_cpus = thread_pool
        .current_num_threads()
        .min(num_nodes / granularity)
        .max(1);
    let next_node = AtomicUsize::new(0);

    thread_pool.scope(|scope| {
        let mut res = Vec::with_capacity(num_cpus);
        for _ in 0..num_cpus {
            // create a channel to receive the result
            let (tx, rx) = std::sync::mpsc::channel();
            res.push(rx);

            // create some references so that we can share them across threads
            let pr_lock_ref = &pr_lock;
            let next_node_ref = &next_node;
            let func_ref = &func;
            let reduce_ref = &reduce;

            scope.spawn(move |_| {
                let mut result = T::default();
                loop {
                    // compute the next chunk of nodes to process
                    let start_pos = next_node_ref.fetch_add(granularity, Ordering::Relaxed);
                    let end_pos = (start_pos + granularity).min(num_nodes);
                    // exit if done
                    if start_pos >= num_nodes {
                        break;
                    }
                    // apply the function and reduce the result
                    result = reduce_ref(result, func_ref(start_pos..end_pos));
                    // update the progress logger if specified
                    if let Some(pr_lock) = pr_lock_ref {
                        pr_lock
                            .lock()
                            .unwrap()
                            .update_with_count((start_pos..end_pos).len());
                    }
                }
                // comunicate back that the thread finished
                tx.send(result).unwrap();
            });
        }
        // reduce the results
        let mut result = T::default();
        for rx in res {
            result = reduce(result, rx.recv().unwrap());
        }
        result
    })
}
