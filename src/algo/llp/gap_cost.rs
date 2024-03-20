
use crate::traits::*;
use sux::prelude::*;
use dsi_progress_logger::prelude::*;


pub fn compute_log_gap_cost<G: SequentialGraph + Sync>(
    thread_pool: &rayon::ThreadPool,
    graph: &G,
    deg_cumul: &(impl Succ<Input = usize, Output = usize> + Send + Sync),
    pr: Option<&mut ProgressLogger>,
) -> f64 {
    graph.par_apply(
        |range| {
            graph
                .iter_from(range.start)
                .take(range.len())
                .map_into_iter(|(x, succ)| {
                    let mut cost = 0;
                    let mut sorted: Vec<_> = succ.into_iter().collect();
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
        deg_cumul,
        pr,
    )
}