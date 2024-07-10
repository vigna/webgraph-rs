/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::*;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use sux::prelude::*;

/// Computes the gap cost, that is, the sum of the costs of the logarithms
/// of the differences between successors.
///
/// Note that this implementation uses the _base_ of the base-2 logarithm
/// as a measure of cost, where as the Java implementation uses the _ceiling_.
pub(crate) fn compute_log_gap_cost<G: SequentialGraph + Sync>(
    graph: &G,
    arc_granularity: usize,
    deg_cumul: &(impl Succ<Input = usize, Output = usize> + Send + Sync),
    thread_pool: &rayon::ThreadPool,
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
        arc_granularity,
        deg_cumul,
        thread_pool,
        pr,
    )
}
