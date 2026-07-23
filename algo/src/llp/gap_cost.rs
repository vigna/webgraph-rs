/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use sux::traits::{IndexedSeq, Succ};
use webgraph::{traits::*, utils::Granularity};

/// Computes the gap cost, that is, the sum of the costs of the logarithms of
/// the differences between successors.
///
/// Note that this implementation uses the _floor_ of the base-2 logarithm as a
/// measure of cost, whereas the Java implementation uses the _ceiling_.
pub(crate) fn compute_log_gap_cost<G: SequentialGraph + Sync>(
    graph: G,
    granularity: Granularity,
    deg_cumul: &(impl for<'a> Succ<Input = u64, Output<'a> = u64> + IndexedSeq + Send + Sync),
    pr: &mut impl ConcurrentProgressLog,
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
                        // usize -> u128 is lossless, and the +1 cannot
                        // overflow in u128, so the log is always exact.
                        cost += ((x as u128).abs_diff(sorted[0] as u128) + 1).ilog2() as usize;
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
        granularity,
        deg_cumul,
        pr,
    )
}
