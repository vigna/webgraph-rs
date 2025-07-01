/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use dsi_progress_logger::ConcurrentProgressLog;
use no_break::NoBreak;
use rayon::ThreadPool;
use std::{
    mem::MaybeUninit,
    ops::ControlFlow::Continue,
    sync::atomic::{AtomicUsize, Ordering},
};
use sync_cell_slice::SyncSlice;
use webgraph::traits::RandomAccessGraph;
use webgraph::visits::{
    breadth_first::{EventNoPred, ParFairNoPred},
    Parallel,
};

/// Connected components of symmetric graphs by parallel visits.
pub fn symm_par(
    graph: impl RandomAccessGraph + Sync,
    thread_pool: &ThreadPool,
    pl: &mut impl ConcurrentProgressLog,
) -> Sccs {
    // TODO debug_assert!(check_symmetric(&graph));

    let num_nodes = graph.num_nodes();
    pl.item_name("node");
    pl.expected_updates(Some(num_nodes));
    pl.start("Computing strongly connected components...");

    let mut visit = ParFairNoPred::new(&graph);
    let mut component = Box::new_uninit_slice(num_nodes);

    let number_of_components = AtomicUsize::new(0);
    let slice = component.as_sync_slice();

    for node in 0..num_nodes {
        visit
            .par_visit_with(
                [node],
                pl.clone(),
                |pl, event| {
                    match event {
                        EventNoPred::Init { .. } => {}
                        EventNoPred::Visit { node, .. } => {
                            pl.light_update();
                            unsafe {
                                slice[node].set(MaybeUninit::new(
                                    number_of_components.load(Ordering::Relaxed),
                                ))
                            };
                        }
                        EventNoPred::Done { .. } => {
                            number_of_components.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => (),
                    }
                    Continue(())
                },
                thread_pool,
            )
            .continue_value_no_break();
    }

    let component = unsafe { component.assume_init() };

    pl.done();

    Sccs::new(number_of_components.load(Ordering::Relaxed), component)
}
