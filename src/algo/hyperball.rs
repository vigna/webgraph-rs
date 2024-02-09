/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::sync::atomic::Ordering;

use crate::traits::SequentialGraph;
use anyhow::Result;
use dsi_progress_logger::*;
use epserde::prelude::*;
use lender::prelude::*;
use sux::count::{AtomicHyperLogLogVec, HyperLogLogVec};
use sux::traits::ConvertTo;

/// Build the hyperball iterations on a given graph
pub fn hyperball<G>(
    graph: &G,
    basename: &str,
    log2_precision: usize,
    num_threads: Option<usize>,
    granularity: usize,
) -> Result<()> 
where
    G: SequentialGraph + Sync,
{
    // create the hyperloglog vector
    let num_bits = (graph.num_nodes() as f32).log2().log2().ceil() as usize;
    let frontier = AtomicHyperLogLogVec::new(num_bits, log2_precision, graph.num_nodes())?;

    // build a thread_pool so we avoid having to re-create the threads
    let num_threads = num_threads.unwrap_or_else(num_cpus::get);
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()?;

    // fill it with the initial values
    let mut pl = ProgressLogger::default();
    pl.item_name("node")
        .expected_updates(Some(graph.num_nodes()));
    pl.start("Running first iteration...");

    graph.par_apply(
        |nodes| {
            let mut iter = graph.iter_from(nodes.start).take(nodes.len());
            while let Some((src, succ)) = iter.next() {
                // for each successor
                for dst in succ {
                    frontier.insert(src, &dst, Ordering::Relaxed);
                }
            }
        }, 
        |_, _| {()}, // nothing to collect
        &thread_pool, 
        granularity, 
        Some(&mut pl)
    );
    // save it to disk
    let non_atomic: HyperLogLogVec = frontier.convert_to()?;
    non_atomic.store(format!("{}.hyperball.1", basename))?;
    let mut frontier: AtomicHyperLogLogVec = non_atomic.convert_to()?;

    let mut new_frontier = AtomicHyperLogLogVec::new(num_bits, log2_precision, graph.num_nodes())?;

    // run the transitive iterations
    let mut modified = graph.num_nodes();
    let mut iteration_num = 1;
    while modified > 0 {
        iteration_num += 1;

        let mut pl = ProgressLogger::default();
        pl.item_name("node")
            .expected_updates(Some(graph.num_nodes()));
        pl.start(&format!("Running iteration {}...", iteration_num));

        modified = graph.par_apply(
            |nodes| {
                let mut modified = 0;
                let mut iter = graph.iter_from(nodes.start).take(nodes.len());
                while let Some((src, succ)) = iter.next() {
                    // get the regs of the current node
                    let mut regs = frontier.iter_regs(src, Ordering::SeqCst).collect::<Vec<_>>();
                    let mut node_modified = false;
                    for dst in succ {
                        // keep only the biggest regs
                        regs.iter_mut()
                            .zip(frontier.iter_regs(dst, Ordering::SeqCst))
                            .for_each(|(old, new)| {
                                if new > *old {
                                    node_modified = true;
                                    *old = new;
                                }
                            });
                    }
                    if node_modified {
                        modified += 1;
                    }
                    new_frontier.from_iter(src, regs.into_iter(), Ordering::SeqCst);
                }
                modified
            }, 
            |a, b| {a + b}, // merge the modified flags
            &thread_pool, 
            granularity, 
            Some(&mut pl)
        );

        pl.done();
        log::info!("Modified nodes: {} - {}%", modified, modified as f32 / graph.num_nodes() as f32 * 100.0);

        // dump the frontier to disk and reset it
        let mut non_atomic: HyperLogLogVec = new_frontier.convert_to()?;
        non_atomic.store(format!("{}.hyperball.{}", basename, iteration_num))?;
        non_atomic.reset();
        let new_frontier_tmp: AtomicHyperLogLogVec = non_atomic.convert_to()?;
        new_frontier = new_frontier_tmp;
        core::mem::swap(&mut frontier, &mut new_frontier);
    }

    Ok(())
}
