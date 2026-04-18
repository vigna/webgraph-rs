/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use lender::prelude::*;
use std::path::PathBuf;
use std::process::exit;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "symm",
    about = "Checks that a graph is symmetric (AKA undirected).",
    long_about = "Checks that a graph is symmetric (AKA undirected) by comparing it with its transpose, computed on the fly using parallel sorting. Optionally checks for the absence of loops.",
    next_line_help = true
)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    #[arg(long)]
    /// Also check that the graph has no loops (i.e., that the graph is simple).​
    pub simple: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => check_symm::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => check_symm::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn check_symm<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    let has_ef =
        std::fs::metadata(args.basename.with_extension(EF_EXTENSION)).is_ok_and(|x| x.is_file());

    if !has_ef {
        log::warn!(SEQ_PROC_WARN![], args.basename.display());
    }

    let check_simple = args.simple;

    if has_ef {
        let graph =
            webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&args.basename)
                .endianness::<E>()
                .load()?;

        // Transpose the graph in parallel and compare
        thread_pool.install(|| {
            let transposed =
                webgraph::transform::transpose_split(&graph, args.memory_usage.memory_usage, None)?;
            compare(&graph, &transposed, check_simple)
        })
    } else {
        let graph =
            webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.basename)
                .endianness::<E>()
                .load()?;

        // Transpose sequentially
        let transposed = webgraph::transform::transpose(graph, args.memory_usage.memory_usage)?;

        // Reload the graph for comparison (transpose consumed it)
        let graph =
            webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.basename)
                .endianness::<E>()
                .load()?;

        compare(&graph, &transposed, check_simple)
    }
}

/// Compares a graph with its transpose, checking for symmetry and optionally
/// for the absence of self-loops.
fn compare<G0, G1>(graph: &G0, transposed: &G1, check_simple: bool) -> Result<()>
where
    G0: SequentialGraph,
    G1: SequentialGraph,
    for<'a> G0::Lender<'a>: SortedLender,
    for<'a> G1::Lender<'a>: SortedLender,
{
    if check_simple {
        log::info!("Checking symmetry and absence of self-loops...");
    } else {
        log::info!("Checking symmetry...");
    }

    if graph.num_nodes() != transposed.num_nodes() {
        eprintln!(
            "Number of nodes differ: {} vs {}",
            graph.num_nodes(),
            transposed.num_nodes()
        );
        exit(1);
    }

    let mut g_iter = graph.iter();
    let mut t_iter = transposed.iter();

    while let Some((node, succ)) = g_iter.next() {
        let (_t_node, t_succ) = t_iter.next().expect("transposed graph has fewer nodes");

        let mut s_iter = succ.into_iter();
        let mut t_s_iter = t_succ.into_iter();

        loop {
            match (s_iter.next(), t_s_iter.next()) {
                (Some(s), Some(t)) => {
                    if check_simple && s == node {
                        eprintln!("Node {} has a self-loop", node);
                        exit(1);
                    }
                    if s != t {
                        eprintln!(
                            "Graph is not symmetric: node {} has successor {} but transpose has {}",
                            node, s, t
                        );
                        exit(1);
                    }
                }
                (None, None) => break,
                (Some(_), None) => {
                    eprintln!(
                        "Graph is not symmetric: node {} has more successors than its transpose",
                        node
                    );
                    exit(1);
                }
                (None, Some(_)) => {
                    eprintln!(
                        "Graph is not symmetric: node {} has fewer successors than its transpose",
                        node
                    );
                    exit(1);
                }
            }
        }
    }

    log::info!("Graph is symmetric.");
    if check_simple {
        log::info!("Graph has no self-loops.");
    }
    Ok(())
}
