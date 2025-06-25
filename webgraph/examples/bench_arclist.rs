/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */


use anyhow::Result;
use clap::Parser;
use dsi_bitstream::traits::BE;
use dsi_progress_logger::prelude::*;
use lender::*;
use tempfile::NamedTempFile;
use webgraph::graphs::arc_list_graph::ArcListGraph;
use std::{hint::black_box, path::PathBuf};
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Tests the merge speed of SortPairs", long_about = None)]
struct Args {
    basename: PathBuf,

    /// if true, the benchmark compressed the graph, otherwise it only visits it
    #[arg(short = 'c', long, default_value_t = false)]
    comp: bool,
}

pub fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;
    let args = Args::parse();

    let graph = BvGraph::with_basename(&args.basename).load()?;
    log::info!("Loaded Graph with {} nodes and {} arcs", graph.num_nodes(), graph.num_arcs());

    // loads the full arc list in memory, don't use a big graph
    let mut arcs = Vec::with_capacity(graph.num_arcs() as usize);
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("arcs")
        .expected_updates(Some(graph.num_arcs() as _));
    pl.start("Building sorted list...");
    for_!( (src, succ) in graph.iter() {
        for dst in succ {
            arcs.push((src, dst));
            pl.light_update();
        }
    });
    pl.done();
    log::info!("Built sorted list");

    let arclist = Left(ArcListGraph::new(graph.num_nodes(), arcs.iter().copied()));

    pl.start("Start Benchmark");
    let start = std::time::Instant::now();

    if args.comp {
        let temp_file = NamedTempFile::new()?;
        log::info!("Temporary file created at: {}", temp_file.path().display());
        BvComp::single_thread::<BE, _>(
            temp_file.path(),
            &arclist,
            CompFlags::default(),
            false,
            Some(arclist.num_nodes()),
        )?;
    } else {
        for_!( (src, succ) in arclist.iter() {
            for dst in succ {
                black_box((src, dst)); // Just to ensure we are doing something with the data
            }
            pl.light_update();
        });
    }

    let elapsed = start.elapsed();
    pl.done();
    log::info!("Elapsed time: {:?}", elapsed);

    Ok(())
}
