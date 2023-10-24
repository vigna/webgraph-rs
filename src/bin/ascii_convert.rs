/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use dsi_progress_logger::ProgressLogger;
use lender::*;
use webgraph::traits::SequentialGraph;

#[derive(Parser, Debug)]
#[command(about = "Dumps a graph as an COO arc list", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let seq_graph = webgraph::graph::bvgraph::load_seq(&args.basename)?;
    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "offset";
    pr.start("Computing offsets...");

    let mut iter = seq_graph.iter();
    while let Some((node_id, successors)) = iter.next() {
        println!(
            "{}\t{}",
            node_id,
            successors
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join("\t")
        );
    }

    pr.done();

    Ok(())
}
