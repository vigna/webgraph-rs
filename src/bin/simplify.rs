/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

 use anyhow::Result;
 use clap::Parser;
 use webgraph::graph::arc_list_graph;
 use webgraph::prelude::*;
 
 #[derive(Parser, Debug)]
 #[command(about = "Simplify a BVGraph, i.e. make it undirected and remove duplicates and selfloops", long_about = None)]
 struct Args {
     /// The basename of the graph.
     basename: String,
     /// The basename of the transposed graph. Defaults to `basename` + `.simple`.
     simplified: Option<String>,
 
     #[clap(flatten)]
     num_cpus: NumCpusArg,
 
     #[clap(flatten)]
     pa: PermutationArgs,
 
     #[clap(flatten)]
     ca: CompressArgs,
 }
 
 pub fn main() -> Result<()> {
    let args = Args::parse();
    let simplified = args.simplified.unwrap_or_else(|| args.basename.clone() + ".simple");

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let seq_graph = webgraph::graph::bvgraph::load_seq(&args.basename)?;

    // transpose the graph
    let sorted = webgraph::algorithms::simplify(&seq_graph, args.pa.batch_size).unwrap();
    // compress the transposed graph
    parallel_compress_sequential_iter::<
        &arc_list_graph::ArcListGraph<_>,
        _,
    >(
        simplified,
        &sorted,
        sorted.num_nodes(),
        args.ca.into(),
        args.num_cpus.num_cpus,
        temp_dir(args.pa.temp_dir),
    )
    .unwrap();

    Ok(())
 }
 