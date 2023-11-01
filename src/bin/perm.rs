/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use dsi_progress_logger::*;
use epserde::prelude::*;
use lender::*;
use std::io::{BufReader, Read};
use tempfile::tempdir;
use webgraph::graph::arc_list_graph;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Permutes a graph", long_about = None)]
struct Args {
    /// The basename of the source graph.
    source: String,
    /// The basename of the destination graph.
    dest: String,
    /// The permutation.
    perm: String,

    /// How many triples to sort at once and dump on a file.
    #[arg(short, long, default_value_t = 1_000_000_000)]
    batch_size: usize,

    #[arg(short = 'j', long)]
    /// The number of cores to use
    num_cpus: Option<usize>,

    /// The directory where to put the temporary files needed to sort the paris
    /// this defaults to the system temporary directory as specified by the
    /// enviroment variable TMPDIR
    #[arg(short, long)]
    tmp_dir: Option<String>,

    #[arg(short = 'e', long, default_value_t = false)]
    /// Load the permutation from ε-serde format.
    epserde: bool,

    #[arg(short = 'o', long, default_value_t = false)]
    /// Build the offsets while compressing the graph .
    build_offsets: bool,
}

fn permute(
    args: Args,
    graph: &impl SequentialGraph,
    perm: &[usize],
    num_nodes: usize,
) -> Result<()> {
    let mut glob_pl = ProgressLogger::default();
    glob_pl.display_memory(true).item_name("node");

    let tmpdir = tempdir().unwrap();
    // create a stream where to dump the sorted pairs
    let mut sort_pairs = SortPairs::new(
        args.batch_size,
        args.tmp_dir
            .unwrap_or_else(|| tmpdir.path().to_str().unwrap().to_string()),
    )
    .unwrap();

    // dump the paris
    PermutedGraph { graph, perm }.iter().for_each(|(x, succ)| {
        succ.into_iter().for_each(|s| {
            sort_pairs.push(x, s).unwrap();
        })
    });
    // get a graph on the sorted data
    let edges = sort_pairs.iter()?.map(|(src, dst, _)| (src, dst));
    let g = arc_list_graph::ArcListGraph::new(num_nodes, edges);
    // compress it
    parallel_compress_sequential_iter::<
        &arc_list_graph::ArcListGraph<std::iter::Map<KMergeIters<_>, _>>,
    >(
        args.dest,
        &g,
        g.num_nodes(),
        CompFlags::default(),
        args.num_cpus.unwrap_or(num_cpus::get()),
    )?;

    Ok(())
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let mut glob_pl = ProgressLogger::default();
    glob_pl.display_memory(true).item_name("node");
    glob_pl.start("Permuting the graph...");
    // TODO!: check that batchsize fits in memory, and that print the maximum
    // batch_size usable

    let graph = webgraph::graph::bvgraph::load_seq(&args.source)?;

    let num_nodes = graph.num_nodes();
    // read the permutation

    if args.epserde {
        let perm = <Vec<usize>>::mmap(&args.perm, Flags::default())?;
        permute(args, &graph, perm.as_ref(), num_nodes)?;
    } else {
        let mut file = BufReader::new(std::fs::File::open(&args.perm)?);
        let mut perm = Vec::with_capacity(num_nodes);
        let mut buf = [0; core::mem::size_of::<usize>()];

        let mut perm_pl = ProgressLogger::default();
        perm_pl.display_memory(true).item_name("node");

        for _ in 0..num_nodes {
            file.read_exact(&mut buf)?;
            perm.push(usize::from_be_bytes(buf));
            perm_pl.light_update();
        }
        perm_pl.done();
        permute(args, &graph, perm.as_ref(), num_nodes)?;
    }
    glob_pl.done();
    Ok(())
}
