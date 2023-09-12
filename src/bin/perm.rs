use anyhow::Result;
use clap::Parser;
use dsi_progress_logger::ProgressLogger;
use epserde::Deserialize;
use tempfile::tempdir;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Performs an LLP round", long_about = None)]
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
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // TODO!: check that batchsize fits in memory, and that print the maximum
    // batch_size usable

    let graph = webgraph::graph::bvgraph::load(&args.source)?;

    let num_nodes = graph.num_nodes();
    let mut glob_pr = ProgressLogger::default().display_memory();
    glob_pr.item_name = "node";

    // read the permutation
    let perm = <Vec<usize>>::mmap(args.perm, epserde::Flags::default())?;

    let tmpdir = tempdir().unwrap();
    // create a stream where to dump the sorted pairs
    let mut sort_pairs = SortPairs::new(
        args.batch_size,
        args.tmp_dir
            .unwrap_or_else(|| tmpdir.path().to_str().unwrap().to_string()),
    )
    .unwrap();

    // dump the paris
    PermutedGraph {
        graph: &graph,
        perm: &perm,
    }
    .iter_nodes()
    .for_each(|(x, succ)| {
        succ.for_each(|s| {
            sort_pairs.push(x, s, ()).unwrap();
        })
    });
    // get a graph on the sorted data
    let edges = sort_pairs.iter()?.map(|(src, dst, _)| (src, dst));
    let g = COOIterToGraph::new(num_nodes, edges);
    // compress it
    parallel_compress_sequential_iter(
        args.dest,
        g.iter_nodes(),
        CompFlags::default(),
        args.num_cpus.unwrap_or(num_cpus::get()),
    )?;
    Ok(())
}
