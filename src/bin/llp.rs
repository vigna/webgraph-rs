use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use std::{io::prelude::*, vec};
use webgraph::{invert_in_place, prelude::*};

#[derive(Parser, Debug)]
#[command(about = "Performs an LLP round", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,

    #[arg(short, long, default_value_t = 100)]
    /// The maximum number of LLP iterations
    max_iters: usize,

    #[arg(short = 'r', long, default_value_t = 1000)]
    /// The size of the chunks each thread processes for the LLP
    granularity: usize,

    #[arg(short, long, default_value_t = 100000)]
    /// The size of the cnunks each thread processes for the random permutation
    /// at the start of each iteration
    chunk_size: usize,

    #[arg(short, long)]
    /// The gamma to use in LLP
    gammas: Vec<f64>,

    #[arg(short = 'j', long)]
    /// The number of cores to use
    num_cpus: Option<usize>,

    #[arg(short, long, default_value_t = 0)]
    /// The seed to use for the prng
    seed: u64,
}

pub fn main() -> Result<()> {
    let start = std::time::Instant::now();
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // load the graph
    let graph = webgraph::graph::bvgraph::load(&args.basename)?;

    // compute the LLP
    let labels = layered_label_propagation(
        &graph,
        args.gammas,
        args.num_cpus,
        args.max_iters,
        args.chunk_size,
        args.granularity,
        0,
    )?;

    let mut llp_perm = (0..graph.num_nodes()).collect::<Vec<_>>();
    llp_perm.par_sort_unstable_by(|&a, &b| labels[a].cmp(&labels[b]));
    invert_in_place(llp_perm.as_mut_slice());

    let mut seen = vec![false; graph.num_nodes()];
    for &node in llp_perm.iter() {
        if seen[node] {
            panic!("");
        }
        seen[node] = true;
    }

    log::info!("Elapsed: {}", start.elapsed().as_secs_f64());
    // dump the labels
    // TODO!: This can be done better maybe
    let mut file = std::fs::File::create(format!("{}.llp.order", args.basename))?;
    for word in llp_perm.into_iter() {
        file.write(&word.to_be_bytes())?;
    }
    Ok(())
}
