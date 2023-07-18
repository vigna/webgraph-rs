use anyhow::Result;
use clap::Parser;
use std::io::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Performs an LLP round", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,

    #[arg(short, long, default_value_t = 100)]
    /// The maximum number of LLP iterations
    max_iters: usize,

    #[arg(short, long, default_value_t = 1000)]
    /// The size of the chunks each thread processes for the LLP
    granularity: usize,

    #[arg(short, long, default_value_t = 100000)]
    /// The size of the cnunks each thread processes for the random permutation
    /// at the start of each iteration
    chunk_size: usize,

    #[arg(short, long, default_value_t = 1.0)]
    /// The gamma to use in LLP
    gamma: f64,

    #[arg(short = 'j', long)]
    /// The number of cores to use
    num_cpus: Option<usize>,

    #[arg(short, long, default_value_t = 0x6135062444a930d0)]
    /// The seed to use for the prng
    seed: u64,
}

fn ceil_log2(x: usize) -> usize {
    if x <= 2 {
        x - 1
    } else {
        64 - (x - 1).leading_zeros() as usize
    }
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

    let mut perm = vec![0; graph.num_nodes()];
    // compute the LLP
    let labels = layered_label_propagation(
        &graph,
        &mut perm,
        args.gamma,
        args.num_cpus,
        args.max_iters,
        args.chunk_size,
        args.granularity,
        0,
    )?;

    log::info!("Elapsed: {}", start.elapsed().as_secs_f64());
    // dump the labels
    let labels = unsafe { std::mem::transmute::<Box<[usize]>, Box<[u8]>>(labels) };
    std::fs::File::create(format!("{}-{}.labels", args.basename, 0))?.write_all(&labels)?;

    // dump the permutation
    let cost = PermutedGraph {
        graph: &graph,
        perm: &perm,
    }
    .iter_nodes()
    .map(|(x, succ)| {
        let mut cost = 0;
        if !succ.len() != 0 {
            let mut sorted: Vec<_> = succ.collect();
            sorted.sort();
            cost += ceil_log2((x as isize - sorted[0] as isize).unsigned_abs());
            cost += sorted
                .windows(2)
                .map(|w| ceil_log2(w[1] - w[0]))
                .sum::<usize>();
        }
        cost
    })
    .sum::<usize>();
    log::info!("The final cost is: {}", cost);

    Ok(())
}
