use anyhow::Result;
use clap::Parser;
use epserde::ser::Serialize;
use rand::prelude::SliceRandom;
use std::io::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Create a random permutation for a given graph", long_about = None)]
struct Args {
    /// The basename of the graph.
    source: String,
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

    #[arg(short = 'e', long)]
    /// Load the permutation from ε-serde format.
    epserde: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let graph = webgraph::graph::bvgraph::load_seq(&args.source)?;

    let mut rng = rand::thread_rng();
    let mut perm = (0..graph.num_nodes()).collect::<Vec<_>>();
    perm.shuffle(&mut rng);

    if args.epserde {
        perm.store(&args.perm)?;
    } else {
        let mut file = std::io::BufWriter::new(std::fs::File::create(args.perm)?);
        for perm in perm {
            file.write_all(&perm.to_be_bytes())?;
        }
    }

    Ok(())
}
