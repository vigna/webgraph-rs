use clap::Parser;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::hint::black_box;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Benchmarks the Rust Webgraph implementation", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,

    /// The number of test repetitions
    #[arg(short, long, default_value = "10")]
    repeats: usize,

    /// The number of successor lists in random-access tests
    #[arg(short, long, default_value = "1000000")]
    n: usize,

    /// Test sequential access speed by scanning the whole graph
    #[arg(short = 's', long)]
    sequential: bool,

    /// Test random access to the first successor
    #[arg(short = 'f', long)]
    first: bool,

    /// Test sequential degrees_only access speed by scanning the whole graph
    #[arg(short = 'd', long)]
    degrees_only: bool,

    /// Do not test speed, but check that the sequential and random-access successor lists are the same
    #[arg(short = 'c', long)]
    check: bool,
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    if args.check {
        // Create a sequential reader
        let seq_graph = webgraph::bvgraph::load_seq(&args.basename)?;
        let seq_graph = seq_graph.map_codes_reader_builder(DynamicCodesReaderSkipperBuilder::from);
        // create a random access reader;
        let random_reader = webgraph::bvgraph::load(&args.basename)?;

        // Check that sequential and random-access interfaces return the same result
        let mut seq_iter = seq_graph.iter_nodes();
        let mut deg_reader = seq_graph.iter_degrees();
        for node_id in 0..seq_graph.num_nodes() {
            let seq = seq_iter.next_successors()?;
            let random = random_reader.successors(node_id).collect::<Vec<_>>();

            assert_eq!(deg_reader.next_degree()?, seq.len(), "{}", node_id);
            assert_eq!(seq, random, "{}", node_id);
        }
    } else if args.sequential {
        // Sequential speed test
        for _ in 0..args.repeats {
            // Create a sequential reader
            let mut c = 0;
            let seq_graph = webgraph::bvgraph::load_seq(&args.basename)?;
            let start = std::time::Instant::now();
            for (_, succ) in &seq_graph {
                c += succ.count();
            }
            println!(
                "Sequential:{:>20} ns/arc",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );

            assert_eq!(c, seq_graph.num_arcs_hint().unwrap());
        }
    } else if args.degrees_only {
        // Sequential speed test
        for _ in 0..args.repeats {
            let seq_graph = webgraph::bvgraph::load_seq(&args.basename)?;
            let seq_graph =
                seq_graph.map_codes_reader_builder(DynamicCodesReaderSkipperBuilder::from);
            let mut deg_reader = seq_graph.iter_degrees();

            let mut c: usize = 0;
            let start = std::time::Instant::now();
            for _ in 0..seq_graph.num_nodes() {
                c += deg_reader.next_degree()?;
            }
            println!(
                "Degrees Only:{:>20} ns/arc",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );

            assert_eq!(c, seq_graph.num_arcs_hint().unwrap());
        }
    } else {
        let graph = webgraph::bvgraph::load(&args.basename)?;

        // Random-access speed test
        for _ in 0..args.repeats {
            // create a random access reader;

            let mut random = SmallRng::seed_from_u64(0);
            let mut c: usize = 0;
            let mut u: usize = 0;

            let start = std::time::Instant::now();
            if args.first {
                for _ in 0..args.n {
                    u += graph
                        .successors(random.gen_range(0..graph.num_nodes()))
                        .next()
                        .unwrap_or(0);
                    c += 1;
                }
            } else {
                for _ in 0..args.n {
                    c += graph
                        .successors(random.gen_range(0..graph.num_nodes()))
                        .count();
                }
            }

            println!(
                "{}:    {:>20} ns/arc",
                if args.first { "First" } else { "Random" },
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );
            black_box(u);
        }
    }
    Ok(())
}
