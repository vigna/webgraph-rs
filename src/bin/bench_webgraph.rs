use clap::Parser;
use dsi_bitstream::prelude::*;
use mmap_rs::*;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::fs::File;
use std::hint::black_box;
use std::io::BufReader;
use std::io::Seek;
use webgraph::prelude::*;

type ReadType = u32;
type BufferType = u64;

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

    /// Test sequential degrees_on;y access speed by scanning the whole graph
    #[arg(short = 'd', long)]
    degrees_only: bool,

    /// Do not test speed, but check that the sequential and random-access successor lists are the same
    #[arg(short = 'c', long)]
    check: bool,
}

fn mmap_file(path: &str) -> Mmap {
    let mut file = std::fs::File::open(path).unwrap();
    let file_len = file.seek(std::io::SeekFrom::End(0)).unwrap();
    unsafe {
        MmapOptions::new(file_len as _)
            .unwrap()
            .with_file(file, 0)
            .map()
            .unwrap()
    }
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let f = File::open(format!("{}.properties", args.basename))?;
    let map = java_properties::read(BufReader::new(f))?;
    let comp_flags = CompFlags::from_properties(&map)?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;
    let num_arcs = map.get("arcs").unwrap().parse::<usize>()?;
    let min_interval_length = map.get("minintervallength").unwrap().parse::<usize>()?;
    let compression_window = map.get("windowsize").unwrap().parse::<usize>()?;

    // Read the offsets
    let data_graph = mmap_file(&format!("{}.graph", args.basename));

    let graph_slice = unsafe {
        core::slice::from_raw_parts(
            data_graph.as_ptr() as *const ReadType,
            (data_graph.len() + core::mem::size_of::<ReadType>() - 1)
                / core::mem::size_of::<ReadType>(),
        )
    };

    if args.check {
        // Create a sequential reader
        let mut seq_reader = WebgraphSequentialIter::new(
            DynamicCodesReader::new(
                BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                    &graph_slice,
                )),
                &comp_flags,
            )?,
            compression_window,
            min_interval_length,
            num_nodes,
        );

        // create a random access reader;
        let random_reader = webgraph::bvgraph::load(&args.basename)?;

        // Create a degrees reader
        let mut deg_reader = WebgraphDegreesIter::new(
            DynamicCodesReaderSkipper::new(
                BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                    &graph_slice,
                )),
                &comp_flags,
            )?,
            min_interval_length,
            compression_window,
            num_nodes,
        );

        // Check that sequential and random-access interfaces return the same result
        for node_id in 0..num_nodes {
            let seq = seq_reader.next_successors()?;
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

            assert_eq!(c, num_arcs);
        }
    } else if args.degrees_only {
        // Sequential speed test
        for _ in 0..args.repeats {
            // Create a degrees reader
            let mut deg_reader = WebgraphDegreesIter::new(
                DynamicCodesReaderSkipper::new(
                    BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                        &graph_slice,
                    )),
                    &comp_flags,
                )?,
                min_interval_length,
                compression_window,
                num_nodes,
            );

            let mut c: usize = 0;
            let start = std::time::Instant::now();
            for _ in 0..num_nodes {
                c += deg_reader.next_degree()?;
            }
            println!(
                "Degrees Only:{:>20} ns/arc",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );

            assert_eq!(c, num_arcs);
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
                        .successors(random.gen_range(0..num_nodes))
                        .next()
                        .unwrap_or(0);
                    c += 1;
                }
            } else {
                for _ in 0..args.n {
                    c += graph.successors(random.gen_range(0..num_nodes)).count();
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
