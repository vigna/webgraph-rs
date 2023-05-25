use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use java_properties;
use mmap_rs::*;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::fs::File;
use std::hint::black_box;
use std::io::BufReader;
use std::io::Seek;
use sux::prelude::*;
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

macro_rules! build_offsets {
    ($basename:expr, $num_nodes:expr, $data_graph:expr) => {{
        let data_offsets = mmap_file(&format!("{}.offsets", $basename));
        let offsets_slice = unsafe {
            core::slice::from_raw_parts(
                data_offsets.as_ptr() as *const ReadType,
                (data_offsets.len() + core::mem::size_of::<ReadType>() - 1)
                    / core::mem::size_of::<ReadType>(),
            )
        };
        // Read the offsets gammas
        let mut reader = BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
            &offsets_slice,
        ));

        let mut pr_offsets = ProgressLogger::default();
        pr_offsets.expected_updates = Some($num_nodes as _);
        pr_offsets.item_name = "offset".to_string();
        pr_offsets.start("Loading offsets...");
        // Read the offsets gammas
        let mut offsets = EliasFanoBuilder::new(
            ($data_graph.len() * 8 * core::mem::size_of::<ReadType>()) as u64,
            $num_nodes as u64,
        );

        let mut offset = 0;
        for _ in 0..$num_nodes {
            offset += reader.read_gamma().unwrap() as usize;
            offsets.push(offset as _).unwrap();
            pr_offsets.update();
        }
        pr_offsets.done_with_count($num_nodes as _);
        let offsets: EliasFano<SparseIndex<BitMap<Vec<u64>>, Vec<u64>, 8>, CompactArray<Vec<u64>>> =
            offsets.build().convert_to().unwrap();

        offsets
    }};
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
        let offsets = build_offsets!(args.basename, num_nodes, data_graph);
        // Create a sequential reader
        let mut seq_reader = WebgraphSequentialIter::new(
            DynamicCodesReader::new(
                BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                    &graph_slice,
                )),
                &comp_flags,
            )?,
            min_interval_length,
            compression_window,
            num_nodes as usize,
        );

        // create a random access reader;
        let random_reader = BVGraph::new(
            DynamicCodesReader::new(
                BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                    &graph_slice,
                )),
                &comp_flags,
            )?,
            offsets,
            min_interval_length,
            compression_window,
            num_nodes as usize,
            num_arcs as usize,
        );

        // Create a degrees reader
        let mut deg_reader = WebgraphDegreesIter::new(
            DynamicCodesReader::new(
                BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                    &graph_slice,
                )),
                &comp_flags,
            )?,
            min_interval_length,
            compression_window,
            num_nodes as usize,
        );

        // Check that sequential and random-access interfaces return the same result
        for node_id in 0..num_nodes {
            let seq = seq_reader.next_successors()?;
            let random = random_reader.successors(node_id)?.collect::<Vec<_>>();

            assert_eq!(deg_reader.next_degree()? as usize, seq.len(), "{}", node_id);
            assert_eq!(seq, random, "{}", node_id);
        }
    } else if args.sequential {
        // Sequential speed test
        for _ in 0..args.repeats {
            // Create a sequential reader
            let mut seq_reader = WebgraphSequentialIter::new(
                DynamicCodesReader::new(
                    BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                        &graph_slice,
                    )),
                    &comp_flags,
                )?,
                min_interval_length,
                compression_window,
                num_nodes as usize,
            );

            let mut c: usize = 0;
            let start = std::time::Instant::now();
            for _ in 0..num_nodes {
                c += seq_reader.next_successors()?.iter().count();
            }
            println!(
                "Sequential:{:>20} ns/arc",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );

            assert_eq!(c, num_arcs as usize);
        }
    } else if args.degrees_only {
        // Sequential speed test
        for _ in 0..args.repeats {
            // Create a degrees reader
            let mut deg_reader = WebgraphDegreesIter::new(
                DynamicCodesReader::new(
                    BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                        &graph_slice,
                    )),
                    &comp_flags,
                )?,
                min_interval_length,
                compression_window,
                num_nodes as usize,
            );

            let mut c: usize = 0;
            let start = std::time::Instant::now();
            for _ in 0..num_nodes {
                c += deg_reader.next_degree()? as usize;
            }
            println!(
                "Degrees Only:{:>20} ns/arc",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );

            assert_eq!(c, num_arcs as usize);
        }
    } else {
        let offsets = build_offsets!(args.basename, num_nodes, data_graph);
        // Random-access speed test
        for _ in 0..args.repeats {
            // create a random access reader;
            let random_reader = BVGraph::new(
                DynamicCodesReader::new(
                    BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                        &graph_slice,
                    )),
                    &comp_flags,
                )?,
                offsets.clone(),
                min_interval_length,
                compression_window,
                num_nodes as usize,
                num_arcs as usize,
            );

            let mut random = SmallRng::seed_from_u64(0);
            let mut c: usize = 0;
            let mut u: usize = 0;

            let start = std::time::Instant::now();
            if args.first {
                for _ in 0..args.n {
                    u += random_reader
                        .successors(random.gen_range(0..num_nodes))?
                        .next()
                        .unwrap_or(0);
                    c += 1;
                }
            } else {
                for _ in 0..args.n {
                    c += random_reader
                        .successors(random.gen_range(0..num_nodes))?
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
