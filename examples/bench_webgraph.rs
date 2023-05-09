use clap::Parser;
use java_properties;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::fs::File;
use std::io::BufReader;
use std::io::Seek;
use mmap_rs::*;
use webgraph::prelude::*;
use sux::prelude::*;

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
    n: u64,

    /// Test sequential access speed by scanning the whole graph
    #[arg(short = 's', long)]
    sequential: bool,

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

    let f = File::open(format!("{}.properties", args.basename))?;
    let map = java_properties::read(BufReader::new(f))?;

    let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;
    let num_arcs = map.get("arcs").unwrap().parse::<u64>()?;
    // Read the offsets
    let data_offsets = mmap_file(&format!("{}.offsets", args.basename));
    let data_graph = mmap_file(&format!("{}.graph", args.basename));

    let offsets_slice = unsafe {
        core::slice::from_raw_parts(
            data_offsets.as_ptr() as *const ReadType, 
            (data_offsets.len() + core::mem::size_of::<ReadType>() - 1) / core::mem::size_of::<ReadType>(),
        )
    };
    let graph_slice = unsafe {
        core::slice::from_raw_parts(
            data_graph.as_ptr() as *const ReadType, 
            (data_graph.len() + core::mem::size_of::<ReadType>() - 1) / core::mem::size_of::<ReadType>(),
        )
    };

    // Read the offsets gammas
    let mut reader =
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&offsets_slice));
        
    let mut pr_offsets = ProgressLogger::default();
    pr_offsets.item_name = "offset".to_string();
    pr_offsets.start("Loading offsets...");
    // Read the offsets gammas
    let mut offsets = EliasFanoBuilder::new(
        (data_graph.len() * 8 * core::mem::size_of::<ReadType>()) as u64,
        num_nodes,
    );

    let mut offset = 0;
    for _ in 0..num_nodes {
        offset += reader.read_gamma::<true>().unwrap() as usize;
        offsets.push(offset as _).unwrap();
        pr_offsets.update();
    }
    pr_offsets.done_with_count(num_nodes as _);
    let offsets: EliasFano<SparseIndex<BitMap<Vec<u64>>, Vec<u64>, 8>, CompactArray<Vec<u64>>> =
        offsets.build().convert_to().unwrap();

    if args.check {
        // Create a sequential reader
        let mut code_reader = DefaultCodesReader::new(
            BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&graph_slice)),
        );
        let mut seq_reader = WebgraphReaderSequential::new(code_reader, 4, 16);

        // create a random access reader
        let code_reader = DefaultCodesReader::new(
            BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&graph_slice)),
        );
        let random_reader = WebgraphReaderRandomAccess::new(code_reader, offsets, 4);

        // Check that sequential and random-access interfaces return the same result
        for node_id in 0..num_nodes {
            let seq = seq_reader.next_successors()?;
            let random = random_reader
                .successors(node_id)?
                .collect::<Vec<_>>();

            // Why won't assert!(seq.iter().eq(random.iter())) work if I don't collect?
            assert!(seq.iter().eq(random.iter()));
        }
    } else if args.sequential {
        // Sequential speed test
        for _ in 0..args.repeats {
            // Create a sequential reader
            let mut code_reader =
                DefaultCodesReader::new(BufferedBitStreamRead::<M2L, BufferType, _>::new(
                    MemWordReadInfinite::new(&graph_slice),
                ));
            let mut seq_reader = WebgraphReaderSequential::new(code_reader, 4, 16);
            let mut c: usize = 0;
            let start = std::time::Instant::now();
            for _ in 0..num_nodes {
                c += seq_reader.next_successors()?.iter().count();
            }
            println!(
                "Sequential:{:>20} ns/arcs",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );

            assert_eq!(c, num_arcs as usize);
        }
    } else {
        // Random-access speed test
        for _ in 0..args.repeats {
            // create a random access reader
            let code_reader =
                DefaultCodesReader::new(BufferedBitStreamRead::<M2L, BufferType, _>::new(
                    MemWordReadInfinite::new(&data_graph),
                ));
            let random_reader = WebgraphReaderRandomAccess::new(code_reader, offsets.clone(), 4);

            let mut random = SmallRng::seed_from_u64(0);
            let mut c: usize = 0;

            let start = std::time::Instant::now();
            for _ in 0..args.n {
                c += random_reader
                    .successors(random.gen_range(0..num_nodes))?
                    .count();
            }

            println!(
                "Random:    {:>20} ns/arcs",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );
        }
    }
    Ok(())
}
