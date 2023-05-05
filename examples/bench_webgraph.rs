use clap::Parser;
use java_properties;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::fs::File;
use std::io::BufReader;
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

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let f = File::open(format!("{}.properties", args.basename))?;
    let map = java_properties::read(BufReader::new(f))?;

    let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;
    let num_arcs = map.get("arcs").unwrap().parse::<u64>()?;
    // Read the offsets
    let mut data_offsets = std::fs::read(format!("{}.offsets", args.basename)).unwrap();
    // pad with zeros so we can read with ReadType words
    while data_offsets.len() % core::mem::size_of::<ReadType>() != 0 {
        data_offsets.push(0);
    }
    // we must do this becasue Vec<u8> is not guaranteed to be properly aligned
    let data_offsets = data_offsets
        .chunks(core::mem::size_of::<ReadType>())
        .map(|chunk| ReadType::from_ne_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    let mut data_graph = std::fs::read(format!("{}.graph", args.basename)).unwrap();
    // pad with zeros so we can read with ReadType words
    while data_graph.len() % core::mem::size_of::<ReadType>() != 0 {
        data_graph.push(0);
    }
    // we must do this becasue Vec<u8> is not guaranteed to be properly aligned
    let data_graph = data_graph
        .chunks(core::mem::size_of::<ReadType>())
        .map(|chunk| ReadType::from_ne_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
            
    // Read the offsets gammas
    let mut offsets = EliasFanoBuilder::new(data_graph.len() as u64 * 8, num_nodes);
    let mut reader =
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&data_offsets));
    let mut offset = 0;
    for _ in 0..num_nodes {
        offset += reader.read_gamma::<true>().unwrap() as usize;
        offsets.push(offset as _).unwrap();
    }

    let offsets = offsets.build();

    if args.check {
        // Create a sequential reader
        let mut code_reader = DefaultCodesReader::new(
            BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&data_graph)),
        );
        let mut seq_reader = WebgraphReaderSequential::new(&mut code_reader, 4, 16);

        // create a random access reader
        let code_reader = DefaultCodesReader::new(
            BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&data_graph)),
        );
        let random_reader = WebgraphReaderRandomAccess::new(code_reader, offsets, 4);

        // Check that sequential and random-access interfaces return the same result
        for node_id in 0..num_nodes {
            let seq = seq_reader.get_successors_iter(node_id)?;
            let random = random_reader
                .get_successors_iter(node_id)?
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
                    MemWordReadInfinite::new(&data_graph),
                ));
            let mut seq_reader = WebgraphReaderSequential::new(&mut code_reader, 4, 16);
            let mut c: usize = 0;
            let start = std::time::Instant::now();
            for node_id in 0..num_nodes {
                c += seq_reader.get_successors_iter(node_id)?.iter().count();
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
            let code_reader = DefaultCodesReader::new(
                BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&data_graph)),
            );
            let random_reader = WebgraphReaderRandomAccess::new(code_reader, offsets.clone(), 4);

            let mut random = SmallRng::seed_from_u64(0);
            let mut c: usize = 0;

            let start = std::time::Instant::now();
            for _ in 0..args.n {
                c += random_reader
                    .get_successors_iter(random.gen_range(0..args.n))?
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
