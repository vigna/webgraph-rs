use clap::Parser;
use java_properties;
use mmap_rs::*;
use mmap_rs::*;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use sux::prelude::*;
use webgraph::prelude::*;

type ReadType = u32;
type BufferType = u64;

#[derive(Parser, Debug)]
#[command(about = "Dumps a graph as an arc list", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
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
            (data_offsets.len() + core::mem::size_of::<ReadType>() - 1)
                / core::mem::size_of::<ReadType>(),
        )
    };
    let graph_slice = unsafe {
        core::slice::from_raw_parts(
            data_graph.as_ptr() as *const ReadType,
            (data_graph.len() + core::mem::size_of::<ReadType>() - 1)
                / core::mem::size_of::<ReadType>(),
        )
    };

    // Read the offsets gammas
    let mut offsets = EliasFanoBuilder::new(
        (data_graph.len() * 8 * core::mem::size_of::<ReadType>()) as u64,
        num_nodes,
    );
    let mut reader =
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&offsets_slice));

    let mut pr_offsets = ProgressLogger::default();
    pr_offsets.name = "offset".to_string();
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

    let mut code_reader = DefaultCodesReader::new(
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&graph_slice)),
    );
    let mut seq_reader = WebgraphReaderSequential::new(&mut code_reader, 4, 16);
    let mut c: usize = 0;
    let start = std::time::Instant::now();
    for node_id in 0..num_nodes {
        for succ in seq_reader.get_successors_iter(node_id)?.iter() {
            println!("{}\t{}", node_id, succ);
        }
    }

    Ok(())
}
