use clap::Parser;
use java_properties;
use mmap_rs::*;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::io::Seek;
use sux::prelude::*;
use webgraph::prelude::*;
use webgraph::utils::ProgressLogger;

type ReadType = u32;
type BufferType = u64;

#[derive(Parser, Debug)]
#[command(about = "Visit the Rust Webgraph implementation", long_about = None)]
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

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let f = File::open(format!("{}.properties", args.basename))?;
    let map = java_properties::read(BufReader::new(f))?;

    let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;
    let min_interval_length = map.get("minintervallength").unwrap().parse::<usize>()?;
    let compression_window = map.get("windowsize").unwrap().parse::<usize>()?;

    assert_eq!(map.get("compressionflags").unwrap(), "");

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

    let mut reader =
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&offsets_slice));

    let mut pr_offsets = ProgressLogger::default();
    pr_offsets.expected_updates = Some(num_nodes as _);
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

    let code_reader = DefaultCodesReader::new(BufferedBitStreamRead::<M2L, BufferType, _>::new(
        MemWordReadInfinite::new(&graph_slice),
    ));
    let random_reader = BVGraph::new(
        code_reader, offsets.clone(), 
        min_interval_length, compression_window, num_nodes as usize,
    );

    let mut visited = BitMap::new(num_nodes as usize);
    let mut queue = VecDeque::new();

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "node".to_string();
    pr.local_speed = true;
    pr.expected_updates = Some(num_nodes as usize);
    pr.start("Visiting graph...");

    for start in 0..num_nodes {
        if visited.get(start as usize).unwrap() != 0 {
            continue;
        }
        queue.push_back(start as _);
        visited.set(start as _, 1).unwrap();
        pr.update();
        let mut current_node;

        while queue.len() > 0 {
            current_node = queue.pop_front().unwrap();
            for succ in random_reader.successors(current_node).unwrap() {
                if visited.get(succ as usize).unwrap() == 0 {
                    queue.push_back(succ);
                    visited.set(succ as _, 1).unwrap();
                    pr.update();
                }
            }
        }
    }

    pr.done();

    Ok(())
}
