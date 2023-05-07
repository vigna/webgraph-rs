use clap::Parser;
use java_properties;
use mmap_rs::*;
use std::fs::File;
use std::io::BufReader;
use std::io::Seek;
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
    let data_graph = mmap_file(&format!("{}.graph", args.basename));

    let graph_slice = unsafe {
        core::slice::from_raw_parts(
            data_graph.as_ptr() as *const ReadType,
            (data_graph.len() + core::mem::size_of::<ReadType>() - 1)
                / core::mem::size_of::<ReadType>(),
        )
    };

    let mut code_reader = DefaultCodesReader::new(
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&graph_slice)),
    );
    let mut seq_reader = WebgraphReaderSequential::new(&mut code_reader, 4, 16);

    for node_id in 0..num_nodes {
        for succ in seq_reader.get_successors_iter(node_id)?.iter() {
            println!("{}\t{}", node_id, succ);
        }
    }

    Ok(())
}
