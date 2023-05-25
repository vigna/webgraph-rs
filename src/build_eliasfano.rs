use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use sux::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Create the '.ef' file for a graph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let f = File::open(format!("{}.properties", args.basename))?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;

    let mut file = File::open(format!("{}.graph", args.basename)).unwrap();
    let file_len = 8 * file.seek(std::io::SeekFrom::End(0)).unwrap();

    let mut efb = EliasFanoBuilder::new(file_len, num_nodes + 1);

    let mut ef_file = BufWriter::new(File::create(format!("{}.ef", args.basename))?);

    // Create the offsets file
    let of_file = BufReader::new(File::open(&format!("{}.offsets", args.basename))?);
    // create a bit reader on the file
    let mut reader = BufferedBitStreamRead::<BE, u64, _>::new(<FileBackend<u32, _>>::new(of_file));
    // progress bar
    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "offset".into();
    pr.start("Translating offsets...");
    // read the graph a write the offsets
    let mut offset = 0;
    for _ in 0..num_nodes + 1 {
        // write where
        offset += reader.read_gamma()?;
        efb.push(offset as _).unwrap();
        // decode the next nodes so we know where the next node_id starts
        pr.light_update();
    }
    pr.done();

    let ef = efb.build();
    let ef: webgraph::EF<_, _, _> = ef.convert_to().unwrap();
    ef.serialize(&mut ef_file).unwrap();
    Ok(())
}
