use std::fs::File;
use std::io::BufWriter;
use anyhow::Result;
use clap::Parser;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Create the '.offsets' file for a graph", long_about = None)]
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

    // Create the sequential iterator over the graph
    let mut seq_reader = WebgraphReaderSequential::from_basename(args.basename)?;
    // Create the offsets file
    let file = std::fs::File::create(
        &format!("{}.offsets", args.basename)
    )?;
    // create a bit writer on the file
    let mut writer = <BufferedBitStreamWrite<M2L, _>>::new(
        <FileBackend<u64, _>>::new(BufWriter::new(file))
    )?;
    // progress bar
    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "offset".into();
    pr.start("Computing offsets...");
    // read the graph a write the offsets
    let mut offset = 0;
    for node_id in 0..seq_reader.get_nodes_number().saturating_sub(1) {
        // write where 
        let new_offset = seq_reader.get_position();
        writer.write_gamma::<true>(new_offset - offset - 1)?;
        offset = new_offset;
        // decode the next nodes so we know where the next node_id starts
        let _ = seq_reader.get_successors_iter(node_id)?;
        pr.light_update();
    }
    // write the last offset, this is done to avoid decoding the last node
    writer.write_gamma::<true>(seq_reader.get_position() - offset - 1)?;
    pr.light_update();
    pr.done();
    Ok(())
}
