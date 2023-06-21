use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use std::io::BufWriter;
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
    let seq_graph = webgraph::bvgraph::load_seq(&args.basename)?;
    let seq_graph =
        seq_graph.map_codes_reader_builder(|cbr| DynamicCodesReaderSkipperBuilder::from(cbr));
    // Create the offsets file
    let file = std::fs::File::create(&format!("{}.offsets", args.basename))?;
    // create a bit writer on the file
    let mut writer = <BufferedBitStreamWrite<BE, _>>::new(<FileBackend<u64, _>>::new(
        BufWriter::with_capacity(1 << 20, file),
    ));
    // progress bar
    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "offset";
    pr.expected_updates = Some(seq_graph.num_nodes());
    pr.start("Computing offsets...");
    // read the graph a write the offsets
    let mut offset = 0;
    let mut degs_iter = seq_graph.iter_degrees();
    for (new_offset, _node_id, _degree) in &mut degs_iter {
        // write where
        writer.write_gamma((new_offset - offset) as _)?;
        offset = new_offset;
        // decode the next nodes so we know where the next node_id starts
        pr.light_update();
    }
    // write the last offset, this is done to avoid decoding the last node
    writer.write_gamma((degs_iter.get_pos() - offset) as _)?;
    pr.light_update();
    pr.done();
    Ok(())
}
