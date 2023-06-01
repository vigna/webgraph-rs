use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use std::fs::File;
use std::io::BufWriter;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Recompress a graph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// The basename for the newly compressed graph.
    new_basename: String,
    /// The compression windows
    #[clap(default_value_t = 7)]
    compression_window: usize,
    /// The minimum interval length
    #[clap(default_value_t = 4)]
    min_interval_length: usize,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let seq_reader = WebgraphSequentialIter::load_mapped(&args.basename)?;

    let file_path = format!("{}.graph", args.new_basename);
    let writer = <ConstCodesWriter<BE, _>>::new(<BufferedBitStreamWrite<BE, _>>::new(
        FileBackend::new(BufWriter::new(File::create(&file_path)?)),
    ));
    let mut bvcomp = BVComp::new(writer, args.compression_window, args.min_interval_length);

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "node".into();
    pr.start("Reading nodes...");
    pr.expected_updates = Some(seq_reader.num_nodes());

    for (_, iter) in seq_reader {
        bvcomp.push(iter)?;
        pr.light_update();
    }

    pr.done();
    bvcomp.flush()?;
    Ok(())
}
