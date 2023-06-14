use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use std::io::BufWriter;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Visit the Rust Webgraph implementation", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// The basename of the transposed graph.
    transpose: String,
    /// The size of a batch.
    batch_size: usize,
    /// Location for storage of temporary files
    #[arg(short = 't', long)]
    temp_dir: bool,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let seq_reader = WebgraphSequentialIter::load_mapped(&args.basename)?;
    let mut sorted = Sorted::new(seq_reader.num_nodes(), args.batch_size)?;

    let mut pl = ProgressLogger::default();
    pl.item_name = "node".into();
    pl.expected_updates = Some(seq_reader.num_nodes());
    pl.start("Creating batches...");

    for (node, succ) in seq_reader {
        for s in succ {
            sorted.push(s, node)?;
        }
        pl.light_update();
    }
    let sorted = sorted.build()?;
    pl.done();

    let file = std::fs::File::create(&format!("{}.graph", args.transpose))?;

    let bit_write =
        <BufferedBitStreamWrite<LE, _>>::new(<FileBackend<u64, _>>::new(BufWriter::new(file)));

    let codes_writer = DynamicCodesWriter::new(
        bit_write,
        &CompFlags {
            ..Default::default()
        },
    );

    let mut bvcomp = BVComp::new(codes_writer, 1, 4, 3);
    pl.expected_updates = Some(sorted.num_nodes());
    pl.item_name = "node".into();
    pl.start("Writing...");
    for (_, succ) in sorted.iter_nodes()? {
        bvcomp.push(succ)?;
        pl.light_update();
    }
    bvcomp.flush()?;
    pl.done();

    Ok(())
}
