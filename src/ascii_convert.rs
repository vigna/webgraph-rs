use anyhow::Result;
use clap::Parser;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Dumps a graph as an COO arc list", long_about = None)]
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

    let (nodes_num, mut seq_reader) = WebgraphReaderSequential::from_basename(&args.basename)?;

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "offset".into();
    pr.start("Computing offsets...");

    for node_id in 0..nodes_num {
        for succ in seq_reader.next_successors()?.iter() {
            println!("{}\t{}", node_id, succ);
        }
    }
    
    pr.done();

    Ok(())
}
