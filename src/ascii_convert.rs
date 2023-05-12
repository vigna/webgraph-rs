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

    let seq_reader = WebgraphSequentialIter::load_mapped(&args.basename)?;

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "offset".into();
    pr.start("Computing offsets...");

    for (node_id, successors) in seq_reader.enumerate() {
        println!("{}\t{}", node_id, successors.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("\t"));
    }
    
    pr.done();

    Ok(())
}
