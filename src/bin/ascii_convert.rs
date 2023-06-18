use anyhow::Result;
use clap::Parser;
use dsi_progress_logger::ProgressLogger;

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

    let seq_reader = webgraph::bvgraph::load_seq(&args.basename)?;
    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "offset";
    pr.start("Computing offsets...");

    for (node_id, successors) in seq_reader {
        println!(
            "{}\t{}",
            node_id,
            successors
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join("\t")
        );
    }

    pr.done();

    Ok(())
}
