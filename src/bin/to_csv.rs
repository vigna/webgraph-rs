use clap::Parser;
use dsi_progress_logger::*;
use lender_derive::for_;
use std::io::Write;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Read a BVGraph and print the edge list `{src}\t{dst}` to stdout", long_about = None)]
struct Args {
    /// The basename of the dst.
    basename: String,

    #[arg(long, default_value_t = ',')]
    /// The index of the column containing the source node str.
    pub csv_separator: char,
}

fn main() {
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let args = Args::parse();

    let graph = webgraph::graph::bvgraph::load_seq(&args.basename).unwrap();
    let num_nodes = graph.num_nodes();

    // read the csv and put it inside the sort pairs
    let mut stdout = std::io::BufWriter::new(std::io::stdout().lock());
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("nodes")
        .expected_updates(Some(num_nodes));
    pl.start("Reading BVGraph");

    for_! ( (src, succ) in graph.iter() {
        for dst in succ {
            writeln!(stdout, "{}{}{}", src, args.csv_separator, dst).unwrap();
        }
        pl.light_update();
    });

    pl.done();
}
