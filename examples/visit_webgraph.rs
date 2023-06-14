use clap::Parser;
use dsi_progress_logger::ProgressLogger;
use std::collections::VecDeque;
use sux::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Visit the Rust Webgraph implementation", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let graph = webgraph::webgraph::load(&args.basename)?;

    let mut visited = BitMap::new(graph.num_nodes());
    let mut queue = VecDeque::new();

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "node";
    pr.local_speed = true;
    pr.expected_updates = Some(graph.num_nodes());
    pr.start("Visiting graph...");

    for start in 0..graph.num_nodes() {
        if visited.get(start as usize).unwrap() != 0 {
            continue;
        }
        queue.push_back(start as _);
        visited.set(start as _, 1).unwrap();
        pr.update();
        let mut current_node;

        while queue.len() > 0 {
            current_node = queue.pop_front().unwrap();
            for succ in graph.successors(current_node).unwrap() {
                if visited.get(succ as usize).unwrap() == 0 {
                    queue.push_back(succ);
                    visited.set(succ as _, 1).unwrap();
                    pr.update();
                }
            }
        }
    }

    pr.done();

    Ok(())
}
