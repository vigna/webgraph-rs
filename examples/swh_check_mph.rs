use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
use std::io::prelude::*;
use webgraph::prelude::*;

pub fn main() -> Result<()> {
    // Setup a stderr logger because ProgressLogger uses the `log` crate
    // to printout
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    println!("loading MPH...");
    let mph = webgraph::utils::mph::GOVMPH::load("/home/zack/graph/latest/compressed/graph.cmph")?;

    println!("loading compressed graph into memory (with mmap)...");
    let graph = webgraph::bvgraph::load("/home/zack/graph/latest/compressed/graph")?;

    println!("opening graph.nodes.csv...");
    let file = std::io::BufReader::with_capacity(
        1 << 20,
        std::fs::File::open("/home/zack/graph/latest/compressed/graph.nodes.csv")?,
    );

    // Setup the progress logger for
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Visiting graph...");

    for (node_id, line) in file.lines().enumerate() {
        let line = line?;
        let id = mph.get_byte_array(line.as_bytes());
        assert_eq!(
            id as usize, node_id,
            "line {:?} has id {} but mph got {}",
            line, node_id, id
        );
    }

    pl.done();

    Ok(())
}
