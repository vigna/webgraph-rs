use anyhow::Result;
use bitvec::prelude::*;
use dsi_progress_logger::ProgressLogger;
use std::collections::VecDeque;
use webgraph::prelude::*;

pub fn main() -> Result<()> {
    // Setup a stderr logger because ProgressLogger uses the `log` crate
    // to printout
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // Load the mph
    let mph = webgraph::utils::mph::GOVMPH::load("tests/data/test.cmph")?;

    // Lookup an swhid
    let swhid = "swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2";
    let node_id = mph.get_byte_array(swhid.as_bytes()) as usize;

    // Load a default bvgraph with memory mapping,
    let graph = webgraph::bvgraph::load("tests/data/cnr-2000")?;

    // Setup a queue and a visited bitmap for the visit
    let num_nodes = graph.num_nodes();
    let mut visited = bitvec![u64, Lsb0; 0; num_nodes];
    let mut queue = VecDeque::new();
    assert!(node_id < num_nodes);
    queue.push_back(node_id);

    // Setup the progress logger for
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("Visiting graph...");

    // Standard BFS
    while let Some(current_node) = queue.pop_front() {
        for succ in graph.successors(current_node) {
            if !visited[succ] {
                queue.push_back(succ);
                visited.set(succ as _, true);
                pl.light_update();
            }
        }
    }

    pl.done();

    Ok(())
}
