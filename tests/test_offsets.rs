use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::io::prelude::*;
use sux::traits::Select;
use webgraph::prelude::*;

#[test]
fn test_offsets() -> Result<()> {
    // load the graph
    let graph = webgraph::graph::bvgraph::load("tests/data/cnr-2000")?;

    // Read the offsets gammas
    let mut offsets_file = std::fs::File::open("tests/data/cnr-2000.offsets")?;
    let mut offsets_data = vec![0; offsets_file.metadata()?.len() as usize];
    offsets_file.read_exact(&mut offsets_data)?;

    let mut offsets = Vec::with_capacity(graph.num_nodes());
    let mut reader =
        BufferedBitStreamRead::<BE, u64, _>::new(MemWordReadInfinite::new(&offsets_data));
    let mut offset = 0;
    for _ in 0..graph.num_nodes() + 1 {
        offset += reader.read_gamma().unwrap() as usize;
        offsets.push(offset as u64);
    }
    println!("{:?}", offsets.len());

    // Load Elias-fano
    let ef_offsets = epserde::map::<webgraph::EF<Vec<u64>>>(
        "tests/data/cnr-2000.ef",
        epserde::Flags::TRANSPARENT_HUGE_PAGES,
    )?;

    for (i, offset) in offsets.iter().enumerate() {
        assert_eq!(*offset, ef_offsets.select(i).unwrap() as _);
    }

    // Check that they read the same
    for (node_id, seq_succ) in graph.iter_nodes() {
        let rand_succ = graph.successors(node_id).collect::<Vec<_>>();
        assert_eq!(rand_succ, seq_succ.collect::<Vec<_>>());
    }

    Ok(())
}
