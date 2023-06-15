use std::collections::HashMap;

use anyhow::Result;
use dsi_bitstream::prelude::*;
use sux::ranksel::elias_fano;
use webgraph::prelude::*;

type ReadType = u32;
type BufferType = u64;

const NODES: usize = 325557;
const ARCS: usize = 3216152;

#[test]
fn test_sequential_reading() -> Result<()> {
    // Read the offsets
    let mut data = std::fs::read("tests/data/cnr-2000.offsets").unwrap();
    // pad with zeros so we can read with ReadType words
    while data.len() % core::mem::size_of::<ReadType>() != 0 {
        data.push(0);
    }
    // we must do this becasue Vec<u8> is not guaranteed to be properly aligned
    let data = data
        .chunks(core::mem::size_of::<ReadType>())
        .map(|chunk| ReadType::from_ne_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();

    // Read the offsets gammas
    let mut offsets = Vec::with_capacity(NODES);
    let mut reader =
        BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(&data));
    let mut offset = 0;
    for _ in 0..NODES {
        offset += reader.read_gamma().unwrap() as usize;
        offsets.push(offset as u64);
    }

    let mut builder = elias_fano::EliasFanoBuilder::new(offset as u64 + 1, offsets.len() as u64);
    for o in offsets {
        builder.push(o)?;
    }

    let mut data = std::fs::read("tests/data/cnr-2000.graph").unwrap();
    // pad with zeros so we can read with ReadType words
    while data.len() % core::mem::size_of::<ReadType>() != 0 {
        data.push(0);
    }
    // we must do this becasue Vec<u8> is not guaranteed to be properly aligned
    let data = data
        .chunks(core::mem::size_of::<ReadType>())
        .map(|chunk| ReadType::from_ne_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();

    let cf = &CompFlags::from_properties(&HashMap::new()).unwrap();
    // create a random access reader
    let bvgraph = BVGraph::new(
        <DynamicCodesReaderBuilder<BE, _>>::new(data, cf).unwrap(),
        sux::prelude::encase_mem(builder.build()),
        cf.min_interval_length,
        cf.compression_window,
        NODES,
        ARCS,
    );

    // Check that they read the same
    for (node_id, seq_succ) in bvgraph.iter_nodes() {
        let rand_succ = bvgraph.successors(node_id).collect::<Vec<_>>();
        assert_eq!(rand_succ, seq_succ.collect::<Vec<_>>());
    }

    Ok(())
}
