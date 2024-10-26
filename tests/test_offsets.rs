/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use lender::*;
use std::io::prelude::*;
use sux::prelude::*;
use webgraph::prelude::*;

#[test]
fn test_offsets() -> Result<()> {
    // load the graph
    let graph = BvGraph::with_basename("tests/data/cnr-2000")
        .endianness::<BE>()
        .load()?;
    // Read the offsets gammas
    let mut offsets_file = std::fs::File::open("tests/data/cnr-2000.offsets")?;
    let mut offsets_data = vec![0; offsets_file.metadata()?.len() as usize];
    offsets_file.read_exact(&mut offsets_data)?;

    let mut offsets = Vec::with_capacity(graph.num_nodes());
    let mut reader = BufBitReader::<BE, _>::new(MemWordReader::new(&offsets_data));
    let mut offset = 0;
    for _ in 0..graph.num_nodes() + 1 {
        offset += reader.read_gamma().unwrap() as usize;
        offsets.push(offset as u64);
    }
    println!("{:?}", offsets.len());

    // Load Elias-fano
    let ef_offsets = <webgraph::graphs::bvgraph::EF>::mmap(
        "tests/data/cnr-2000.ef",
        deser::Flags::TRANSPARENT_HUGE_PAGES,
    )?;

    for (i, offset) in offsets.iter().enumerate() {
        assert_eq!(*offset, ef_offsets.get(i) as _);
    }

    // Check that they read the same
    let mut iter_nodes = graph.iter();
    while let Some((node_id, seq_succ)) = iter_nodes.next() {
        let rand_succ = graph.successors(node_id).collect::<Vec<_>>();
        assert_eq!(rand_succ, seq_succ.collect::<Vec<_>>());
    }

    for (i, (offset, outdegree)) in graph.offset_deg_iter().enumerate() {
        assert_eq!(offset, ef_offsets.get(i) as _);
        assert_eq!(outdegree, graph.outdegree(i));
    }

    for start_node in 0..100 {
        for (i, (offset, outdegree)) in graph.offset_deg_iter_from(start_node).enumerate() {
            assert_eq!(offset, ef_offsets.get(start_node + i) as _);
            assert_eq!(outdegree, graph.outdegree(start_node + i));
        }
    }

    Ok(())
}

#[test]
fn test_offsets_as_slice() -> Result<()> {
    // load the graph
    let graph0 = BvGraph::with_basename("tests/data/cnr-2000")
        .endianness::<BE>()
        .load()?;
    let graph1 = BvGraph::with_basename("tests/data/cnr-2000")
        .endianness::<BE>()
        .load()?
        .offsets_to_slice();

    graph0
        .iter()
        .zip(graph1.iter())
        .for_each(|((s, a), (t, b))| {
            assert_eq!(s, t);
            itertools::assert_equal(a, b);
        });
    Ok(())
}
