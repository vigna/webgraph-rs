/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{bail, Result};
use dsi_bitstream::prelude::BE;
use lender::*;
use webgraph::prelude::*;

#[test]
fn test_iter_nodes() -> Result<()> {
    let bvgraph = BVGraph::with_basename("tests/data/cnr-2000")
        .endianness::<BE>()
        .load()?;

    let mut seen_node_ids = Vec::new();

    // Check that they read the same
    let mut iter_nodes = bvgraph.iter();
    while let Some((node_id, seq_succ)) = iter_nodes.next() {
        seen_node_ids.push(node_id);
        let rand_succ = bvgraph.successors(node_id).collect::<Vec<_>>();
        assert_eq!(rand_succ, seq_succ.into_iter().collect::<Vec<_>>());
    }

    assert_eq!(seen_node_ids, (0..bvgraph.num_nodes()).collect::<Vec<_>>());

    Ok(())
}

#[test]
fn test_iter_nodes_from() -> Result<()> {
    let bvgraph = BVGraph::with_basename("tests/data/cnr-2000")
        .endianness::<BE>()
        .load()?;

    for i in [0, 1, 2, 5, 10, 100] {
        let mut seen_node_ids = Vec::new();
        // Check that they read the same
        let mut iter_nodes = bvgraph.iter_from(i).take(100);
        while let Some((node_id, seq_succ)) = iter_nodes.next() {
            seen_node_ids.push(node_id);
            assert!(itertools::equal(
                bvgraph.successors(node_id),
                seq_succ.into_iter()
            ));
        }

        assert_eq!(
            seen_node_ids,
            (i..bvgraph.num_nodes()).take(100).collect::<Vec<_>>()
        );
    }

    Ok(())
}

#[test]
fn test_split_iter() -> Result<()> {
    let bvgraph_seq = BVGraphSeq::with_basename("tests/data/cnr-2000")
        .endianness::<BE>()
        .load()?;

    let mut iter = bvgraph_seq.iter();
    for lender in bvgraph_seq.split_iter(10) {
        for_![(split_node_id, split_succ) in lender {
            let Some((seq_node_id, seq_succ)) = iter.next() else {
                bail!("Too many nodes in split_iter");
            };
            assert_eq!(seq_node_id, split_node_id);
            assert!(itertools::equal(seq_succ, split_succ));
        }];
    }
    Ok(())
}
