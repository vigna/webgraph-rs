/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

mod common;

use anyhow::{Result, bail};
use dsi_bitstream::prelude::BE;
use lender::*;
use rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom};
use webgraph::{graphs::arc_list_graph::ArcListGraph, prelude::*};

#[test]
fn test_iter_nodes() -> Result<()> {
    let bvgraph = BvGraph::with_basename(common::cnr_2000_basename())
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
    let bvgraph = BvGraph::with_basename(common::cnr_2000_basename())
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
fn test_split_iter_bv() -> Result<()> {
    let bvgraph_seq = BvGraphSeq::with_basename("../data/cnr-2000")
        .endianness::<BE>()
        .load()?;

    test_split_iter(&bvgraph_seq)
}

#[test]
fn test_split_iter_bvrnd() -> Result<()> {
    let bvgraph = BvGraph::with_basename(common::cnr_2000_basename())
        .endianness::<BE>()
        .load()?;

    test_split_iter(&bvgraph)
}

#[test]
fn test_split_iter_perm() -> Result<()> {
    let bvgraph_seq = BvGraphSeq::with_basename("../data/cnr-2000")
        .endianness::<BE>()
        .load()?;

    let mut perm = (0..bvgraph_seq.num_nodes()).collect::<Vec<_>>();
    perm.shuffle(&mut SmallRng::seed_from_u64(0));

    let perm_graph = PermutedGraph {
        graph: &bvgraph_seq,
        perm: &perm,
    };

    test_split_iter(&perm_graph)
}

#[test]
fn test_split_iter_arc_list() -> Result<()> {
    let bvgraph_seq = BvGraphSeq::with_basename("../data/cnr-2000")
        .endianness::<BE>()
        .load()?;

    let arc_list_graph = ArcListGraph::new(
        bvgraph_seq.num_nodes(),
        bvgraph_seq
            .iter()
            .map_into_iter(|(node_id, succ)| {
                succ.into_iter().map(|s| (node_id, s)).collect::<Vec<_>>()
            })
            .flatten(),
    );

    test_split_iter(&arc_list_graph)
}

fn test_split_iter<'a, S: SequentialGraph + SplitLabeling>(g: &'a S) -> anyhow::Result<()>
where
    <S as SplitLabeling>::SplitLender<'a>: Clone,
{
    let mut iter = g.iter();
    for lender in g.split_iter(10) {
        for_![(split_node_id, split_succ) in lender {
            let Some((seq_node_id, seq_succ)) = iter.next() else {
                bail!("Too many nodes in split_iter");
            };
            assert_eq!(seq_node_id, split_node_id);
            assert!(itertools::equal(seq_succ, split_succ));
        }];
    }
    assert!(iter.next().is_none(), "Too few nodes in split_iter");
    Ok(())
}

/// Checks that `split_iter_at` with custom cutpoints returns the correct
/// node/successor pairs.
#[test]
fn test_split_iter_at_bvrnd() -> Result<()> {
    let g = BvGraph::with_basename(common::cnr_2000_basename())
        .endianness::<BE>()
        .load()?;

    // Cutpoints that don't start at 0 and don't end at num_nodes
    let cutpoints = [10, 20, 50, 100];
    test_split_iter_at(&g, &cutpoints)?;

    // Cutpoints starting at 0 but not ending at num_nodes
    let cutpoints = [0, 7, 42, 100];
    test_split_iter_at(&g, &cutpoints)?;

    // Cutpoints spanning the full range
    let n = g.num_nodes();
    let cutpoints = [0, n / 4, n / 2, 3 * n / 4, n];
    test_split_iter_at(&g, &cutpoints)?;

    // Cutpoints with empty segments (repeated values)
    let cutpoints = [10, 10, 30, 30, 50];
    test_split_iter_at(&g, &cutpoints)?;

    // Two-element cutpoints (single segment)
    let cutpoints = [5, 15];
    test_split_iter_at(&g, &cutpoints)?;

    Ok(())
}

#[test]
fn test_split_iter_at_bvseq() -> Result<()> {
    let g = BvGraphSeq::with_basename("../data/cnr-2000")
        .endianness::<BE>()
        .load()?;

    // Cutpoints starting at 0 (required for seq, which walks from the start)
    let cutpoints = [0, 7, 42, 100];
    test_split_iter_at(&g, &cutpoints)?;

    let n = g.num_nodes();
    let cutpoints = [0, n / 3, 2 * n / 3, n];
    test_split_iter_at(&g, &cutpoints)?;

    // Empty segments
    let cutpoints = [0, 0, 50, 50, 100];
    test_split_iter_at(&g, &cutpoints)?;

    Ok(())
}

fn test_split_iter_at<'a, S: SequentialGraph + SplitLabeling>(
    g: &'a S,
    cutpoints: &[usize],
) -> anyhow::Result<()>
where
    <S as SplitLabeling>::SplitLender<'a>: Clone,
{
    let mut count = 0;
    for (i, lender) in g
        .split_iter_at(cutpoints.iter().copied())
        .into_iter()
        .enumerate()
    {
        let start = cutpoints[i];
        let end = cutpoints[i + 1];
        let mut iter = g.iter_from(start).take(end - start);
        for_![(split_node_id, split_succ) in lender {
            let Some((seq_node_id, seq_succ)) = iter.next() else {
                bail!("Too many nodes in split_iter_at segment {i}");
            };
            assert_eq!(seq_node_id, split_node_id);
            assert!(itertools::equal(seq_succ, split_succ));
        }];
        assert!(
            iter.next().is_none(),
            "Too few nodes in split_iter_at segment {i}"
        );
        count += 1;
    }
    assert_eq!(count, cutpoints.len() - 1, "Wrong number of segments");
    Ok(())
}
