/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Fuzz compression and graph reading for both bvcomp and bvcompz, testing roundtrip
//! correctness. This is slower than `bvcomp_and_read` but tests both compressors,
//! and uses the higher-level constructs that the user is supposed to use.

use crate::fuzz::utils::CompFlagsFuzz;
use crate::graphs::bvgraph;
use crate::prelude::*;

use arbitrary::Arbitrary;
use dsi_bitstream::prelude::*;

#[derive(Arbitrary, Debug)]
pub struct FuzzCase {
    pub compression_flags: CompFlagsFuzz,
    pub edges: Vec<(u8, u8)>,
    pub chunk_size: usize,
}

pub fn harness(data: FuzzCase) {
    let comp_flags = data.compression_flags.into();
    let chunk_size = data.chunk_size.min(10 + data.edges.len());

    // convert the edges to a graph
    let mut edges = data
        .edges
        .into_iter()
        .map(|(src, dst)| (src as usize, dst as usize))
        .collect::<Vec<_>>();
    edges.sort();
    let graph = BTreeGraph::from_arcs(edges);

    let tmp_dir = tempfile::Builder::new()
        .prefix("bvcompz_roundtrip_fuzz")
        .tempdir()
        .unwrap();

    let tmp_path_bvcompz = tmp_dir.path().join("bvcompz");
    let tmp_path_bvcomp = tmp_dir.path().join("bvcomp");

    let mut bvcomp = BvComp::with_basename(&tmp_path_bvcomp).with_comp_flags(comp_flags);
    bvcomp.comp_graph::<BE>(&graph).unwrap();
    let new_graph = BvGraphSeq::with_basename(&tmp_path_bvcomp)
        .endianness::<BE>()
        .load()
        .unwrap();
    labels::eq_sorted(&graph, &new_graph).unwrap();
    bvgraph::check_offsets(&new_graph, &tmp_path_bvcomp).unwrap();

    let mut bvcompz = BvCompZ::with_basename(&tmp_path_bvcompz)
        .with_comp_flags(comp_flags)
        .with_chunk_size(chunk_size);
    bvcompz.par_comp_graph::<BE>(&graph).unwrap();
    let new_graph = BvGraphSeq::with_basename(&tmp_path_bvcompz)
        .endianness::<BE>()
        .load()
        .unwrap();
    labels::eq_sorted(&graph, &new_graph).unwrap();
    bvgraph::check_offsets(&new_graph, &tmp_path_bvcompz).unwrap();
}
