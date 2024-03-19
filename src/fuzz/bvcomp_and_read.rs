/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use arbitrary::Arbitrary;
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use lender::prelude::*;
use sux::prelude::*;

#[derive(Clone, Debug, arbitrary::Arbitrary)]
pub enum CodeFuzz {
    Unary,
    Gamma,
    Delta,
    Zeta3,
}
impl From<CodeFuzz> for Code {
    fn from(value: CodeFuzz) -> Self {
        match value {
            CodeFuzz::Unary => Code::Unary,
            CodeFuzz::Gamma => Code::Gamma,
            CodeFuzz::Delta => Code::Delta,
            CodeFuzz::Zeta3 => Code::Zeta { k: 3 },
        }
    }
}

#[derive(Clone, Debug, arbitrary::Arbitrary)]
pub struct CompFlagsFuzz {
    pub outdegrees: CodeFuzz,
    pub references: CodeFuzz,
    pub blocks: CodeFuzz,
    pub intervals: CodeFuzz,
    pub residuals: CodeFuzz,
    pub min_interval_length: u8,
    pub compression_window: u8,
    pub max_ref_count: u8,
}

impl From<CompFlagsFuzz> for CompFlags {
    fn from(value: CompFlagsFuzz) -> Self {
        CompFlags {
            outdegrees: value.outdegrees.into(),
            references: value.references.into(),
            blocks: value.blocks.into(),
            intervals: value.intervals.into(),
            residuals: value.residuals.into(),
            min_interval_length: value.min_interval_length as usize,
            compression_window: value.compression_window as usize,
            max_ref_count: value.max_ref_count as usize,
        }
    }
}

#[derive(Arbitrary, Debug)]
pub struct FuzzCase {
    pub compression_flags: CompFlagsFuzz,
    pub edges: Vec<(u8, u8)>,
}

pub fn harness(data: FuzzCase) {
    let comp_flags = data.compression_flags.into();
    // convert the edges to a graph
    let mut edges = data
        .edges
        .into_iter()
        .map(|(src, dst)| (src as usize, dst as usize))
        .collect::<Vec<_>>();
    edges.sort();
    let graph = Left(VecGraph::from_arc_list(edges));
    // Compress in big endian
    let mut codes_data_be = Vec::new();
    {
        let bit_writer = <BufBitWriter<BE, _>>::new(MemWordWriterVec::new(&mut codes_data_be));
        let codes_writer = <DynCodesEncoder<BE, _>>::new(bit_writer, &comp_flags);
        let mut bvcomp = BVComp::new(
            codes_writer,
            comp_flags.compression_window,
            comp_flags.max_ref_count,
            comp_flags.min_interval_length,
            0,
        );
        bvcomp.extend(graph.iter()).unwrap();
        bvcomp.flush().unwrap();
    }
    // Compress in little endian
    let mut codes_data_le = Vec::new();
    {
        let bit_writer = <BufBitWriter<LE, _>>::new(MemWordWriterVec::new(&mut codes_data_le));
        let codes_writer = <DynCodesEncoder<LE, _>>::new(bit_writer, &comp_flags);
        let mut bvcomp = BVComp::new(
            codes_writer,
            comp_flags.compression_window,
            comp_flags.max_ref_count,
            comp_flags.min_interval_length,
            0,
        );
        bvcomp.extend(graph.iter()).unwrap();
        bvcomp.flush().unwrap();
    }
    assert_eq!(codes_data_be.len(), codes_data_le.len());

    // convert to u32 for faster reader
    let data_be: &[u32] = unsafe {
        core::slice::from_raw_parts(
            codes_data_be.as_ptr() as *const u32,
            codes_data_be.len() * (core::mem::size_of::<u64>() / core::mem::size_of::<u32>()),
        )
    };
    let data_le: &[u32] = unsafe {
        core::slice::from_raw_parts(
            codes_data_le.as_ptr() as *const u32,
            codes_data_le.len() * (core::mem::size_of::<u64>() / core::mem::size_of::<u32>()),
        )
    };
    // create code reader builders
    let codes_reader_be = <DynCodesDecoderFactory<BE, _, _>>::new(
        MemoryFactory::from_data(data_be),
        MemCase::from(EmptyDict::default()),
        comp_flags.clone(),
    )
    .unwrap();
    let codes_reader_le = <DynCodesDecoderFactory<LE, _, _>>::new(
        MemoryFactory::from_data(data_le),
        MemCase::from(EmptyDict::default()),
        comp_flags.clone(),
    )
    .unwrap();

    // test sequential graphs and build the offsets
    let mut efb = EliasFanoBuilder::new(
        graph.num_nodes() + 1,
        data_be.len() * 8 * core::mem::size_of::<u32>(),
    );
    let mut offsets = Vec::with_capacity(graph.num_nodes() + 1);
    offsets.push(0);
    efb.push(0).unwrap();

    // create seq graphs
    let seq_graph_be = BVGraphSeq::new(
        codes_reader_be,
        graph.num_nodes(),
        Some(graph.num_arcs()),
        comp_flags.compression_window,
        comp_flags.min_interval_length,
    );
    let seq_graph_le = BVGraphSeq::new(
        codes_reader_le,
        graph.num_nodes(),
        Some(graph.num_arcs()),
        comp_flags.compression_window,
        comp_flags.min_interval_length,
    );
    // create seq iters
    let mut seq_iter = graph.iter();
    let mut seq_iter_be = seq_graph_be.iter();
    let mut seq_iter_le = seq_graph_le.iter();
    assert_eq!(seq_iter_be.bit_pos().unwrap(), 0);
    assert_eq!(seq_iter_le.bit_pos().unwrap(), 0);
    // verify that they are the same and build the offsets
    for _ in 0..graph.num_nodes() {
        let (node_id, succ) = seq_iter.next().unwrap();
        let (node_id_be, succ_be) = seq_iter_be.next().unwrap();
        let (node_id_le, succ_le) = seq_iter_le.next().unwrap();
        let succ_be = succ_be.collect::<Vec<_>>();
        let succ_le = succ_le.collect::<Vec<_>>();
        let succ = succ.into_iter().collect::<Vec<_>>();
        assert_eq!(node_id, node_id_be);
        assert_eq!(node_id_be, node_id_le);
        assert_eq!(
            seq_iter_be.bit_pos().unwrap(),
            seq_iter_le.bit_pos().unwrap()
        );
        assert_eq!(succ_be, succ_le);
        assert_eq!(succ, succ_be);
        offsets.push(seq_iter_be.bit_pos().unwrap());
        efb.push(seq_iter_be.bit_pos().unwrap() as usize).unwrap();
    }
    // build elias-fano
    let ef = efb.build();

    // verify that elias-fano has the right values
    assert_eq!(IndexedDict::len(&ef), offsets.len());
    for (i, offset) in offsets.iter().enumerate() {
        assert_eq!(ef.get(i as usize) as u64, *offset);
    }

    // create code reader builders
    let codes_reader_be = <DynCodesDecoderFactory<BE, _, _>>::new(
        MemoryFactory::from_data(data_be),
        MemCase::from(ef.clone()),
        comp_flags.clone(),
    )
    .unwrap();
    let codes_reader_le = <DynCodesDecoderFactory<LE, _, _>>::new(
        MemoryFactory::from_data(data_le),
        MemCase::from(ef.clone()),
        comp_flags.clone(),
    )
    .unwrap();

    // Create the two bvgraphs
    let graph_be: BVGraph<_> = BVGraph::new(
        codes_reader_be,
        graph.num_nodes(),
        graph.num_arcs(),
        comp_flags.compression_window,
        comp_flags.min_interval_length,
    );
    let graph_le: BVGraph<_> = BVGraph::new(
        codes_reader_le,
        graph.num_nodes(),
        graph.num_arcs(),
        comp_flags.compression_window,
        comp_flags.min_interval_length,
    );

    // Compare the three graphs
    assert_eq!(graph.num_arcs(), graph_be.num_arcs());
    assert_eq!(graph.num_arcs(), graph_le.num_arcs());

    assert_eq!(graph.num_nodes(), graph_be.num_nodes());
    assert_eq!(graph.num_nodes(), graph_le.num_nodes());

    for node_id in 0..graph.num_nodes() {
        assert_eq!(graph.outdegree(node_id), graph_be.outdegree(node_id));
        assert_eq!(graph.outdegree(node_id), graph_le.outdegree(node_id));

        let true_successors = graph.successors(node_id).into_iter().collect::<Vec<_>>();
        let be_successors = graph_be.successors(node_id).collect::<Vec<_>>();
        let le_successors = graph_le.successors(node_id).collect::<Vec<_>>();

        assert_eq!(true_successors, be_successors);
        assert_eq!(true_successors, le_successors);
    }
}
