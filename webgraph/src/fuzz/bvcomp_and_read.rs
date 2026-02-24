/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Fuzz in-memory compression and reading of bvcomp graphs. This is fast but
//! uses low-level constructs.

use crate::fuzz::utils::CompFlagsFuzz;
use crate::prelude::*;
use arbitrary::Arbitrary;
use dsi_bitstream::prelude::*;
use lender::prelude::*;
use sux::prelude::*;
use sux::traits::IndexedSeq;

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
    let graph = BTreeGraph::from_arcs(edges);
    // Compress in big endian
    let mut codes_data_be: Vec<u64> = Vec::new();
    let mut offsets_be: Vec<u8> = vec![];
    {
        let bit_writer = <BufBitWriter<BE, _>>::new(MemWordWriterVec::new(&mut codes_data_be));
        let codes_writer = <DynCodesEncoder<BE, _>>::new(bit_writer, &comp_flags).unwrap();
        let offsets_writer = OffsetsWriter::from_write(&mut offsets_be, true).unwrap();
        let mut bvcomp = BvComp::new(
            codes_writer,
            offsets_writer,
            comp_flags.compression_window,
            comp_flags.max_ref_count,
            comp_flags.min_interval_length,
            0,
        );
        bvcomp.extend(graph.iter()).unwrap();
        bvcomp.flush().unwrap();
    }
    // Compress in little endian
    let mut codes_data_le: Vec<u64> = Vec::new();
    let mut offsets_le: Vec<u8> = vec![];
    {
        let bit_writer = <BufBitWriter<LE, _>>::new(MemWordWriterVec::new(&mut codes_data_le));
        let codes_writer = <DynCodesEncoder<LE, _>>::new(bit_writer, &comp_flags).unwrap();
        let offsets_writer = OffsetsWriter::from_write(&mut offsets_le, true).unwrap();
        let mut bvcomp = BvComp::new(
            codes_writer,
            offsets_writer,
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
        <EmptyDict<usize, usize>>::default().into(),
        comp_flags,
    )
    .unwrap();
    let codes_reader_le = <DynCodesDecoderFactory<LE, _, _>>::new(
        MemoryFactory::from_data(data_le),
        <EmptyDict<usize, usize>>::default().into(),
        comp_flags,
    )
    .unwrap();

    // test sequential graphs and build the offsets
    let mut efb = EliasFanoBuilder::new(
        graph.num_nodes() + 1,
        (data_be.len() + 1) * 8 * core::mem::size_of::<u32>(),
    );
    let mut offsets = Vec::with_capacity(graph.num_nodes() + 1);
    offsets.push(0);
    efb.push(0);

    // create seq graphs
    let seq_graph_be = BvGraphSeq::new(
        codes_reader_be,
        graph.num_nodes(),
        Some(graph.num_arcs()),
        comp_flags.compression_window,
        comp_flags.min_interval_length,
    );
    let seq_graph_le = BvGraphSeq::new(
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

    let mut offsets_reader_be =
        BufBitReader::<BE, _>::new(<WordAdapter<u32, _>>::new(offsets_be.as_slice()));
    let mut offsets_reader_le =
        BufBitReader::<BE, _>::new(<WordAdapter<u32, _>>::new(offsets_le.as_slice()));
    assert_eq!(offsets_reader_be.read_gamma().unwrap(), 0);
    assert_eq!(offsets_reader_le.read_gamma().unwrap(), 0);

    // verify that they are the same and build the offsets
    let mut cumulative_offset_be = 0;
    let mut cumulative_offset_le = 0;
    for _ in 0..graph.num_nodes() {
        let (node_id, succ) = seq_iter.next().unwrap();
        let (node_id_be, succ_be) = seq_iter_be.next().unwrap();
        let (node_id_le, succ_le) = seq_iter_le.next().unwrap();
        let succ_be = succ_be.collect::<Vec<_>>();
        let succ_le = succ_le.collect::<Vec<_>>();
        let succ = succ.into_iter().collect::<Vec<_>>();
        assert_eq!(node_id, node_id_be);
        assert_eq!(node_id_be, node_id_le);
        assert_eq!(succ_be, succ_le);
        assert_eq!(succ, succ_be);

        let iter_offset_be = seq_iter_be.bit_pos().unwrap();
        let iter_offset_le = seq_iter_le.bit_pos().unwrap();
        assert_eq!(iter_offset_be, iter_offset_le);

        let offset_be = offsets_reader_be.read_gamma().unwrap();
        let offset_le = offsets_reader_le.read_gamma().unwrap();
        cumulative_offset_be += offset_be;
        cumulative_offset_le += offset_le;
        assert_eq!(cumulative_offset_be as usize, iter_offset_be as usize);
        assert_eq!(cumulative_offset_le as usize, iter_offset_le as usize);

        offsets.push(seq_iter_be.bit_pos().unwrap());
        efb.push(seq_iter_be.bit_pos().unwrap() as usize);
    }

    let mut seq_iter_be = seq_graph_be.offset_deg_iter();
    let mut seq_iter_le = seq_graph_le.offset_deg_iter();
    // verify that they are the same and build the offsets
    for node_id in 0..graph.num_nodes() {
        let deg = graph.successors(node_id).count();
        let (offset_be, deg_be) = seq_iter_be.next().unwrap();
        let (offset_le, deg_le) = seq_iter_le.next().unwrap();
        assert_eq!(deg, deg_be);
        assert_eq!(deg_be, deg_le);
        assert_eq!(offset_be, offset_le);
    }
    // build elias-fano
    let ef = efb.build();
    let ef: EF = unsafe { ef.map_high_bits(SelectAdaptConst::<_, _, 12, 4>::new) };

    // verify that elias-fano has the right values
    assert_eq!(ef.len(), offsets.len());
    for (i, offset) in offsets.iter().enumerate() {
        assert_eq!(ef.get(i) as u64, *offset);
    }

    // create code reader builders
    let codes_reader_be = <DynCodesDecoderFactory<BE, _, _>>::new(
        MemoryFactory::from_data(data_be),
        ef.clone().into(),
        comp_flags,
    )
    .unwrap();
    let codes_reader_le = <DynCodesDecoderFactory<LE, _, _>>::new(
        MemoryFactory::from_data(data_le),
        ef.clone().into(),
        comp_flags,
    )
    .unwrap();

    // Creates the two bvgraphs
    let graph_be: BvGraph<_> = BvGraph::new(
        codes_reader_be,
        graph.num_nodes(),
        graph.num_arcs(),
        comp_flags.compression_window,
        comp_flags.min_interval_length,
    );
    let graph_le: BvGraph<_> = BvGraph::new(
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

        let true_successors = graph.successors(node_id).collect::<Vec<_>>();
        let be_successors = graph_be.successors(node_id).collect::<Vec<_>>();
        let le_successors = graph_le.successors(node_id).collect::<Vec<_>>();

        assert_eq!(true_successors, be_successors);
        assert_eq!(true_successors, le_successors);
    }
}
