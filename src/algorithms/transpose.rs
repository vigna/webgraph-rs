/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::for_iter;
use crate::graph::arc_list_graph;
use crate::traits::SequentialGraph;
use crate::utils::{BatchIterator, KMergeIters, SortPairs};
use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
use hrtb_lending_iterator::*;
/// Create transpose the graph and return a sequential graph view of it
#[allow(clippy::type_complexity)]
pub fn transpose(
    graph: &impl SequentialGraph,
    batch_size: usize,
) -> Result<
    arc_list_graph::ArcListGraph<
        std::iter::Map<KMergeIters<BatchIterator>, fn((usize, usize, ())) -> (usize, usize)>,
    >,
> {
    let dir = tempfile::tempdir()?;
    let mut sorted = SortPairs::new(batch_size, dir.into_path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name = "node";
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_iter! { (src, succ) in graph.iter() =>
        for dst in succ {
            sorted.push(dst, src)?;
        }
        pl.light_update();
    }
    // merge the batches
    let map: fn((usize, usize, ())) -> (usize, usize) = |(src, dst, _)| (src, dst);
    let sorted = arc_list_graph::ArcListGraph::new(graph.num_nodes(), sorted.iter()?.map(map));
    pl.done();

    Ok(sorted)
}

#[cfg(test)]
#[cfg_attr(test, test)]
fn test_transposition() -> anyhow::Result<()> {
    use crate::graph::{arc_list_graph::ArcListGraph, vec_graph::VecGraph};
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = VecGraph::from_arc_list(&arcs);

    let trans = transpose(&g, 3)?;
    let g2 = VecGraph::from_node_iter::<&ArcListGraph<_>>(&trans);

    let trans = transpose(&g2, 3)?;
    let g3 = VecGraph::from_node_iter::<&ArcListGraph<_>>(&trans);

    assert_eq!(g, g3);
    Ok(())
}
/*
#[cfg(test)]
#[cfg_attr(test, test)]
fn test_transposition_labeled() -> anyhow::Result<()> {
    use crate::graph::vec_graph::VecGraph;
    use dsi_bitstream::prelude::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    struct Payload(f64);

    impl Label for Payload {
        fn from_bitstream<E: Endianness, B: CodeRead<E>>(bitstream: &mut B) -> Result<Self> {
            let mantissa = bitstream.read_gamma()?;
            let exponent = bitstream.read_gamma()?;
            let result = f64::from_bits((exponent << 53) | mantissa);
            Ok(Payload(result))
        }

        fn to_bitstream<E: Endianness, B: CodeWrite<E>>(
            &self,
            bitstream: &mut B,
        ) -> Result<usize> {
            let value = self.0 as u64;
            let mantissa = value & ((1 << 53) - 1);
            let exponent = value >> 53;
            let mut written_bits = 0;
            written_bits += bitstream.write_gamma(mantissa)?;
            written_bits += bitstream.write_gamma(exponent)?;
            Ok(written_bits)
        }
    }
    let arcs = vec![
        (0, 1, Payload(1.0)),
        (0, 2, Payload(f64::EPSILON)),
        (1, 2, Payload(2.0)),
        (1, 3, Payload(f64::NAN)),
        (2, 4, Payload(f64::INFINITY)),
        (3, 4, Payload(f64::NEG_INFINITY)),
    ];

    // test transposition without labels
    let g = VecGraph::from_arc_and_label_list(&arcs);

    let trans = transpose(&g, 3)?;
    let g2 = VecGraph::from_node_iter(trans.iter_nodes());

    let trans = transpose(&g2, 3)?;
    let g3 = VecGraph::from_node_iter(trans.iter_nodes());

    let g4 = VecGraph::from_node_iter(g.iter_nodes());

    assert_eq!(g3, g4);

    //// test transposition with labels
    //let trans = transpose_labeled(&g, 3)?;
    //let g5 = VecGraph::from_labeled_node_iter(trans.iter_nodes());
    //
    //let trans = transpose_labeled(&g5, 3)?;
    //let g6 = VecGraph::from_labeled_node_iter(trans.iter_nodes());
    //
    //assert_eq!(g, g6);
    Ok(())
}
*/
