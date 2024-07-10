/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::graphs::arc_list_graph;
use crate::prelude::proj::Left;
use crate::prelude::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, SortPairs};
use crate::prelude::{BitDeserializer, BitSerializer, LabeledSequentialGraph, SequentialGraph};
use crate::traits::graph::UnitLabelGraph;
use anyhow::Result;
use dsi_bitstream::traits::NE;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use tempfile::Builder;

/// Returns the transpose of the provided labeled graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameters, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn transpose_labeled<
    S: BitSerializer<NE, BitWriter> + Clone,
    D: BitDeserializer<NE, BitReader> + Clone + 'static,
>(
    graph: &impl LabeledSequentialGraph<S::SerType>,
    batch_size: usize,
    serializer: S,
    deserializer: D,
) -> Result<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<D>, D::DeserType>>>
where
    S::SerType: Send + Sync + Copy,
    D::DeserType: Clone + Copy,
{
    let dir = Builder::new().prefix("Transpose").tempdir()?;
    let mut sorted = SortPairs::new_labeled(batch_size, dir.path(), serializer, deserializer)?;

    let mut pl = progress_logger!(
        item_name = "node",
        expected_updates = Some(graph.num_nodes()),
        display_memory = true
    );
    pl.start("Creating batches...");
    // create batches of sorted edges
    for_!( (src, succ) in graph.iter() {
        for (dst, l) in succ {
            sorted.push_labeled(dst, src, l)?;
        }
        pl.light_update();
    });
    // merge the batches
    let sorted = arc_list_graph::ArcListGraph::new_labeled(graph.num_nodes(), sorted.iter()?);
    pl.done();

    Ok(sorted)
}

/// Returns the transpose of the provided graph as a [sequential
/// graph](crate::traits::SequentialGraph).
///
/// For the meaning of the additional parameter, see
/// [`SortPairs`](crate::prelude::sort_pairs::SortPairs).
#[allow(clippy::type_complexity)]
pub fn transpose(
    graph: impl SequentialGraph,
    batch_size: usize,
) -> Result<Left<arc_list_graph::ArcListGraph<KMergeIters<BatchIterator<()>, ()>>>> {
    Ok(Left(transpose_labeled(
        &UnitLabelGraph(graph),
        batch_size,
        (),
        (),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_transposition() -> anyhow::Result<()> {
        use crate::graphs::vec_graph::VecGraph;
        let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
        let g = Left(VecGraph::from_arc_list(arcs));

        let trans = transpose(&g, 3)?;
        let g2 = Left(VecGraph::from_lender(&trans));

        let trans = transpose(g2, 3)?;
        let g3 = Left(VecGraph::from_lender(&trans));

        assert_eq!(g, g3);
        Ok(())
    }

    #[test]
    fn test_transposition_labeled() -> anyhow::Result<()> {
        use dsi_bitstream::codes::{GammaRead, GammaWrite};
        use dsi_bitstream::traits::{BitRead, BitWrite};

        use crate::graphs::vec_graph::VecGraph;
        use crate::traits::SequentialLabeling;

        #[derive(Clone, Copy, PartialEq, Debug)]
        struct Payload(f64);

        #[derive(Clone, Copy, PartialEq, Debug)]
        struct BD {}

        impl BitDeserializer<NE, BitReader> for BD
        where
            BitReader: GammaRead<NE>,
        {
            type DeserType = Payload;

            fn deserialize(
                &self,
                bitstream: &mut BitReader,
            ) -> Result<Self::DeserType, <BitReader as BitRead<NE>>::Error> {
                let mantissa = bitstream.read_gamma()?;
                let exponent = bitstream.read_gamma()?;
                let result = f64::from_bits((exponent << 53) | mantissa);
                Ok(Payload(result))
            }
        }

        #[derive(Clone, Copy, PartialEq, Debug)]
        struct BS {}

        impl BitSerializer<NE, BitWriter> for BS
        where
            BitWriter: GammaWrite<NE>,
        {
            type SerType = Payload;

            fn serialize(
                &self,
                value: &Self::SerType,
                bitstream: &mut BitWriter,
            ) -> Result<usize, <BitWriter as BitWrite<NE>>::Error> {
                let value = value.0.to_bits();
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

        // TODO pass &arcs
        let g = VecGraph::<Payload>::from_labeled_arc_list(arcs);

        let trans = transpose_labeled(&g, 2, BS {}, BD {})?;
        let g2 = VecGraph::<Payload>::from_labeled_lender(trans.iter());

        let trans = transpose_labeled(&g2, 2, BS {}, BD {})?;
        let g3 = VecGraph::<Payload>::from_labeled_lender(trans.iter());

        let g4 = VecGraph::from_labeled_lender(g.iter());

        assert_eq!(g3, g4);

        Ok(())
    }
}
