/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_bitstream::codes::{GammaRead, GammaWrite};
use dsi_bitstream::traits::{BitRead, BitWrite};
use dsi_bitstream::traits::{Endianness, BE};
use webgraph::graphs::vec_graph::LabeledVecGraph;
use webgraph::prelude::{transpose, transpose_labeled, transpose_split};
use webgraph::traits::labels::SequentialLabeling;
use webgraph::traits::{graph, BitDeserializer, BitSerializer};
use webgraph::utils::gaps::GapsCodec;
use webgraph::utils::MemoryUsage;
use webgraph::utils::{BitReader, BitWriter};

#[test]
fn test_transpose() -> anyhow::Result<()> {
    use webgraph::graphs::vec_graph::VecGraph;
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = VecGraph::from_arcs(arcs);

    let trans = transpose(&g, MemoryUsage::BatchSize(3))?;
    let g2 = VecGraph::from_lender(&trans);

    let trans = transpose(g2, MemoryUsage::BatchSize(3))?;
    let g3 = VecGraph::from_lender(&trans);

    graph::eq(&g, &g3)?;
    Ok(())
}

#[test]
fn test_transpose_labeled() -> anyhow::Result<()> {
    #[derive(Clone, Copy, PartialEq, Debug)]
    struct Payload(f64);

    #[derive(Clone, Copy, PartialEq, Debug, Default)]
    struct BD;

    impl<E: Endianness> BitDeserializer<E, BitReader<E>> for BD
    where
        BitReader<E>: GammaRead<E>,
    {
        type DeserType = Payload;

        fn deserialize(
            &self,
            bitstream: &mut BitReader<E>,
        ) -> Result<Self::DeserType, <BitReader<E> as BitRead<E>>::Error> {
            let mantissa = bitstream.read_gamma()?;
            let exponent = bitstream.read_gamma()?;
            let result = f64::from_bits((exponent << 53) | mantissa);
            Ok(Payload(result))
        }
    }

    #[derive(Clone, Copy, PartialEq, Debug, Default)]
    struct BS;

    impl<E: Endianness> BitSerializer<E, BitWriter<E>> for BS
    where
        BitWriter<E>: GammaWrite<E>,
    {
        type SerType = Payload;

        fn serialize(
            &self,
            value: &Self::SerType,
            bitstream: &mut BitWriter<E>,
        ) -> Result<usize, <BitWriter<E> as BitWrite<E>>::Error> {
            let value = value.0.to_bits();
            let mantissa = value & ((1 << 53) - 1);
            let exponent = value >> 53;
            let mut written_bits = 0;
            written_bits += bitstream.write_gamma(mantissa)?;
            written_bits += bitstream.write_gamma(exponent)?;
            Ok(written_bits)
        }
    }
    let arcs = [
        ((0, 1), Payload(1.0)),
        ((0, 2), Payload(f64::EPSILON)),
        ((1, 2), Payload(2.0)),
        ((2, 4), Payload(f64::INFINITY)),
        ((3, 4), Payload(f64::NEG_INFINITY)),
    ];
    let g = LabeledVecGraph::<Payload>::from_arcs(arcs);

    let trans = transpose_labeled(
        &g,
        MemoryUsage::BatchSize(3),
        GapsCodec::<BE, BS, BD>::new(BS, BD),
    )?;
    let g2 = LabeledVecGraph::<Payload>::from_lender(trans.iter());

    let trans = transpose_labeled(
        &g2,
        MemoryUsage::BatchSize(3),
        GapsCodec::<BE, BS, BD>::new(BS, BD),
    )?;
    let g3 = LabeledVecGraph::<Payload>::from_lender(trans.iter());

    let g4 = LabeledVecGraph::from_lender(g.iter());

    graph::eq_labeled(&g3, &g4)?;
    Ok(())
}

#[test]
fn test_transpose_split() -> anyhow::Result<()> {
    use webgraph::graphs::vec_graph::VecGraph;
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = VecGraph::from_arcs(arcs);

    let trans: Vec<_> = transpose_split(&g, MemoryUsage::BatchSize(3))?.into();
    let mut g2 = VecGraph::new();
    for lender in trans {
        g2.add_lender(lender);
    }

    let trans = transpose(g2, MemoryUsage::BatchSize(3))?;
    let g3 = VecGraph::from_lender(&trans);

    graph::eq(&g, &g3)?;
    Ok(())
}
