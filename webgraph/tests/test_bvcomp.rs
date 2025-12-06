/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::ExactSizeLender;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
};
use tempfile::NamedTempFile;

use anyhow::Result;
use dsi_bitstream::prelude::{factory::CodesReaderFactoryHelper, *};
use std::path::Path;
use webgraph::{graphs::bvgraph, graphs::random::ErdosRenyi, prelude::*};
use Codes::{Delta, Gamma, Unary, Zeta};

#[cfg_attr(feature = "slow_tests", test)]
#[cfg_attr(not(feature = "slow_tests"), allow(dead_code))]
fn test_bvcomp_slow() -> Result<()> {
    env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Info)
        .try_init()?;
    _test_bvcomp_slow::<LE>().and(_test_bvcomp_slow::<BE>())
}

fn _test_bvcomp_slow<E: Endianness>() -> Result<()>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    MmapHelper<u32>: for<'a> CodesReaderFactoryHelper<E, CodesReader<'a>: BitSeek>,
    BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
    BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
{
    let tmp_file = NamedTempFile::new()?;
    let tmp_path = tmp_file.path();
    let seq_graph = BTreeGraph::from_lender(ErdosRenyi::new(100, 0.1, 0).iter());
    for compression_window in [0, 1, 3, 16] {
        for max_ref_count in [0, 1, 3, usize::MAX] {
            for min_interval_length in [0, 1, 3] {
                for outdegrees in [Unary, Gamma, Delta] {
                    for references in [Unary, Gamma, Delta] {
                        for blocks in [Unary, Gamma, Delta] {
                            for intervals in [Unary, Gamma, Delta] {
                                for residuals in [Gamma, Delta, Zeta { k: 2 }, Zeta { k: 3 }] {
                                    eprintln!();
                                    eprintln!(
                                        "Testing with outdegrees = {:?}, references = {:?}, blocks = {:?}, intervals = {:?}, residuals = {:?}, compression_window = {}, max_ref_count = {}, min_interval_length = {}",
                                        outdegrees,
                                        references,
                                        blocks,
                                        intervals,
                                        residuals,
                                        compression_window,
                                        max_ref_count,
                                        min_interval_length,
                                    );
                                    let compression_flags = CompFlags {
                                        outdegrees,
                                        references,
                                        blocks,
                                        intervals,
                                        residuals,
                                        min_interval_length,
                                        compression_window,
                                        max_ref_count,
                                    };

                                    _test_body::<E, _, _>(tmp_path, &seq_graph, compression_flags)?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    // Cleanup
    std::fs::remove_file(tmp_path)?;
    std::fs::remove_file(tmp_path.with_added_extension(GRAPH_EXTENSION))?;
    std::fs::remove_file(tmp_path.with_added_extension(OFFSETS_EXTENSION))?;
    std::fs::remove_file(tmp_path.with_added_extension(PROPERTIES_EXTENSION))?;
    Ok(())
}

fn _test_body<E: Endianness, G: SequentialGraph + SplitLabeling, P: AsRef<Path>>(
    tmp_path: P,
    seq_graph: &G,
    comp_flags: CompFlags,
) -> Result<()>
where
    for<'a> G::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
    for<'a> <G as SplitLabeling>::SplitLender<'a>: ExactSizeLender,
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    MmapHelper<u32>: for<'a> CodesReaderFactoryHelper<E, CodesReader<'a>: BitSeek>,
    BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
    BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
{
    let tmp_path = tmp_path.as_ref();
    let mut bvcomp = BvComp::with_basename(tmp_path).with_comp_flags(comp_flags);
    bvcomp.comp_graph::<E>(seq_graph)?;
    let new_graph = BvGraphSeq::with_basename(tmp_path)
        .endianness::<E>()
        .load()?;
    labels::eq_sorted(seq_graph, &new_graph)?;
    bvgraph::check_offsets(&new_graph, tmp_path)?;

    bvcomp.par_comp_graph::<E>(seq_graph)?;
    let new_graph = BvGraphSeq::with_basename(tmp_path)
        .endianness::<E>()
        .load()?;
    labels::eq_sorted(seq_graph, &new_graph)?;
    bvgraph::check_offsets(&new_graph, tmp_path)?;

    for chunk_size in [1, 10, 1000] {
        let mut bvcompz = BvCompZ::with_basename(tmp_path)
            .with_comp_flags(comp_flags)
            .with_chunk_size(chunk_size);
        bvcompz.comp_graph::<E>(seq_graph)?;
        let new_graph = BvGraphSeq::with_basename(tmp_path)
            .endianness::<E>()
            .load()?;
        labels::eq_sorted(seq_graph, &new_graph)?;
        bvgraph::check_offsets(&new_graph, tmp_path)?;

        bvcompz.par_comp_graph::<E>(seq_graph)?;
        let new_graph = BvGraphSeq::with_basename(tmp_path)
            .endianness::<E>()
            .load()?;
        labels::eq_sorted(seq_graph, &new_graph)?;
        bvgraph::check_offsets(&new_graph, tmp_path)?;
    }
    Ok(())
}

pub struct EncoderValidator<E: Encode> {
    encoder: E,
    start_nodes: usize,
    end_nodes: usize,
    flush: bool,
    // encoder has to be flushed, while estimator does not
    is_estimator: bool,
}

impl<E: Encode> EncoderValidator<E> {
    pub fn new(encoder: E) -> Self {
        Self {
            encoder,
            start_nodes: 0,
            end_nodes: 0,
            flush: false,
            is_estimator: false,
        }
    }
    pub fn new_estimator(encoder: E) -> Self {
        Self {
            encoder,
            start_nodes: 0,
            end_nodes: 0,
            flush: false,
            is_estimator: true,
        }
    }
}

impl<E: Encode> core::ops::Drop for EncoderValidator<E> {
    fn drop(&mut self) {
        assert_eq!(self.start_nodes, self.end_nodes);
        if !self.is_estimator {
            assert!(self.flush, "flush not called");
        }
    }
}

impl<E: Encode> Encode for EncoderValidator<E> {
    type Error = E::Error;
    fn start_node(&mut self, node: usize) -> std::prelude::v1::Result<usize, Self::Error> {
        assert_eq!(self.start_nodes, self.end_nodes);
        self.start_nodes += 1;
        self.encoder.start_node(node)
    }
    fn end_node(&mut self, node: usize) -> std::prelude::v1::Result<usize, Self::Error> {
        self.end_nodes += 1;
        assert_eq!(self.start_nodes, self.end_nodes);
        self.encoder.end_node(node)
    }
    fn flush(&mut self) -> std::prelude::v1::Result<usize, Self::Error> {
        assert!(!self.flush, "flush called twice");
        self.flush = true;
        self.encoder.flush()
    }
    fn write_outdegree(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_outdegree(value)
    }
    fn write_reference_offset(
        &mut self,
        value: u64,
    ) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_reference_offset(value)
    }
    fn write_block_count(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_block_count(value)
    }
    fn write_block(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_block(value)
    }
    fn write_interval_count(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_interval_count(value)
    }
    fn write_interval_start(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_interval_start(value)
    }
    fn write_interval_len(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_interval_len(value)
    }
    fn write_first_residual(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_first_residual(value)
    }
    fn write_residual(&mut self, value: u64) -> std::prelude::v1::Result<usize, Self::Error> {
        self.encoder.write_residual(value)
    }
}

impl<E: EncodeAndEstimate> EncodeAndEstimate for EncoderValidator<E> {
    type Estimator<'a>
        = EncoderValidator<E::Estimator<'a>>
    where
        Self: 'a;

    fn estimator(&mut self) -> Self::Estimator<'_> {
        EncoderValidator::new_estimator(self.encoder.estimator())
    }
}
