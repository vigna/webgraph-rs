/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::{fs::File, io::BufWriter};
use tempfile::NamedTempFile;

use anyhow::Result;
use dsi_bitstream::prelude::{factory::CodesReaderFactoryHelper, *};
use std::path::Path;
use webgraph::{graphs::random::ErdosRenyi, prelude::*};
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
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let tmp_file = NamedTempFile::new()?;
    let tmp_path = tmp_file.path();
    let seq_graph = ErdosRenyi::new(100, 0.1, 0);
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
    std::fs::remove_file(tmp_path.with_added_extension("graph"))?;
    std::fs::remove_file(tmp_path.with_added_extension("offsets"))?;
    std::fs::remove_file(tmp_path.with_added_extension("properties"))?;
    Ok(())
}

fn _test_body<E: Endianness, G: SequentialGraph, P: AsRef<Path>>(
    tmp_path: P,
    seq_graph: &G,
    comp_flags: CompFlags,
) -> Result<()>
where
    for<'a> G::Lender<'a>: SortedLender,
    for<'a, 'b> LenderIntoIter<'b, G::Lender<'a>>: SortedIterator,
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let tmp_path = tmp_path.as_ref();
    BvCompConfig::new(tmp_path)
        .with_comp_flags(comp_flags)
        .comp_graph::<E>(&seq_graph)?;
    let new_graph = BvGraphSeq::with_basename(tmp_path)
        .endianness::<E>()
        .load()?;
    labels::eq_sorted(seq_graph, &new_graph)?;

    for chunk_size in [1, 10, 1000] {
        BvCompConfig::new(tmp_path)
            .with_comp_flags(comp_flags)
            .with_chunk_size(chunk_size)
            .comp_graph::<E>(&seq_graph)?;
        let new_graph = BvGraphSeq::with_basename(tmp_path)
            .endianness::<E>()
            .load()?;
        labels::eq_sorted(seq_graph, &new_graph)?;
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
