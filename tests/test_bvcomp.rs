/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::*;
use std::{fs::File, io::BufWriter};
use tempfile::NamedTempFile;

const NODES: usize = 325557;

use anyhow::Result;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use std::path::Path;
use webgraph::{graphs::random::ErdosRenyi, prelude::*};
use Code::{Delta, Gamma, Unary, Zeta};

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
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodeWrite<E>,
    BufBitReader<E, MemWordReader<u32, MmapHelper<u32>>>: CodeRead<E>,
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
                                        outdegrees, references, blocks, intervals, residuals, compression_window, max_ref_count, min_interval_length,
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

                                    _test_body::<E, _>(tmp_path, &seq_graph, compression_flags)?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    std::fs::remove_file(tmp_path)?;
    Ok(())
}

fn _test_body<E: Endianness, P: AsRef<Path>>(
    tmp_path: P,
    seq_graph: &impl SequentialGraph,
    compression_flags: CompFlags,
) -> Result<()>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodeWrite<E>,
    BufBitReader<E, MemWordReader<u32, MmapHelper<u32>>>: CodeRead<E>,
{
    let writer = EncoderValidator::new(<DynCodesEncoder<E, _>>::new(
        <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(File::create(
            tmp_path.as_ref(),
        )?))),
        &compression_flags,
    ));
    let mut bvcomp = BVComp::new(
        writer,
        compression_flags.compression_window,
        compression_flags.max_ref_count,
        compression_flags.min_interval_length,
        0,
    );

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("node")
        .expected_updates(Some(NODES));

    pl.start("Compressing...");

    // TODO: use LoadConfig
    let mut iter_nodes = seq_graph.iter();
    while let Some((_, iter)) = iter_nodes.next() {
        bvcomp.push(iter)?;
        pl.light_update();
    }

    pl.done();
    bvcomp.flush()?;

    let code_reader = DynCodesDecoder::new(
        BufBitReader::<E, _>::new(MemWordReader::<u32, _>::new(MmapHelper::mmap(
            tmp_path.as_ref(),
            mmap_rs::MmapFlags::empty(),
        )?)),
        &compression_flags,
    )?;
    let mut seq_reader1 = sequential::Iter::new(
        code_reader,
        NODES,
        compression_flags.compression_window,
        compression_flags.min_interval_length,
    );

    pl.start("Checking equality...");
    let mut iter_nodes = seq_graph.iter();
    for _ in 0..seq_graph.num_nodes() {
        let (node0, iter0) = iter_nodes.next().unwrap();
        let (node1, iter1) = seq_reader1.next().unwrap();
        assert_eq!(node0, node1);
        assert_eq!(
            iter0.into_iter().collect::<Vec<_>>(),
            iter1.into_iter().collect::<Vec<_>>()
        );
        pl.light_update();
    }
    pl.done();
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

impl<E: MeasurableEncoder> MeasurableEncoder for EncoderValidator<E> {
    type Estimator<'a> = EncoderValidator<E::Estimator<'a>>
    where
        Self: 'a;

    fn estimator(&mut self) -> Self::Estimator<'_> {
        EncoderValidator::new_estimator(self.encoder.estimator())
    }
}
