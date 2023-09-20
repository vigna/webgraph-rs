/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::{fs::File, io::BufWriter};
use tempfile::NamedTempFile;

const NODES: usize = 325557;

use anyhow::Result;
use dsi_bitstream::{
    prelude::{
        BufferedBitStreamRead, BufferedBitStreamWrite,
        Code::{Delta, Gamma, Unary, Zeta},
        FileBackend, MemWordReadInfinite,
    },
    traits::BE,
};
use dsi_progress_logger::ProgressLogger;
use webgraph::{
    graph::bvgraph::{
        BVComp, CompFlags, DynamicCodesReader, DynamicCodesWriter, WebgraphSequentialIter,
    },
    prelude::*,
    utils::MmapBackend,
};

#[cfg_attr(feature = "slow_tests", test)]
#[cfg_attr(not(feature = "slow_tests"), allow(dead_code))]
fn test_bvcomp_slow() -> Result<()> {
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let tmp_file = NamedTempFile::new()?;
    let tmp_path = tmp_file.path();
    for outdegrees in [Unary, Gamma, Delta] {
        for references in [Unary, Gamma, Delta] {
            for blocks in [Unary, Gamma, Delta] {
                for intervals in [Unary, Gamma, Delta] {
                    for residuals in [Unary, Gamma, Delta, Zeta { k: 3 }] {
                        for compression_window in [0, 1, 2, 4, 7, 8, 10] {
                            for min_interval_length in [0, 2, 4, 7, 8, 10] {
                                for max_ref_count in [0, 1, 2, 3] {
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

                                    let seq_graph =
                                        webgraph::graph::bvgraph::load_seq("tests/data/cnr-2000")?;

                                    let writer = <DynamicCodesWriter<BE, _>>::new(
                                        <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(
                                            BufWriter::new(File::create(tmp_path)?),
                                        )),
                                        &compression_flags,
                                    );
                                    let mut bvcomp = BVComp::new(
                                        writer,
                                        compression_window,
                                        min_interval_length,
                                        max_ref_count,
                                        0,
                                    );

                                    let mut pl = ProgressLogger::default().display_memory();
                                    pl.item_name = "node";
                                    pl.start("Compressing...");
                                    pl.expected_updates = Some(NODES);

                                    for (_, iter) in &seq_graph {
                                        bvcomp.push(iter)?;
                                        pl.light_update();
                                    }

                                    pl.done();
                                    bvcomp.flush()?;

                                    let code_reader = DynamicCodesReader::new(
                                        BufferedBitStreamRead::<BE, u64, _>::new(
                                            MemWordReadInfinite::<u32, _>::new(MmapBackend::load(
                                                tmp_path,
                                                mmap_rs::MmapFlags::empty(),
                                            )?),
                                        ),
                                        &compression_flags,
                                    )?;
                                    let seq_reader1 = WebgraphSequentialIter::new(
                                        code_reader,
                                        compression_flags.compression_window,
                                        compression_flags.min_interval_length,
                                        NODES,
                                    );

                                    pl.start("Checking equality...");
                                    for ((_, iter0), (_, iter1)) in
                                        seq_graph.iter_nodes().zip(seq_reader1)
                                    {
                                        assert_eq!(
                                            iter0.collect::<Vec<_>>(),
                                            iter1.collect::<Vec<_>>()
                                        );
                                        pl.light_update();
                                    }
                                    pl.done();
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
