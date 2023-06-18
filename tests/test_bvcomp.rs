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
use mmap_rs::MmapOptions;
use webgraph::{
    bvgraph::{BVComp, CompFlags, DynamicCodesReader, DynamicCodesWriter, WebgraphSequentialIter},
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
                                    };

                                    let seq_reader =
                                        WebgraphSequentialIter::load_mapped("tests/data/cnr-2000")?;

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
                                    );

                                    let mut pl = ProgressLogger::default().display_memory();
                                    pl.item_name = "node".into();
                                    pl.start("Compressing...");
                                    pl.expected_updates = Some(NODES);

                                    for (_, iter) in seq_reader {
                                        bvcomp.push(iter)?;
                                        pl.light_update();
                                    }

                                    pl.done();
                                    bvcomp.flush()?;

                                    let seq_reader0 =
                                        WebgraphSequentialIter::load_mapped("tests/data/cnr-2000")?;

                                    let path = std::path::Path::new(tmp_path);
                                    let file_len = path.metadata()?.len();
                                    let file = std::fs::File::open(path)?;

                                    let data = unsafe {
                                        MmapOptions::new(file_len as _)
                                            .unwrap()
                                            .with_file(file, 0)
                                            .map()
                                            .unwrap()
                                    };

                                    let code_reader = DynamicCodesReader::new(
                                        BufferedBitStreamRead::<BE, u64, _>::new(
                                            MemWordReadInfinite::<u32, _>::new(MmapBackend::new(
                                                data,
                                            )),
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
                                    for ((_, iter0), (_, iter1)) in seq_reader0.zip(seq_reader1) {
                                        itertools::assert_equal(iter0, iter1);
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
