/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

mod common;

use Codes::{Delta, Gamma, Unary, Zeta};
use anyhow::{Result, ensure};
use dsi_bitstream::prelude::{factory::CodesReaderFactoryHelper, *};
use std::path::Path;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
};
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::{graphs::bvgraph, graphs::random::ErdosRenyi, prelude::*};

#[cfg_attr(feature = "slow_tests", test)]
#[cfg_attr(not(feature = "slow_tests"), allow(dead_code))]
fn test_bvcomp_slow() -> Result<()> {
    common::init_logger();
    _test_bvcomp_slow::<LE>().and(_test_bvcomp_slow::<BE>())
}

fn _test_bvcomp_slow<E: Endianness>() -> Result<()>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    BufBitReader<E, WordAdapter<u32, BufReader<File>>>: BitRead<E>,
    MmapHelper<u32>: for<'a> CodesReaderFactoryHelper<E, CodesReader<'a>: BitSeek>,
{
    let (_dir, basename) = common::tmp_basename()?;
    let graph = VecGraph::from_lender(ErdosRenyi::new(100, 0.1, 0).iter());
    for compression_window in [0, 1, 3, 16] {
        for max_ref_count in [0, 1, 3, usize::MAX] {
            for min_interval_length in [0, 1, 3] {
                for outdegrees in [Unary, Gamma, Delta] {
                    for references in [Unary, Gamma, Delta] {
                        for blocks in [Unary, Gamma, Delta] {
                            for intervals in [Unary, Gamma, Delta] {
                                for residuals in [Gamma, Delta, Zeta(2), Zeta(3)] {
                                    let comp_flags = CompFlags {
                                        outdegrees,
                                        references,
                                        blocks,
                                        intervals,
                                        residuals,
                                        min_interval_length,
                                        compression_window,
                                        max_ref_count,
                                    };
                                    eprintln!("Testing with {comp_flags:?}");
                                    _test_body::<E>(&basename, &graph, comp_flags)?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Compresses `graph` sequentially and in parallel, with both compressors, and
/// checks the result.
fn _test_body<E: Endianness>(basename: &Path, graph: &VecGraph, comp_flags: CompFlags) -> Result<()>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    BufBitReader<E, WordAdapter<u32, BufReader<File>>>: BitRead<E>,
    MmapHelper<u32>: for<'a> CodesReaderFactoryHelper<E, CodesReader<'a>: BitSeek>,
{
    let check = || -> Result<()> {
        let new_graph = BvGraphSeq::with_basename(basename)
            .endianness::<E>()
            .load()?;
        labels::eq_sorted(graph, &new_graph)?;
        ensure!(bvgraph::check_offsets(&new_graph, basename)?);
        Ok(())
    };

    let mut bvcomp = BvComp::with_basename(basename).comp_flags(comp_flags);
    bvcomp.comp_graph::<E>(graph)?;
    check()?;
    bvcomp.par_comp::<E, _>(graph)?;
    check()?;

    for chunk_size in [1, 10, 1000] {
        let mut bvcompz = BvCompZ::with_basename(basename)
            .comp_flags(comp_flags)
            .chunk_size(chunk_size);
        bvcompz.comp_graph::<E>(graph)?;
        check()?;
        bvcompz.par_comp::<E, _>(graph)?;
        check()?;
    }
    Ok(())
}

#[test]
fn test_bvcompz_push_label_error_propagates() -> Result<()> {
    use webgraph::graphs::bvgraph::DynCodesEncoder;

    let (_dir, basename) = common::tmp_basename()?;
    let writer = buf_bit_writer::from_path::<BE, usize>(basename.with_extension("graph"))?;
    let encoder = <DynCodesEncoder<BE, _>>::new(writer, &CompFlags::default())?;
    let mut comp = BvCompZ::new(
        encoder,
        OffsetsWriter::from_path(basename.with_extension("offsets"), false)?,
        7,
        4,
        3,
        2,
        0,
        common::FailingStore::<usize>(std::marker::PhantomData),
        NoMerge,
    );
    // Regression: label-store errors panicked instead of propagating.
    assert!(comp.push([(1usize, 42usize)]).is_err());
    Ok(())
}
