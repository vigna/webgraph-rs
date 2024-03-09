/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::path::{Path, PathBuf};

use anyhow::Result;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use lender::*;
use webgraph::prelude::*;

fn logger_init() {
    env_logger::builder().is_test(true).try_init().unwrap();
}
#[test]
fn test_par_bvcomp() -> Result<()> {
    logger_init();
    _test_par_bvcomp("tests/data/cnr-2000")?;
    _test_par_bvcomp("tests/data/cnr-2000-hc")?;
    Ok(())
}

fn _test_par_bvcomp(basename: &str) -> Result<()> {
    let comp_flags = CompFlags::default();
    let tmp_basename = PathBuf::from(String::from(basename) + "-par");

    // load the graph
    let graph = webgraph::graphs::bvgraph::sequential::BVGraphSeq::with_basename(basename)
        .endianness::<BE>()
        .load()?;

    let mut graph_filename = PathBuf::from(basename);
    graph_filename.set_extension("graph");

    let expected_size = graph_filename.metadata()?.len();
    for thread_num in 1..10 {
        log::info!("Testing with {} threads", thread_num);
        // create a threadpool and make the compression use it, this way
        // we can test with different number of threads
        let start = std::time::Instant::now();
        // recompress the graph in parallel
        BVComp::parallel::<BE, _>(
            &tmp_basename,
            &graph,
            graph.num_nodes(),
            comp_flags,
            thread_num,
            temp_dir(std::env::temp_dir()),
        )
        .unwrap();
        log::info!("The compression took: {}s", start.elapsed().as_secs_f64());

        let found_size = std::fs::File::open(suffix_path(&tmp_basename, ".graph"))?
            .metadata()?
            .len();

        if (found_size as f64) > (expected_size as f64) * 1.1 {
            panic!(
                "The compressed graph is too big: {} > {}",
                found_size, expected_size
            );
        }

        let comp_graph =
            webgraph::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&tmp_basename)
                .endianness::<BE>()
                .load()?;
        let mut iter = comp_graph.iter();

        let mut pr = ProgressLogger::default();
        pr.display_memory(true)
            .item_name("node")
            .expected_updates(Some(graph.num_nodes()));
        pr.start("Checking that the newly compressed graph is equivalent to the original one...");

        let mut iter_nodes = graph.iter();
        while let Some((node, succ_iter)) = iter_nodes.next() {
            let (new_node, new_succ_iter) = iter.next().unwrap();
            assert_eq!(node, new_node);
            let succ = succ_iter.collect::<Vec<_>>();
            let new_succ = new_succ_iter.collect::<Vec<_>>();
            assert_eq!(succ, new_succ, "Node {} differs", node);
            pr.light_update();
        }

        pr.done();
        // cancel the file at the end
        std::fs::remove_file(suffix_path(&tmp_basename, ".graph"))?;
        std::fs::remove_file(suffix_path(&tmp_basename, ".properties"))?;
        log::info!("\n");
    }

    Ok(())
}
