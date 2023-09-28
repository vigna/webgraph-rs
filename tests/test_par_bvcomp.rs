/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_bitstream::{
    prelude::{BufBitReader, MemWordReaderInf},
    traits::BigEndian,
};
use dsi_progress_logger::ProgressLogger;
use webgraph::prelude::*;

#[test]
fn test_par_bvcomp() -> Result<()> {
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();
    let comp_flags = CompFlags::default();
    let tmp_basename = "tests/data/cnr-2000-par";

    // load the graph
    let graph = webgraph::graph::bvgraph::load_seq("tests/data/cnr-2000")?;
    for thread_num in 1..10 {
        log::info!("Testing with {} threads", thread_num);
        // create a threadpool and make the compression use it, this way
        // we can test with different number of threads
        let start = std::time::Instant::now();
        // recompress the graph in parallel
        webgraph::graph::bvgraph::parallel_compress_sequential_iter::<
            _,
            WebgraphSequentialIter<
                DynamicCodesReader<
                    BigEndian,
                    BufBitReader<BigEndian, u64, MemWordReaderInf<u32, &[u32]>>,
                >,
            >,
        >(
            tmp_basename,
            &mut graph.iter_nodes(),
            graph.num_nodes(),
            comp_flags,
            thread_num,
        )
        .unwrap();
        log::info!("The compression took: {}s", start.elapsed().as_secs_f64());

        let comp_graph = webgraph::graph::bvgraph::load_seq(tmp_basename)?;
        let mut iter = comp_graph.iter_nodes();

        let mut pr = ProgressLogger::default().display_memory();
        pr.item_name = "node";
        pr.start("Checking that the newly compressed graph is equivalent to the original one...");
        pr.expected_updates = Some(graph.num_nodes());

        while let Some((node, succ_iter)) = graph.iter_nodes().next() {
            let (new_node, new_succ_iter) = iter.next().unwrap();
            assert_eq!(node, new_node);
            let succ = succ_iter.collect::<Vec<_>>();
            let new_succ = new_succ_iter.collect::<Vec<_>>();
            assert_eq!(succ, new_succ, "Node {} differs", node);
            pr.light_update();
        }

        pr.done();
        // cancel the file at the end
        std::fs::remove_file(format!("{}.graph", tmp_basename))?;
        std::fs::remove_file(format!("{}.properties", tmp_basename))?;
        log::info!("\n");
    }

    Ok(())
}
