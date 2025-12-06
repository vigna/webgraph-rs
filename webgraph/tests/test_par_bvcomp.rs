/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::path::PathBuf;

use anyhow::Result;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use log::info;
use webgraph::prelude::*;

#[test]
fn test_par_bvcomp() -> Result<()> {
    env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init()?;

    _test_par_bvcomp("../data/cnr-2000")?;
    Ok(())
}

fn _test_par_bvcomp(basename: &str) -> Result<()> {
    let comp_flags = CompFlags::default();
    let tmp_basename = PathBuf::from(String::from(basename) + "-par");

    // load the graph
    let graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(basename)
        .endianness::<BE>()
        .load()?;

    let mut graph_filename = PathBuf::from(basename);
    graph_filename.set_extension(GRAPH_EXTENSION);

    let expected_size = graph_filename.metadata()?.len();
    for thread_num in 1..10 {
        log::info!("Testing with {} threads", thread_num);
        // create a threadpool and make the compression use it, this way
        // we can test with different number of threads
        let start = std::time::Instant::now();
        // recompress the graph in parallel
        BvComp::parallel_graph::<BE>(
            &tmp_basename,
            &graph,
            comp_flags,
            &rayon::ThreadPoolBuilder::new()
                .num_threads(thread_num)
                .build()
                .expect("Failed to create thread pool"),
            temp_dir(std::env::temp_dir())?,
        )
        .unwrap();
        log::info!("The compression took: {}s", start.elapsed().as_secs_f64());

        let found_size = std::fs::File::open(tmp_basename.with_extension(GRAPH_EXTENSION))?
            .metadata()?
            .len();

        if (found_size as f64) > (expected_size as f64) * 1.2 {
            panic!(
                "The compressed graph is too big: {} > {}",
                found_size, expected_size
            );
        }

        let comp_graph =
            webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&tmp_basename)
                .endianness::<BE>()
                .load()?;

        info!("Checking that the newly compressed graph is equivalent to the original one...");
        graph::eq(&graph, &comp_graph)?;

        let offsets_path = tmp_basename.with_extension(OFFSETS_EXTENSION);
        let mut offsets_reader = buf_bit_reader::from_path::<BE, u32>(&offsets_path)?;
        let mut pr = ProgressLogger::default();
        pr.display_memory(true)
            .item_name("node")
            .expected_updates(Some(graph.num_nodes()));
        pr.start("Checking that the generated offsets are correct...");

        let mut offset = 0;
        for (real_offset, _degree) in comp_graph.offset_deg_iter().by_ref() {
            let gap_offset = offsets_reader.read_gamma().unwrap();
            offset += gap_offset;
            assert_eq!(offset, real_offset);
            pr.light_update();
        }
        pr.done();

        // cancel the file at the end
        std::fs::remove_file(tmp_basename.with_extension(GRAPH_EXTENSION))?;
        std::fs::remove_file(tmp_basename.with_extension(OFFSETS_EXTENSION))?;
        std::fs::remove_file(tmp_basename.with_extension(PROPERTIES_EXTENSION))?;
        log::info!("\n");
    }

    Ok(())
}
