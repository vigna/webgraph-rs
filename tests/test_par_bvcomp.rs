use anyhow::Result;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use mmap_rs::MmapOptions;
use std::fs::File;
use std::path::PathBuf;
use webgraph::prelude::*;

#[test]
fn test_par_bvcomp() -> Result<()> {
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();
    let comp_flags = CompFlags::default();
    let tmp_path = "tests/data/cnr-2000-par.graph";

    // load the graph
    let graph = webgraph::bvgraph::load_seq("tests/data/cnr-2000")?;
    for thread_num in 1..10 {
        log::info!("Testing with {} threads", thread_num);
        // create a threadpool and make the compression use it, this way
        // we can test with different number of threads
        let start = std::time::Instant::now();
        rayon::ThreadPoolBuilder::new()
            .num_threads(thread_num)
            .build()
            .unwrap()
            .install(|| {
                // recompress the graph in parallel
                webgraph::bvgraph::parallel_compress_sequential_iter(
                    tmp_path,
                    graph.iter_nodes(),
                    comp_flags.clone(),
                )
                .unwrap();
            });
        log::info!("The compression took: {}s", start.elapsed().as_secs_f64());

        // manually load a sequential iter on the parallelly compressed graph
        let file_len = PathBuf::from(tmp_path).metadata()?.len();
        let tmp_file = File::open(tmp_path)?;
        let data = unsafe {
            MmapOptions::new(file_len as _)?
                .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                .with_file(tmp_file, 0)
                .map()?
        };

        let bitstream = BufferedBitStreamRead::<BE, u64, _>::new(
            MemWordReadInfinite::<u32, _>::new(MmapBackend::new(data)),
        );
        let code_reader = DynamicCodesReader::new(bitstream, &comp_flags)?;

        let mut iter = WebgraphSequentialIter::new(
            code_reader,
            comp_flags.compression_window,
            comp_flags.min_interval_length,
            graph.num_nodes(),
        );

        // check that it's the same as the original graph = seq_graph.map_codes_reader_builder(|cbr| CodesReaderStatsBuilder::new(cbr));

        let mut pr = ProgressLogger::default().display_memory();
        pr.item_name = "node";
        pr.start("Checking that the newly compressed graph is equivalent to the original one...");
        pr.expected_updates = Some(graph.num_nodes());

        for (node, succ_iter) in graph.iter_nodes() {
            let (new_node, new_succ_iter) = iter.next().unwrap();
            assert_eq!(node, new_node);
            let succ = succ_iter.collect::<Vec<_>>();
            let new_succ = new_succ_iter.collect::<Vec<_>>();
            assert_eq!(succ, new_succ, "Node {} differs", node);
            pr.light_update();
        }

        pr.done();
        // cancel the file at the end
        std::fs::remove_file(tmp_path)?;
        log::info!("\n");
    }

    Ok(())
}
