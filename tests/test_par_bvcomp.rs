use anyhow::Result;
use dsi_bitstream::prelude::*;
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
    // recompress the graph in parallel
    webgraph::bvgraph::parallel_compress_sequential_iter(
        tmp_path,
        graph.iter_nodes(),
        comp_flags.clone(),
    )
    .unwrap();

    // manually load a sequential iter on the parallelly compressed graph
    let file_len = PathBuf::from(tmp_path).metadata()?.len();
    let tmp_file = File::open(tmp_path)?;
    let data = unsafe {
        MmapOptions::new(file_len as _)?
            .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
            .with_file(tmp_file, 0)
            .map()?
    };
    let code_reader = DynamicCodesReader::new(
        BufferedBitStreamRead::<BE, u64, _>::new(MemWordReadInfinite::<u32, _>::new(
            MmapBackend::new(data),
        )),
        &comp_flags,
    )?;
    let mut iter = WebgraphSequentialIter::new(
        code_reader,
        comp_flags.compression_window,
        comp_flags.min_interval_length,
        graph.num_nodes(),
    );

    // check that it's the same as the original graph
    for (node, succ_iter) in graph.iter_nodes() {
        let (new_node, new_succ_iter) = iter.next().unwrap();
        assert_eq!(node, new_node);
        let succ = succ_iter.collect::<Vec<_>>();
        let new_succ = new_succ_iter.collect::<Vec<_>>();
        assert_eq!(succ, new_succ);
    }

    Ok(())
}
