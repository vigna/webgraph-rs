use super::*;
use crate::prelude::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;
use java_properties;
use std::fs::*;
use std::io::*;
use std::path::Path;

pub fn load(
    basename: &str,
) -> Result<BVGraph<DynamicCodesReaderBuilder<BE, MmapBackend<u32>>, crate::EF<&[u64]>>> {
    let f = File::open(format!("{}.properties", basename))?;
    let map = java_properties::read(BufReader::new(f))?;

    let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;
    let num_arcs = map.get("arcs").unwrap().parse::<u64>()?;
    let min_interval_length = map.get("minintervallength").unwrap().parse::<usize>()?;
    let compression_window = map.get("windowsize").unwrap().parse::<usize>()?;

    assert_eq!(map.get("compressionflags").unwrap(), "");

    let graph_path_str = format!("{}.graph", basename);
    let graph_path = Path::new(&graph_path_str);
    let file_len = graph_path.metadata()?.len();
    let file = std::fs::File::open(graph_path)?;

    let graph = MmapBackend::new(unsafe {
        mmap_rs::MmapOptions::new(file_len as _)?
            .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
            .with_file(file, 0)
            .map()?
    });

    let offsets = sux::prelude::map::<_, crate::EF<&[u64]>>(
        format!("{}.ef", basename),
        &sux::prelude::Flags::TRANSPARENT_HUGE_PAGES,
    )?;

    let code_reader_builder =
        DynamicCodesReaderBuilder::new(graph, &CompFlags::from_properties(&map)?)?;

    Ok(BVGraph::new(
        code_reader_builder,
        offsets,
        min_interval_length,
        compression_window,
        num_nodes as usize,
        num_arcs as usize,
    ))
}
