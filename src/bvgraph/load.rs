use super::*;
use crate::prelude::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;
use java_properties;
use std::fs::*;
use std::io::*;
use std::path::Path;

macro_rules! impl_loads {
    ($builder:ident, $reader:ident, $load_name:ident, $load_seq_name:ident) => {
        /// Load a BVGraph for random access
        pub fn $load_name(
            basename: &str,
        ) -> Result<BVGraph<$builder<BE, MmapBackend<u32>>, crate::EF<&[u64]>>> {
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
                <$builder<BE, MmapBackend<u32>>>::new(graph, &CompFlags::from_properties(&map)?)?;

            Ok(BVGraph::new(
                code_reader_builder,
                offsets,
                min_interval_length,
                compression_window,
                num_nodes as usize,
                num_arcs as usize,
            ))
        }

        /// Load a BVGraph sequentially
        pub fn $load_seq_name(
            basename: &str,
        ) -> Result<
            WebgraphSequentialIter<
                $reader<
                    BE,
                    BufferedBitStreamRead<BE, u64, MemWordReadInfinite<u32, MmapBackend<u32>>>,
                >,
            >,
        > {
            let f = File::open(format!("{}.properties", basename))?;
            let map = java_properties::read(BufReader::new(f))?;

            let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;
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

            let code_reader = <$reader<
                BE,
                BufferedBitStreamRead<BE, u64, MemWordReadInfinite<u32, MmapBackend<u32>>>,
            >>::new(
                BufferedBitStreamRead::new(MemWordReadInfinite::new(graph)),
                &CompFlags::from_properties(&map)?,
            )?;

            let seq_reader = WebgraphSequentialIter::new(
                code_reader,
                compression_window,
                min_interval_length,
                num_nodes as usize,
            );

            Ok(seq_reader)
        }
    };
}

impl_loads! {DynamicCodesReaderBuilder, DynamicCodesReader, load, load_seq}
impl_loads! {ConstCodesReaderBuilder, ConstCodesReader, load_const, load_seq_const}
