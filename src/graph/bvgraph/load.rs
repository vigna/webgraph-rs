use super::*;
use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use java_properties;
use std::fs::*;
use std::io::*;
use std::path::Path;

macro_rules! impl_loads {
    ($builder:ident, $reader:ident, $load_name:ident, $load_seq_name:ident) => {
        /// Load a BVGraph for random access
        pub fn $load_name<P: AsRef<std::path::Path>>(
            basename: P,
        ) -> Result<BVGraph<$builder<BE, MmapBackend<u32>>, crate::EF<&'static [u64]>>> {
            let basename = basename.as_ref();
            let properties_path = format!("{}.properties", basename.to_string_lossy());
            let f = File::open(&properties_path)
                .with_context(|| format!("Cannot open property file {}", properties_path))?;
            let map = java_properties::read(BufReader::new(f))
                .with_context(|| "cannot parse the .properties file as a java properties file")?;

            let num_nodes = map
                .get("nodes")
                .with_context(|| "Missing nodes property")?
                .parse::<u64>()
                .with_context(|| "Cannot parse nodes as u64")?;
            let num_arcs = map
                .get("arcs")
                .with_context(|| "Missing arcs property")?
                .parse::<u64>()
                .with_context(|| "Cannot parse arcs as u64")?;

            let graph_path_str = format!("{}.graph", basename.to_string_lossy());
            let graph_path = Path::new(&graph_path_str);
            let file_len = graph_path.metadata()?.len();
            let file = std::fs::File::open(graph_path).with_context(|| "Cannot open graph file")?;

            let graph = MmapBackend::new(unsafe {
                mmap_rs::MmapOptions::new(file_len as _)?
                    .with_flags((Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                    .with_file(file, 0)
                    .map()?
            });

            let ef_path = format!("{}.ef", basename.to_string_lossy());
            let offsets = <crate::EF<Vec<u64>>>::mmap(&ef_path, Flags::TRANSPARENT_HUGE_PAGES)
                .with_context(|| format!("Cannot open the elias-fano file {}", ef_path))?;

            let comp_flags = CompFlags::from_properties(&map)?;
            let code_reader_builder = <$builder<BE, MmapBackend<u32>>>::new(graph, comp_flags)?;

            Ok(BVGraph::new(
                code_reader_builder,
                offsets,
                comp_flags.min_interval_length,
                comp_flags.compression_window,
                num_nodes as usize,
                num_arcs as usize,
            ))
        }

        /// Load a BVGraph sequentially
        pub fn $load_seq_name<P: AsRef<std::path::Path>>(
            basename: P,
        ) -> Result<BVGraphSequential<$builder<BE, MmapBackend<u32>>>> {
            let basename = basename.as_ref();
            let properties_path = format!("{}.properties", basename.to_string_lossy());
            let f = File::open(&properties_path)
                .with_context(|| format!("Cannot open property file {}", properties_path))?;
            let map = java_properties::read(BufReader::new(f))
                .with_context(|| "cannot parse the .properties file as a java properties file")?;

            let num_nodes = map
                .get("nodes")
                .with_context(|| "Missing nodes property")?
                .parse::<u64>()
                .with_context(|| "Cannot parse nodes as u64")?;
            let num_arcs = map
                .get("arcs")
                .with_context(|| "Missing arcs property")?
                .parse::<u64>()
                .with_context(|| "Cannot parse arcs as u64")?;

            let graph_path_str = format!("{}.graph", basename.to_string_lossy());
            let graph_path = Path::new(&graph_path_str);
            let file_len = graph_path.metadata()?.len();
            let file = std::fs::File::open(graph_path)?;

            let graph = MmapBackend::new(unsafe {
                mmap_rs::MmapOptions::new(file_len as _)?
                    .with_flags((Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                    .with_file(file, 0)
                    .map()?
            });

            let comp_flags = CompFlags::from_properties(&map)?;
            let code_reader_builder = <$builder<BE, MmapBackend<u32>>>::new(graph, comp_flags)?;

            let seq_reader = BVGraphSequential::new(
                code_reader_builder,
                comp_flags.compression_window,
                comp_flags.min_interval_length,
                num_nodes as usize,
                Some(num_arcs as _),
            );

            Ok(seq_reader)
        }
    };
}

impl_loads! {DynamicCodesReaderBuilder, DynamicCodesReader, load, load_seq}
impl_loads! {ConstCodesReaderBuilder, ConstCodesReader, load_const, load_seq_const}
