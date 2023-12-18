/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::*;
use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use java_properties;
use mmap_rs::MmapFlags;
use std::fs::*;
use std::io::*;
use std::path::Path;

fn parse_properties(path: &str) -> Result<(usize, usize, CompFlags)> {
    let f = File::open(&path).with_context(|| format!("Cannot open property file {}", path))?;
    let map = java_properties::read(BufReader::new(f))
        .with_context(|| format!("cannot parse {} as a java properties file", path))?;

    let num_nodes = map
        .get("nodes")
        .with_context(|| format!("Missing 'nodes' property in {}", path))?
        .parse::<usize>()
        .with_context(|| format!("Cannot parse 'nodes' as usize in {}", path))?;
    let num_arcs = map
        .get("arcs")
        .with_context(|| format!("Missing 'arcs' property in {}", path))?
        .parse::<usize>()
        .with_context(|| format!("Cannot parse arcs as usize in {}", path))?;
    if let Some(endianness) = map.get("endianness") {
        anyhow::ensure!(
            endianness == "big",
            "Unsupported endianness in {}: {}",
            path,
            endianness
        );
    }

    let comp_flags = CompFlags::from_properties(&map)
        .with_context(|| format!("Cannot parse compression flags from {}", path))?;
    Ok((num_nodes, num_arcs, comp_flags))
}

macro_rules! impl_loads {
    ($builder:ident, $reader:ident, $load_name:ident, $load_seq_name:ident) => {
        /// Load a BVGraph for random access
        pub fn $load_name(
            basename: impl AsRef<Path>,
        ) -> Result<
            BVGraph<$builder<BE, MmapBackend<u32>>, crate::EF<&'static [usize], &'static [u64]>>,
        > {
            let basename = basename.as_ref();
            let (num_nodes, num_arcs, comp_flags) =
                parse_properties(&format!("{}.properties", basename.to_string_lossy()))?;

            let graph = MmapBackend::load(
                format!("{}.graph", basename.to_string_lossy()),
                MmapFlags::TRANSPARENT_HUGE_PAGES,
            )?;

            let ef_path = format!("{}.ef", basename.to_string_lossy());
            let offsets =
                <crate::EF<Vec<usize>, Vec<u64>>>::mmap(&ef_path, Flags::TRANSPARENT_HUGE_PAGES)
                    .with_context(|| format!("Cannot open the elias-fano file {}", ef_path))?;

            let code_reader_builder = <$builder<BE, MmapBackend<u32>>>::new(graph, comp_flags)?;

            Ok(BVGraph::new(
                code_reader_builder,
                offsets,
                comp_flags.min_interval_length,
                comp_flags.compression_window,
                num_nodes,
                num_arcs,
            ))
        }

        /// Load a BVGraph sequentially
        pub fn $load_seq_name<P: AsRef<Path>>(
            basename: P,
        ) -> Result<BVGraphSequential<$builder<BE, MmapBackend<u32>>>> {
            let basename = basename.as_ref();
            let (num_nodes, num_arcs, comp_flags) =
                parse_properties(&format!("{}.properties", basename.to_string_lossy()))?;

            let graph = MmapBackend::load(
                format!("{}.graph", basename.to_string_lossy()),
                MmapFlags::TRANSPARENT_HUGE_PAGES,
            )?;

            let code_reader_builder = <$builder<BE, MmapBackend<u32>>>::new(graph, comp_flags)?;

            let seq_reader = BVGraphSequential::new(
                code_reader_builder,
                comp_flags.compression_window,
                comp_flags.min_interval_length,
                num_nodes,
                Some(num_arcs),
            );

            Ok(seq_reader)
        }
    };
}

impl_loads! {DynamicCodesReaderBuilder, DynamicCodesReader, load, load_seq}
impl_loads! {ConstCodesReaderBuilder, ConstCodesReader, load_const, load_seq_const}
