/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::*;
use crate::graph::bvgraph::EmptyDict;
use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use java_properties;
use mmap_rs::MmapFlags;
use std::fs::*;
use std::io::*;
use std::path::{Path, PathBuf};

/// Read the .properties file and return the endianness
pub fn get_endianess<P: AsRef<Path>>(basename: P) -> Result<String> {
    let path = format!("{}.properties", basename.as_ref().to_string_lossy());
    let f = File::open(&path).with_context(|| format!("Cannot open property file {}", path))?;
    let map = java_properties::read(BufReader::new(f))
        .with_context(|| format!("cannot parse {} as a java properties file", path))?;

    let endianness = map
        .get("endianness")
        .map(|x| x.to_string())
        .unwrap_or_else(|| BigEndian::NAME.to_string());

    Ok(endianness)
}

fn parse_properties<E: Endianness>(path: impl AsRef<Path>) -> Result<(usize, u64, CompFlags)> {
    let name = path.as_ref().to_string_lossy();
    let f = File::open(&path).with_context(|| format!("Cannot open property file {}", name))?;
    let map = java_properties::read(BufReader::new(f))
        .with_context(|| format!("cannot parse {} as a java properties file", name))?;

    let num_nodes = map
        .get("nodes")
        .with_context(|| format!("Missing 'nodes' property in {}", name))?
        .parse::<usize>()
        .with_context(|| format!("Cannot parse 'nodes' as usize in {}", name))?;
    let num_arcs = map
        .get("arcs")
        .with_context(|| format!("Missing 'arcs' property in {}", name))?
        .parse::<u64>()
        .with_context(|| format!("Cannot parse arcs as usize in {}", name))?;

    let endianness = map
        .get("endianness")
        .map(|x| x.to_string())
        .unwrap_or_else(|| BigEndian::NAME.to_string());

    anyhow::ensure!(
        endianness == E::NAME,
        "Wrong endianness in {}, got {} while expected {}",
        name,
        endianness,
        E::NAME
    );

    let comp_flags = CompFlags::from_properties(&map)
        .with_context(|| format!("Cannot parse compression flags from {}", name))?;
    Ok((num_nodes, num_arcs, comp_flags))
}

macro_rules! impl_loads {
    ($builder:ident, $load_name_mem:ident, $load_name:ident, $load_seq_name:ident, $load_seq_name_file:ident) => {
        /// Load a BVGraph for random access
        pub fn $load_name<E: Endianness + 'static>(
            basename: impl AsRef<Path>,
        ) -> anyhow::Result<
            BVGraph<
                $builder<
                    E,
                    MmapBackend<u32>,
                    crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
                >,
                crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
            >,
        >
        where
            for<'a> dsi_bitstream::impls::BufBitReader<
                E,
                dsi_bitstream::impls::MemWordReader<u32, &'a [u32]>,
            >: CodeRead<E> + BitSeek,
        {
            let basename = basename.as_ref();
            let (num_nodes, num_arcs, comp_flags) =
                parse_properties::<E>(&format!("{}.properties", basename.to_string_lossy()))?;

            let graph = MmapBackend::load(
                format!("{}.graph", basename.to_string_lossy()),
                MmapFlags::TRANSPARENT_HUGE_PAGES,
            )?;

            let ef_path = format!("{}.ef", basename.to_string_lossy());
            let offsets = <crate::graph::bvgraph::EF<Vec<usize>, Vec<u64>>>::mmap(
                &ef_path,
                deser::Flags::TRANSPARENT_HUGE_PAGES,
            )
            .with_context(|| format!("Cannot open the elias-fano file {}", ef_path))?;

            let code_reader_builder = <$builder<
                E,
                MmapBackend<u32>,
                crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
            >>::new(graph, offsets, comp_flags)?;

            Ok(BVGraph::new(
                code_reader_builder,
                comp_flags.min_interval_length,
                comp_flags.compression_window,
                num_nodes,
                num_arcs,
            ))
        }

        /// Load a BVGraph for random access
        pub fn $load_name_mem<E: Endianness + 'static>(
            basename: impl AsRef<Path>,
        ) -> anyhow::Result<
            BVGraph<
                $builder<
                    E,
                    MemoryFactory<E, MmapBackend<u32>>,
                    crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
                >,
                crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
            >,
        >
        where
            for<'a> dsi_bitstream::impls::BufBitReader<
                E,
                dsi_bitstream::impls::MemWordReader<u32, &'a [u32]>,
            >: CodeRead<E> + BitSeek,
        {
            let basename = basename.as_ref();
            let (num_nodes, num_arcs, comp_flags) =
                parse_properties::<E>(&format!("{}.properties", basename.to_string_lossy()))?;

            let graph = MemoryFactory::new_mmap(format!("{}.graph", basename.to_string_lossy()), code_reader_builder::Flags::TRANSPARENT_HUGE_PAGES | code_reader_builder::Flags::RANDOM_ACCESS)?;

            let ef_path = format!("{}.ef", basename.to_string_lossy());
            let offsets = <crate::graph::bvgraph::EF<Vec<usize>, Vec<u64>>>::mmap(
                &ef_path,
                deser::Flags::TRANSPARENT_HUGE_PAGES,
            )
            .with_context(|| format!("Cannot open the elias-fano file {}", ef_path))?;

            let code_reader_builder = <$builder<
                E,
                MemoryFactory<E, MmapBackend<u32>>,
                crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
            >>::new(graph, offsets, comp_flags)?;

            Ok(BVGraph::new(
                code_reader_builder,
                comp_flags.min_interval_length,
                comp_flags.compression_window,
                num_nodes,
                num_arcs,
            ))
        }


        /// Load a BVGraph sequentially
        pub fn $load_seq_name<E: Endianness + 'static, P: AsRef<Path>>(
            basename: P,
        ) -> Result<BVGraphSequential<$builder<E, MmapBackend<u32>, EmptyDict<usize, usize>>>>
        where
            for<'a> dsi_bitstream::impls::BufBitReader<
                E,
                dsi_bitstream::impls::MemWordReader<u32, &'a [u32]>,
            >: CodeRead<E> + BitSeek,
        {
            let basename = basename.as_ref();
            let (num_nodes, num_arcs, comp_flags) =
                parse_properties::<E>(&format!("{}.properties", basename.to_string_lossy()))?;

            let graph = MmapBackend::load(
                format!("{}.graph", basename.to_string_lossy()),
                MmapFlags::TRANSPARENT_HUGE_PAGES,
            )?;

            let code_reader_builder =
                <$builder<E, MmapBackend<u32>, EmptyDict<usize, usize>>>::new(
                    graph,
                    MemCase::from(EmptyDict::default()),
                    comp_flags,
                )?;

            let seq_reader = BVGraphSequential::new(
                code_reader_builder,
                comp_flags.compression_window,
                comp_flags.min_interval_length,
                num_nodes,
                Some(num_arcs),
            );

            Ok(seq_reader)
        }

        /// Load a BVGraph sequentially
        pub fn $load_seq_name_file<E: Endianness + 'static, P: AsRef<Path>>(
            basename: P,
        ) -> Result<BVGraphSequential<$builder<E, FileFactory<E>, EmptyDict<usize, usize>>>>
        where
            for<'a> BufBitReader<E, WordAdapter<u32, BufReader<File>>>: CodeRead<E> + BitSeek,
        {
            let mut basename = PathBuf::from(basename.as_ref());
            basename.set_extension("properties");
            let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&basename)?;

            basename.set_extension("graph");
            let graph = FileFactory::new(basename.to_string_lossy().to_string())?;

            let code_reader_builder = <$builder<E, FileFactory<E>, EmptyDict<usize, usize>>>::new(
                graph,
                MemCase::from(EmptyDict::default()),
                comp_flags,
            )?;

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

impl_loads! {DynamicCodesReaderBuilder, load_mem, load, load_seq, load_seq_file}
impl_loads! {ConstCodesReaderBuilder, load_mem_const, load_const, load_seq_const, load_seq_const_file}
