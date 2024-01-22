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
use std::io::*;
use std::path::{Path, PathBuf};
use sux::traits::IndexedDict;

pub trait Dispatch: 'static + Clone + Copy {}

#[derive(Debug, Clone, Copy)]
pub struct Static {}
impl Dispatch for Static {}
#[derive(Debug, Clone, Copy)]
pub struct Dynamic {}
impl Dispatch for Dynamic {}

pub trait LoadMode: 'static + Clone + Copy {
    type Factory<E: Endianness>: CodeReaderFactory<E>;

    fn new_factory<E: Endianness>(
        filename: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<Self::Factory<E>>;

    type Offsets: IndexedDict<Input = usize, Output = usize>;

    fn load_offsets(
        offsets: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<MemCase<Self::Offsets>>;
}
#[derive(Debug, Clone, Copy)]
pub struct File {}
impl LoadMode for File {
    type Factory<E: Endianness> = FileFactory<E>;

    fn new_factory<E: Endianness>(
        basename: &PathBuf,
        _flags: code_reader_builder::Flags,
    ) -> Result<Self::Factory<E>> {
        Ok(FileFactory::<E>::new(basename)?)
    }

    type Offsets = EF<Vec<usize>, Vec<u64>>;
    fn load_offsets(
        offsets: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<MemCase<Self::Offsets>> {
        Ok(MemCase::encase(EF::<Vec<usize>, Vec<u64>>::load_full(
            offsets,
        )?))
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Mmap {
    flags: code_reader_builder::Flags,
}
impl LoadMode for Mmap {
    type Factory<E: Endianness> = MmapBackend<u32>;

    fn new_factory<E: Endianness>(
        basename: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<Self::Factory<E>> {
        Ok(MmapBackend::<u32>::load(basename, flags.into())?)
    }

    type Offsets = <EF<Vec<usize>, Vec<u64>> as DeserializeInner>::DeserType<'static>;
    fn load_offsets(
        offsets: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<MemCase<Self::Offsets>> {
        EF::<Vec<usize>, Vec<u64>>::mmap(offsets, flags.into())
    }
}
#[derive(Debug, Clone, Copy)]
pub struct LoadMem {}
impl LoadMode for LoadMem {
    type Factory<E: Endianness> = MemoryFactory<E, Box<[u32]>>;

    fn new_factory<E: Endianness>(
        filename: &PathBuf,
        _flags: code_reader_builder::Flags,
    ) -> Result<Self::Factory<E>> {
        Ok(MemoryFactory::<E, _>::new_mem(filename)?)
    }

    type Offsets = EF<Vec<usize>, Vec<u64>>;
    fn load_offsets(
        offsets: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<MemCase<Self::Offsets>> {
        Ok(MemCase::encase(EF::<Vec<usize>, Vec<u64>>::load_full(
            offsets,
        )?))
    }
}
#[derive(Debug, Clone, Copy)]
pub struct LoadMmap {}
impl LoadMode for LoadMmap {
    type Factory<E: Endianness> = MemoryFactory<E, MmapBackend<u32>>;

    fn new_factory<E: Endianness>(
        filename: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<Self::Factory<E>> {
        Ok(MemoryFactory::<E, _>::new_mmap(filename, flags)?)
    }

    type Offsets = <EF<Vec<usize>, Vec<u64>> as DeserializeInner>::DeserType<'static>;
    fn load_offsets(
        offsets: &PathBuf,
        flags: code_reader_builder::Flags,
    ) -> Result<MemCase<Self::Offsets>> {
        EF::<Vec<usize>, Vec<u64>>::load_mmap(offsets, flags.into())
    }
}

#[derive(Debug)]
pub struct Load<E: Endianness, M: Dispatch, GLM: LoadMode, OLM: LoadMode> {
    basename: PathBuf,
    graph_load_flags: code_reader_builder::Flags,
    offsets_load_flags: code_reader_builder::Flags,
    _marker: std::marker::PhantomData<(E, M, GLM, OLM)>,
}

impl Load<NE, Dynamic, Mmap, Mmap> {
    pub fn with_basename(basename: impl AsRef<Path>) -> Self {
        Self {
            basename: PathBuf::from(basename.as_ref()),
            graph_load_flags: code_reader_builder::Flags::default(),
            offsets_load_flags: code_reader_builder::Flags::default(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, M: Dispatch, GLM: LoadMode, OLM: LoadMode> Load<E, M, GLM, OLM> {
    pub fn dispatch<D: Dispatch>(self) -> Load<E, D, GLM, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode, OLM: LoadMode> Load<E, D, GLM, OLM> {
    pub fn load_mode<LM: LoadMode>(self) -> Load<E, D, LM, LM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch> Load<E, D, Mmap, Mmap> {
    pub fn flags(self, flags: code_reader_builder::Flags) -> Load<E, D, Mmap, Mmap> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch> Load<E, D, LoadMmap, LoadMmap> {
    pub fn flags(self, flags: code_reader_builder::Flags) -> Load<E, D, LoadMmap, LoadMmap> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode, OLM: LoadMode> Load<E, D, GLM, OLM> {
    pub fn graph_load_mode<NGLM: LoadMode>(self, graph_load_mode: NGLM) -> Load<E, D, NGLM, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, OLM: LoadMode> Load<E, D, Mmap, OLM> {
    pub fn graph_load_flags(self, flags: code_reader_builder::Flags) -> Load<E, D, Mmap, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, OLM: LoadMode> Load<E, D, LoadMmap, OLM> {
    pub fn graph_load_flags(self, flags: code_reader_builder::Flags) -> Load<E, D, LoadMmap, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode, OLM: LoadMode> Load<E, D, GLM, OLM> {
    pub fn offsets_load_mode<NOLM: LoadMode>(
        self,
        offsets_load_mode: NOLM,
    ) -> Load<E, D, GLM, NOLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode> Load<E, D, GLM, Mmap> {
    pub fn offsets_load_flags(self, flags: code_reader_builder::Flags) -> Load<E, D, GLM, Mmap> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode> Load<E, D, GLM, LoadMmap> {
    pub fn offsets_load_flags(
        self,
        flags: code_reader_builder::Flags,
    ) -> Load<E, D, GLM, LoadMmap> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, GLM: LoadMode, OLM: LoadMode> Load<E, Dynamic, GLM, OLM> {
    pub fn random(
        mut self,
    ) -> anyhow::Result<BVGraph<DynamicCodesReaderBuilder<E, GLM::Factory<E>, OLM::Offsets>>>
    where
        for<'a> <<GLM as LoadMode>::Factory<E> as CodeReaderFactory<E>>::CodeReader<'a>:
            CodeRead<E> + BitSeek,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;
        self.basename.set_extension("ef");
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)?;

        let code_reader_builder =
            <DynamicCodesReaderBuilder<E, GLM::Factory<E>, OLM::Offsets>>::new(
                factory, offsets, comp_flags,
            )?;

        Ok(BVGraph::new(
            code_reader_builder,
            comp_flags.min_interval_length,
            comp_flags.compression_window,
            num_nodes,
            num_arcs,
        ))
    }

    pub fn sequential(
        mut self,
    ) -> anyhow::Result<
        BVGraphSequential<DynamicCodesReaderBuilder<E, GLM::Factory<E>, EmptyDict<usize, usize>>>,
    >
    where
        for<'a> <<GLM as LoadMode>::Factory<E> as CodeReaderFactory<E>>::CodeReader<'a>:
            CodeRead<E> + BitSeek,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        let code_reader_builder =
            <DynamicCodesReaderBuilder<E, GLM::Factory<E>, EmptyDict<usize, usize>>>::new(
                factory,
                MemCase::encase(EmptyDict::default()),
                comp_flags,
            )?;

        Ok(BVGraphSequential::new(
            code_reader_builder,
            comp_flags.min_interval_length,
            comp_flags.compression_window,
            num_nodes,
            Some(num_arcs),
        ))
    }
}

/// Read the .properties file and return the endianness
pub fn get_endianess<P: AsRef<Path>>(basename: P) -> Result<String> {
    let path = format!("{}.properties", basename.as_ref().to_string_lossy());
    let f = std::fs::File::open(&path)
        .with_context(|| format!("Cannot open property file {}", path))?;
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
    let f = std::fs::File::open(&path)
        .with_context(|| format!("Cannot open property file {}", name))?;
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
                epserde::deser::Flags::TRANSPARENT_HUGE_PAGES,
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

            let graph = MemoryFactory::new_mmap(
                format!("{}.graph", basename.to_string_lossy()),
                code_reader_builder::Flags::TRANSPARENT_HUGE_PAGES
                    | code_reader_builder::Flags::RANDOM_ACCESS,
            )?;

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
    };
}

impl_loads! {DynamicCodesReaderBuilder, load_mem, load, load_seq, load_seq_file}
impl_loads! {ConstCodesReaderBuilder, load_mem_const, load_const, load_seq_const, load_seq_const_file}
