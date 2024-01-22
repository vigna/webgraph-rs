/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::codecs::Flags;
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

pub trait Access: 'static {}

pub struct Sequential {}
impl Access for Sequential {}

pub struct Random {}
impl Access for Random {}

pub trait Dispatch: 'static {}

pub struct Static<
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
    const K: u64 = 3,
> {}
impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > Dispatch for Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
}

pub struct Dynamic {}
impl Dispatch for Dynamic {}

pub trait Mode: 'static {
    type Factory<E: Endianness>: CodeReaderFactory<E>;

    fn new_factory<E: Endianness>(
        graph: &PathBuf,
        flags: codecs::Flags,
    ) -> Result<Self::Factory<E>>;

    type Offsets: IndexedDict<Input = usize, Output = usize>;

    fn load_offsets(offsets: &PathBuf, flags: Flags) -> Result<MemCase<Self::Offsets>>;
}

pub struct File {}
impl Mode for File {
    type Factory<E: Endianness> = FileFactory<E>;
    type Offsets = EF<Vec<usize>, Vec<u64>>;

    fn new_factory<E: Endianness>(graph: &PathBuf, _flags: Flags) -> Result<Self::Factory<E>> {
        Ok(FileFactory::<E>::new(graph)?)
    }

    fn load_offsets(offsets: &PathBuf, _flags: Flags) -> Result<MemCase<Self::Offsets>> {
        Ok(EF::<Vec<usize>, Vec<u64>>::load_full(offsets)?.into())
    }
}

pub struct Mmap {}
impl Mode for Mmap {
    type Factory<E: Endianness> = MmapBackend<u32>;
    type Offsets = <EF<Vec<usize>, Vec<u64>> as DeserializeInner>::DeserType<'static>;

    fn new_factory<E: Endianness>(graph: &PathBuf, flags: Flags) -> Result<Self::Factory<E>> {
        Ok(MmapBackend::load(graph, flags.into())?)
    }

    fn load_offsets(offsets: &PathBuf, flags: Flags) -> Result<MemCase<Self::Offsets>> {
        EF::<Vec<usize>, Vec<u64>>::mmap(offsets, flags.into())
    }
}

pub struct LoadMem {}
impl Mode for LoadMem {
    type Factory<E: Endianness> = MemoryFactory<E, Box<[u32]>>;
    type Offsets = <EF<Vec<usize>, Vec<u64>> as DeserializeInner>::DeserType<'static>;

    fn new_factory<E: Endianness>(graph: &PathBuf, _flags: Flags) -> Result<Self::Factory<E>> {
        Ok(MemoryFactory::<E, _>::new_mem(graph)?)
    }

    fn load_offsets(offsets: &PathBuf, _flags: Flags) -> Result<MemCase<Self::Offsets>> {
        Ok(EF::<Vec<usize>, Vec<u64>>::load_mem(offsets)?)
    }
}

pub struct LoadMmap {}
impl Mode for LoadMmap {
    type Factory<E: Endianness> = MemoryFactory<E, MmapBackend<u32>>;
    type Offsets = <EF<Vec<usize>, Vec<u64>> as DeserializeInner>::DeserType<'static>;

    fn new_factory<E: Endianness>(graph: &PathBuf, flags: Flags) -> Result<Self::Factory<E>> {
        Ok(MemoryFactory::<E, _>::new_mmap(graph, flags)?)
    }

    fn load_offsets(offsets: &PathBuf, flags: Flags) -> Result<MemCase<Self::Offsets>> {
        EF::<Vec<usize>, Vec<u64>>::load_mmap(offsets, flags.into())
    }
}

pub struct Load<E: Endianness, A: Access, D: Dispatch, GLM: Mode, OLM: Mode> {
    pub(crate) basename: PathBuf,
    pub(crate) graph_load_flags: Flags,
    pub(crate) offsets_load_flags: Flags,
    pub(crate) _marker: std::marker::PhantomData<(E, A, D, GLM, OLM)>,
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: Mode, OLM: Mode> Load<E, A, D, GLM, OLM> {
    pub fn endianness<E2: Endianness>(self) -> Load<E2, A, D, GLM, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: Mode, OLM: Mode> Load<E, A, D, GLM, OLM> {
    pub fn dispatch<D2: Dispatch>(self) -> Load<E, A, D2, GLM, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: Mode, OLM: Mode> Load<E, A, D, GLM, OLM> {
    pub fn random_access(self) -> Load<E, Random, D, GLM, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: Mode, OLM: Mode> Load<E, A, D, GLM, OLM> {
    pub fn mode<LM: Mode>(self) -> Load<E, A, D, LM, LM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch> Load<E, A, D, Mmap, Mmap> {
    pub fn flags(self, flags: Flags) -> Load<E, A, D, Mmap, Mmap> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch> Load<E, A, D, LoadMmap, LoadMmap> {
    pub fn flags(self, flags: Flags) -> Load<E, A, D, LoadMmap, LoadMmap> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: Mode, OLM: Mode> Load<E, A, D, GLM, OLM> {
    pub fn graph_mode<NGLM: Mode>(self) -> Load<E, A, D, NGLM, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, OLM: Mode> Load<E, A, D, Mmap, OLM> {
    pub fn graph_load_flags(self, flags: Flags) -> Load<E, A, D, Mmap, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, OLM: Mode> Load<E, A, D, LoadMmap, OLM> {
    pub fn graph_load_flags(self, flags: Flags) -> Load<E, A, D, LoadMmap, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: Mode, OLM: Mode> Load<E, Random, D, GLM, OLM> {
    pub fn offsets_mode<NOLM: Mode>(self) -> Load<E, Random, D, GLM, NOLM> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: Mode> Load<E, Random, D, GLM, Mmap> {
    pub fn offsets_load_flags(self, flags: Flags) -> Load<E, Random, D, GLM, Mmap> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: Mode> Load<E, Random, D, GLM, LoadMmap> {
    pub fn offsets_load_flags(self, flags: Flags) -> Load<E, Random, D, GLM, LoadMmap> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, GLM: Mode, OLM: Mode> Load<E, Random, Dynamic, GLM, OLM> {
    pub fn load(
        mut self,
    ) -> anyhow::Result<BVGraph<DynamicCodesReaderBuilder<E, GLM::Factory<E>, OLM::Offsets>>>
    where
        for<'a> <<GLM as Mode>::Factory<E> as CodeReaderFactory<E>>::CodeReader<'a>:
            CodeRead<E> + BitSeek,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;
        self.basename.set_extension("ef");
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)?;

        Ok(BVGraph::new(
            DynamicCodesReaderBuilder::new(factory, offsets, comp_flags)?,
            comp_flags.min_interval_length,
            comp_flags.compression_window,
            num_nodes,
            num_arcs,
        ))
    }
}

impl<E: Endianness, GLM: Mode, OLM: Mode> Load<E, Sequential, Dynamic, GLM, OLM> {
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BVGraphSequential<DynamicCodesReaderBuilder<E, GLM::Factory<E>, EmptyDict<usize, usize>>>,
    >
    where
        for<'a> <<GLM as Mode>::Factory<E> as CodeReaderFactory<E>>::CodeReader<'a>:
            CodeRead<E> + BitSeek,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BVGraphSequential::new(
            DynamicCodesReaderBuilder::new(
                factory,
                MemCase::from(EmptyDict::default()),
                comp_flags,
            )?,
            comp_flags.compression_window,
            comp_flags.min_interval_length,
            num_nodes,
            Some(num_arcs),
        ))
    }
}

impl<
        E: Endianness,
        GLM: Mode,
        OLM: Mode,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > Load<E, Random, Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>, GLM, OLM>
{
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BVGraph<
            ConstCodesDecoderFactory<
                E,
                GLM::Factory<E>,
                OLM::Offsets,
                OUTDEGREES,
                REFERENCES,
                BLOCKS,
                INTERVALS,
                RESIDUALS,
                K,
            >,
        >,
    >
    where
        for<'a> <<GLM as Mode>::Factory<E> as CodeReaderFactory<E>>::CodeReader<'a>:
            CodeRead<E> + BitSeek,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;
        self.basename.set_extension("ef");
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)?;

        Ok(BVGraph::new(
            ConstCodesDecoderFactory::new(factory, offsets, comp_flags)?,
            comp_flags.min_interval_length,
            comp_flags.compression_window,
            num_nodes,
            num_arcs,
        ))
    }
}

impl<
        E: Endianness,
        GLM: Mode,
        OLM: Mode,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    >
    Load<E, Sequential, Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>, GLM, OLM>
{
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BVGraphSequential<
            ConstCodesDecoderFactory<
                E,
                GLM::Factory<E>,
                EmptyDict<usize, usize>,
                OUTDEGREES,
                REFERENCES,
                BLOCKS,
                INTERVALS,
                RESIDUALS,
                K,
            >,
        >,
    >
    where
        for<'a> <<GLM as Mode>::Factory<E> as CodeReaderFactory<E>>::CodeReader<'a>:
            CodeRead<E> + BitSeek,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BVGraphSequential::new(
            ConstCodesDecoderFactory::new(
                factory,
                MemCase::from(EmptyDict::default()),
                comp_flags,
            )?,
            comp_flags.compression_window,
            comp_flags.min_interval_length,
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

            let factories = <$builder<
                E,
                MmapBackend<u32>,
                crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
            >>::new(graph, offsets, comp_flags)?;

            Ok(BVGraph::new(
                factories,
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
                Flags::TRANSPARENT_HUGE_PAGES | Flags::RANDOM_ACCESS,
            )?;

            let ef_path = format!("{}.ef", basename.to_string_lossy());
            let offsets = <crate::graph::bvgraph::EF<Vec<usize>, Vec<u64>>>::mmap(
                &ef_path,
                deser::Flags::TRANSPARENT_HUGE_PAGES,
            )
            .with_context(|| format!("Cannot open the elias-fano file {}", ef_path))?;

            let factories = <$builder<
                E,
                MemoryFactory<E, MmapBackend<u32>>,
                crate::graph::bvgraph::EF<&'static [usize], &'static [u64]>,
            >>::new(graph, offsets, comp_flags)?;

            Ok(BVGraph::new(
                factories,
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

            let factories = <$builder<E, MmapBackend<u32>, EmptyDict<usize, usize>>>::new(
                graph,
                MemCase::from(EmptyDict::default()),
                comp_flags,
            )?;

            let seq_reader = BVGraphSequential::new(
                factories,
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
impl_loads! {ConstCodesDecoderFactory, load_mem_const, load_const, load_seq_const, load_seq_const_file}
