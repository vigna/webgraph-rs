/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::codecs::MemoryFlags;
use super::*;
use crate::graphs::bvgraph::EmptyDict;
use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use java_properties;
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
    type Factory<E: Endianness>: BitReaderFactory<E>;

    fn new_factory<E: Endianness>(
        graph: &PathBuf,
        flags: codecs::MemoryFlags,
    ) -> Result<Self::Factory<E>>;

    type Offsets: IndexedDict<Input = usize, Output = usize>;

    fn load_offsets(offsets: &PathBuf, flags: MemoryFlags) -> Result<MemCase<Self::Offsets>>;
}

pub struct File {}
impl Mode for File {
    type Factory<E: Endianness> = FileFactory<E>;
    type Offsets = EF;

    fn new_factory<E: Endianness>(
        graph: &PathBuf,
        _flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        Ok(FileFactory::<E>::new(graph)?)
    }

    fn load_offsets(offsets: &PathBuf, _flags: MemoryFlags) -> Result<MemCase<Self::Offsets>> {
        Ok(EF::load_full(offsets)?.into())
    }
}

pub struct Mmap {}
impl Mode for Mmap {
    type Factory<E: Endianness> = MmapBackend<u32>;
    type Offsets = <EF as DeserializeInner>::DeserType<'static>;

    fn new_factory<E: Endianness>(graph: &PathBuf, flags: MemoryFlags) -> Result<Self::Factory<E>> {
        Ok(MmapBackend::load(graph, flags.into())?)
    }

    fn load_offsets(offsets: &PathBuf, flags: MemoryFlags) -> Result<MemCase<Self::Offsets>> {
        EF::mmap(offsets, flags.into())
    }
}

pub struct LoadMem {}
impl Mode for LoadMem {
    type Factory<E: Endianness> = MemoryFactory<E, Box<[u32]>>;
    type Offsets = <EF as DeserializeInner>::DeserType<'static>;

    fn new_factory<E: Endianness>(
        graph: &PathBuf,
        _flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        Ok(MemoryFactory::<E, _>::new_mem(graph)?)
    }

    fn load_offsets(offsets: &PathBuf, _flags: MemoryFlags) -> Result<MemCase<Self::Offsets>> {
        Ok(EF::load_mem(offsets)?)
    }
}

pub struct LoadMmap {}
impl Mode for LoadMmap {
    type Factory<E: Endianness> = MemoryFactory<E, MmapBackend<u32>>;
    type Offsets = <EF as DeserializeInner>::DeserType<'static>;

    fn new_factory<E: Endianness>(graph: &PathBuf, flags: MemoryFlags) -> Result<Self::Factory<E>> {
        Ok(MemoryFactory::<E, _>::new_mmap(graph, flags)?)
    }

    fn load_offsets(offsets: &PathBuf, flags: MemoryFlags) -> Result<MemCase<Self::Offsets>> {
        EF::load_mmap(offsets, flags.into())
    }
}

pub struct Load<E: Endianness, A: Access, D: Dispatch, GLM: Mode, OLM: Mode> {
    pub(crate) basename: PathBuf,
    pub(crate) graph_load_flags: MemoryFlags,
    pub(crate) offsets_load_flags: MemoryFlags,
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
    pub fn flags(self, flags: MemoryFlags) -> Load<E, A, D, Mmap, Mmap> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch> Load<E, A, D, LoadMmap, LoadMmap> {
    pub fn flags(self, flags: MemoryFlags) -> Load<E, A, D, LoadMmap, LoadMmap> {
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
    pub fn graph_load_flags(self, flags: MemoryFlags) -> Load<E, A, D, Mmap, OLM> {
        Load {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, OLM: Mode> Load<E, A, D, LoadMmap, OLM> {
    pub fn graph_load_flags(self, flags: MemoryFlags) -> Load<E, A, D, LoadMmap, OLM> {
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
    pub fn offsets_load_flags(self, flags: MemoryFlags) -> Load<E, Random, D, GLM, Mmap> {
        Load {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: Mode> Load<E, Random, D, GLM, LoadMmap> {
    pub fn offsets_load_flags(self, flags: MemoryFlags) -> Load<E, Random, D, GLM, LoadMmap> {
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
    ) -> anyhow::Result<BVGraph<DynCodesDecoderFactory<E, GLM::Factory<E>, OLM::Offsets>>>
    where
        for<'a> <<GLM as Mode>::Factory<E> as BitReaderFactory<E>>::BitReader<'a>:
            CodeRead<E> + BitSeek,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;
        self.basename.set_extension("ef");
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)?;

        Ok(BVGraph::new(
            DynCodesDecoderFactory::new(factory, offsets, comp_flags)?,
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
        BVGraphSeq<DynCodesDecoderFactory<E, GLM::Factory<E>, EmptyDict<usize, usize>>>,
    >
    where
        for<'a> <<GLM as Mode>::Factory<E> as BitReaderFactory<E>>::BitReader<'a>: CodeRead<E>,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BVGraphSeq::new(
            DynCodesDecoderFactory::new(factory, MemCase::from(EmptyDict::default()), comp_flags)?,
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
        for<'a> <<GLM as Mode>::Factory<E> as BitReaderFactory<E>>::BitReader<'a>:
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
        BVGraphSeq<
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
        for<'a> <<GLM as Mode>::Factory<E> as BitReaderFactory<E>>::BitReader<'a>: CodeRead<E>,
    {
        self.basename.set_extension("properties");
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension("graph");
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BVGraphSeq::new(
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
