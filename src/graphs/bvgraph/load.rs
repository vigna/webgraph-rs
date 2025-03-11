/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::*;
use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::codes::dispatch::code_consts;
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use sealed::sealed;
use std::{
    convert::Infallible,
    io::{self, BufReader},
    path::{Path, PathBuf},
};
use sux::traits::IndexedSeq;

/// Sequential or random access.
#[doc(hidden)]
#[sealed]
pub trait Access: 'static {}

#[derive(Debug, Clone)]
pub struct Sequential {}
#[sealed]
impl Access for Sequential {}

#[derive(Debug, Clone)]
pub struct Random {}
#[sealed]
impl Access for Random {}

/// [`Static`] or [`Dynamic`] dispatch.
#[sealed]
pub trait Dispatch: 'static {}

/// Static dispatch.
///
/// You have to specify all codes used of the graph. The defaults
/// are the same as the default parameters of the Java version.
#[derive(Debug, Clone)]
pub struct Static<
    const OUTDEGREES: usize = { code_consts::GAMMA },
    const REFERENCES: usize = { code_consts::UNARY },
    const BLOCKS: usize = { code_consts::GAMMA },
    const INTERVALS: usize = { code_consts::GAMMA },
    const RESIDUALS: usize = { code_consts::ZETA3 },
> {}

#[sealed]
impl<
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    > Dispatch for Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>
{
}

/// Dynamic dispatch.
///
/// Parameters are retrieved from the graph properties.
#[derive(Debug, Clone)]
pub struct Dynamic {}

#[sealed]
impl Dispatch for Dynamic {}

/// Load mode.
///
/// The load mode is the way the graph data is accessed. Each load mode has
/// a corresponding strategy to access the graph and the offsets.
///
/// You can set both modes with [`LoadConfig::mode`], or set them separately with
/// [`LoadConfig::graph_mode`] and [`LoadConfig::offsets_mode`].
#[sealed]
pub trait LoadMode<E: Endianness>: 'static {
    type Factory: CodeReaderFactory<E>;

    fn new_factory<P: AsRef<Path>>(graph: P, flags: codecs::MemoryFlags) -> Result<Self::Factory>;

    type Offsets: IndexedSeq<Input = usize, Output = usize>;

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>>;
}

/// A type alias for a buffered reader that reads from a memory buffer a `u32` at a time.
pub type MemBufReader<'a, E> = BufBitReader<E, MemWordReader<u32, &'a [u32]>>;
/// A type alias for a buffered reader that reads from a file buffer a `u32` at a time.
pub type FileBufReader<E> = BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>;
/// A type alias for the code reader returned by the [`CodeReaderFactory`]
/// associated with a [`LoadMode`].
pub type LoadModeCodeReader<'a, E, LM> =
    <<LM as LoadMode<E>>::Factory as CodeReaderFactory<E>>::CodeReader<'a>;

/// The graph is read from a file; offsets are fully deserialized in memory.
///
/// Note that you must guarantee that the graph file is padded with enough
/// zeroes so that it can be read one `u32` at a time.
#[derive(Debug, Clone)]
pub struct File {}
#[sealed]
impl<E: Endianness> LoadMode<E> for File
where
    FileBufReader<E>: BitRead<E, Error = io::Error> + CodesRead<E>,
{
    type Factory = FileFactory<E>;
    type Offsets = EF;

    fn new_factory<P: AsRef<Path>>(graph: P, _flags: MemoryFlags) -> Result<Self::Factory> {
        FileFactory::<E>::new(graph)
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        _flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();
        Ok(EF::load_full(path)
            .with_context(|| format!("Cannot load Elias-Fano pointer list {}", path.display()))?
            .into())
    }
}

/// The graph and offsets are memory mapped.
///
/// This is the default mode. You can [set memory-mapping flags](LoadConfig::flags).
#[derive(Debug, Clone)]
pub struct Mmap {}
#[sealed]
impl<E: Endianness> LoadMode<E> for Mmap
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    type Factory = MmapHelper<u32>;
    type Offsets = DeserType<'static, EF>;

    fn new_factory<P: AsRef<Path>>(graph: P, flags: MemoryFlags) -> Result<Self::Factory> {
        MmapHelper::mmap(graph, flags.into())
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();
        EF::mmap(path, flags.into())
            .with_context(|| format!("Cannot map Elias-Fano pointer list {}", path.display()))
    }
}

/// The graph and offsets are loaded into allocated memory.
#[derive(Debug, Clone)]
pub struct LoadMem {}
#[sealed]
impl<E: Endianness> LoadMode<E> for LoadMem
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    type Factory = MemoryFactory<E, Box<[u32]>>;
    type Offsets = DeserType<'static, EF>;

    fn new_factory<P: AsRef<Path>>(graph: P, _flags: MemoryFlags) -> Result<Self::Factory> {
        MemoryFactory::<E, _>::new_mem(graph)
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        _flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();
        EF::load_mem(path)
            .with_context(|| format!("Cannot load Elias-Fano pointer list {}", path.display()))
    }
}

/// The graph and offsets are loaded into memory obtained via `mmap()`.
///
/// You can [set memory-mapping flags](LoadConfig::flags).
#[derive(Debug, Clone)]
pub struct LoadMmap {}
#[sealed]
impl<E: Endianness> LoadMode<E> for LoadMmap
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    type Factory = MemoryFactory<E, MmapHelper<u32>>;
    type Offsets = DeserType<'static, EF>;

    fn new_factory<P: AsRef<Path>>(graph: P, flags: MemoryFlags) -> Result<Self::Factory> {
        MemoryFactory::<E, _>::new_mmap(graph, flags)
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();
        EF::load_mmap(path, flags.into())
            .with_context(|| format!("Cannot load Elias-Fano pointer list {}", path.display()))
    }
}

/// A load configuration for a [`BvGraph`]/[`BvGraphSeq`].
///
/// A basic configuration is returned by
/// [`BvGraph::with_basename`]/[`BvGraphSeq::with_basename`]. The configuration
/// can then be customized using the methods of this struct.
#[derive(Debug, Clone)]
pub struct LoadConfig<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<E>, OLM: LoadMode<E>> {
    pub(crate) basename: PathBuf,
    pub(crate) graph_load_flags: MemoryFlags,
    pub(crate) offsets_load_flags: MemoryFlags,
    pub(crate) _marker: std::marker::PhantomData<(E, A, D, GLM, OLM)>,
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<E>, OLM: LoadMode<E>>
    LoadConfig<E, A, D, GLM, OLM>
{
    /// Set the endianness of the graph and offsets file.
    pub fn endianness<E2: Endianness>(self) -> LoadConfig<E2, A, D, GLM, OLM>
    where
        GLM: LoadMode<E2>,
        OLM: LoadMode<E2>,
    {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<E>, OLM: LoadMode<E>>
    LoadConfig<E, A, D, GLM, OLM>
{
    /// Choose between [`Static`] and [`Dynamic`] dispatch.
    pub fn dispatch<D2: Dispatch>(self) -> LoadConfig<E, A, D2, GLM, OLM> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<E>, OLM: LoadMode<E>>
    LoadConfig<E, A, D, GLM, OLM>
{
    /// Choose the [`LoadMode`] for the graph and offsets.
    pub fn mode<LM: LoadMode<E>>(self) -> LoadConfig<E, A, D, LM, LM> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch> LoadConfig<E, A, D, Mmap, Mmap>
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    /// Set flags for memory-mapping (both graph and offsets).
    pub fn flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, Mmap, Mmap> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch> LoadConfig<E, A, D, LoadMmap, LoadMmap>
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    /// Set flags for memory obtained from `mmap()` (both graph and offsets).
    pub fn flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, LoadMmap, LoadMmap> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<E>, OLM: LoadMode<E>>
    LoadConfig<E, A, D, GLM, OLM>
{
    /// Choose the [`LoadMode`] for the graph only.
    pub fn graph_mode<NGLM: LoadMode<E>>(self) -> LoadConfig<E, A, D, NGLM, OLM> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, OLM: LoadMode<E>> LoadConfig<E, A, D, Mmap, OLM>
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    /// Set flags for memory-mapping the graph.
    pub fn graph_flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, Mmap, OLM> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, OLM: LoadMode<E>> LoadConfig<E, A, D, LoadMmap, OLM>
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    /// Set flags for memory obtained from `mmap()` for the graph.
    pub fn graph_flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, LoadMmap, OLM> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode<E>, OLM: LoadMode<E>>
    LoadConfig<E, Random, D, GLM, OLM>
{
    /// Choose the [`LoadMode`] for the graph only.
    pub fn offsets_mode<NOLM: LoadMode<E>>(self) -> LoadConfig<E, Random, D, GLM, NOLM> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode<E>> LoadConfig<E, Random, D, GLM, Mmap>
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    /// Set flags for memory-mapping the offsets.
    pub fn offsets_flags(self, flags: MemoryFlags) -> LoadConfig<E, Random, D, GLM, Mmap> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode<E>> LoadConfig<E, Random, D, GLM, LoadMmap>
where
    for<'a> MemBufReader<'a, E>: BitRead<E, Error = Infallible> + CodesRead<E>,
{
    /// Set flags for memory obtained from `mmap()` for the graph.
    pub fn offsets_flags(self, flags: MemoryFlags) -> LoadConfig<E, Random, D, GLM, LoadMmap> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, GLM: LoadMode<E>, OLM: LoadMode<E>> LoadConfig<E, Random, Dynamic, GLM, OLM> {
    /// Load a random-access graph with dynamic dispatch.
    #[allow(clippy::type_complexity)]
    pub fn load(
        mut self,
    ) -> anyhow::Result<BvGraph<DynCodesDecoderFactory<E, GLM::Factory, OLM::Offsets>>>
    where
        for<'a> LoadModeCodeReader<'a, E, GLM>: CodesRead<E> + BitSeek,
    {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;
        self.basename.set_extension(EF_EXTENSION);
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)?;

        Ok(BvGraph::new(
            DynCodesDecoderFactory::new(factory, offsets, comp_flags)?,
            num_nodes,
            num_arcs,
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

impl<E: Endianness, GLM: LoadMode<E>, OLM: LoadMode<E>>
    LoadConfig<E, Sequential, Dynamic, GLM, OLM>
{
    /// Load a sequential graph with dynamic dispatch.
    #[allow(clippy::type_complexity)]
    pub fn load(
        mut self,
    ) -> anyhow::Result<BvGraphSeq<DynCodesDecoderFactory<E, GLM::Factory, EmptyDict<usize, usize>>>>
    where
        for<'a> LoadModeCodeReader<'a, E, GLM>: CodesRead<E>,
    {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BvGraphSeq::new(
            DynCodesDecoderFactory::new(factory, MemCase::from(EmptyDict::default()), comp_flags)?,
            num_nodes,
            Some(num_arcs),
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

impl<
        E: Endianness,
        GLM: LoadMode<E>,
        OLM: LoadMode<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    >
    LoadConfig<E, Random, Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>, GLM, OLM>
{
    /// Load a random-access graph with static dispatch.
    #[allow(clippy::type_complexity)]
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BvGraph<
            ConstCodesDecoderFactory<
                E,
                GLM::Factory,
                OLM::Offsets,
                OUTDEGREES,
                REFERENCES,
                BLOCKS,
                INTERVALS,
                RESIDUALS,
            >,
        >,
    >
    where
        for<'a> LoadModeCodeReader<'a, E, GLM>: CodesRead<E> + BitSeek,
    {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;
        self.basename.set_extension(EF_EXTENSION);
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)?;

        Ok(BvGraph::new(
            ConstCodesDecoderFactory::new(factory, offsets, comp_flags)?,
            num_nodes,
            num_arcs,
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

impl<
        E: Endianness,
        GLM: LoadMode<E>,
        OLM: LoadMode<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    >
    LoadConfig<
        E,
        Sequential,
        Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>,
        GLM,
        OLM,
    >
{
    /// Load a sequential graph with static dispatch.
    #[allow(clippy::type_complexity)]
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BvGraphSeq<
            ConstCodesDecoderFactory<
                E,
                GLM::Factory,
                EmptyDict<usize, usize>,
                OUTDEGREES,
                REFERENCES,
                BLOCKS,
                INTERVALS,
                RESIDUALS,
            >,
        >,
    >
    where
        for<'a> LoadModeCodeReader<'a, E, GLM>: CodesRead<E>,
    {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BvGraphSeq::new(
            ConstCodesDecoderFactory::new(
                factory,
                MemCase::from(EmptyDict::default()),
                comp_flags,
            )?,
            num_nodes,
            Some(num_arcs),
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

/// Read the .properties file and return the endianness
pub fn get_endianness<P: AsRef<Path>>(basename: P) -> Result<String> {
    let path = basename.as_ref().with_extension(PROPERTIES_EXTENSION);
    let f = std::fs::File::open(&path)
        .with_context(|| format!("Cannot open property file {}", path.display()))?;
    let map = java_properties::read(BufReader::new(f))
        .with_context(|| format!("cannot parse {} as a java properties file", path.display()))?;

    let endianness = map
        .get("endianness")
        .map(|x| x.to_string())
        .unwrap_or_else(|| BigEndian::NAME.to_string());

    Ok(endianness)
}

/// Read the .properties file and return the number of nodes, number of arcs and compression flags
/// for the graph. The endianness is checked against the expected one.
pub fn parse_properties<E: Endianness>(path: impl AsRef<Path>) -> Result<(usize, u64, CompFlags)> {
    let name = path.as_ref().display();
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

    let comp_flags = CompFlags::from_properties::<E>(&map)
        .with_context(|| format!("Cannot parse compression flags from {}", name))?;
    Ok((num_nodes, num_arcs, comp_flags))
}
