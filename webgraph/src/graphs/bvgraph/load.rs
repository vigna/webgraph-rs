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
use dsi_bitstream::{dispatch::code_consts, dispatch::factory::CodesReaderFactoryHelper};
use epserde::prelude::*;
use sealed::sealed;
use std::{
    io::BufReader,
    path::{Path, PathBuf},
};
use sux::traits::IndexedSeq;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
/// A wrapper that implements `AsRef` for any type.
pub struct Identity<T>(pub T);

impl<T> AsRef<T> for Identity<T> {
    #[inline(always)]
    fn as_ref(&self) -> &T {
        &self.0
    }
}
impl<T> From<T> for Identity<T> {
    #[inline(always)]
    fn from(t: T) -> Self {
        Self(t)
    }
}

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
pub trait LoadMode<O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>>: 'static {
    type Factory<E: Endianness>;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        flags: codecs::MemoryFlags,
    ) -> Result<Self::Factory<E>>;

    type Offsets: AsRef<O>;

    fn load_offsets<P: AsRef<Path>>(offsets: P, flags: MemoryFlags) -> Result<Self::Offsets>;
}

/// A type alias for a buffered reader that reads from a memory buffer a `u32` at a time.
pub type MemBufReader<'a, E> = BufBitReader<E, MemWordReader<u32, &'a [u32]>>;
/// A type alias for a buffered reader that reads from a file buffer a `u32` at a time.
pub type FileBufReader<E> = BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>;
/// A type alias for the [`CodesReaderFactory`] associated with a [`LoadMode`].
///
/// This type can be used in client methods that abstract over endianness to
/// impose the necessary trait bounds on the factory associated with the load
/// mode: one has just to write, for example, for the [`Mmap`] load mode:
/// ```ignore
/// LoadModeFactory<E, Mmap, O>: CodesReaderFactoryHelper<E>
/// ```
///
/// Additional trait bounds on the [`CodesRead`] associated with the factory
/// can be imposed by using the [`LoadModeCodesReader`] type alias.
pub type LoadModeFactory<E, LM, O> = <LM as LoadMode<O>>::Factory<E>;
/// A type alias for the code reader returned by the [`CodesReaderFactory`]
/// associated with a [`LoadMode`].
///
/// This type can be used in client methods that abstract over endianness to
/// impose bounds on the code reader associated to the factory associated with
/// the load mode, usually in conjunction with [`LoadModeFactory`]. For example,
/// for the [`Mmap`] load mode:
/// ```ignore
/// LoadModeFactory<E, Mmap, O>: CodesReaderFactoryHelper<E>
/// LoadModeCodesReader<'a, E, Mmap, O>: BitSeek
/// ```
pub type LoadModeCodesReader<'a, E, LM, O> =
    <LoadModeFactory<E, LM, O> as CodesReaderFactory<E>>::CodesReader<'a>;

/// The graph is read from a file; offsets are fully deserialized in memory.
///
/// Note that you must guarantee that the graph file is padded with enough
/// zeroes so that it can be read one `u32` at a time.
#[derive(Debug, Clone)]
pub struct File {}
#[sealed]
impl LoadMode<EF> for File {
    type Factory<E: Endianness> = FileFactory<E>;
    type Offsets = Identity<EF>;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        _flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        FileFactory::<E>::new(graph)
    }

    fn load_offsets<P: AsRef<Path>>(offsets: P, _flags: MemoryFlags) -> Result<Self::Offsets> {
        let path = offsets.as_ref();
        Ok(Identity(
            unsafe { EF::load_full(path) }
                .with_context(|| format!("Cannot load Elias-Fano pointer list {}", path.display()))?,
        ))
    }
}

/// The graph and offsets are memory mapped.
///
/// This is the default mode. You can [set memory-mapping flags](LoadConfig::flags).
#[derive(Debug, Clone)]
pub struct Mmap {}
#[sealed]
impl LoadMode<EF> for Mmap {
    type Factory<E: Endianness> = MmapHelper<u32>;
    type Offsets = MemCase<EF>;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        MmapHelper::mmap(graph, flags.into())
    }

    fn load_offsets<P: AsRef<Path>>(offsets: P, flags: MemoryFlags) -> Result<Self::Offsets> {
        let path = offsets.as_ref();
        unsafe {
            EF::mmap(path, flags.into())
                .with_context(|| format!("Cannot map Elias-Fano pointer list {}", path.display()))
        }
    }
}

/// The graph and offsets are loaded into allocated memory.
#[derive(Debug, Clone)]
pub struct LoadMem {}
#[sealed]
impl LoadMode<EF> for LoadMem {
    type Factory<E: Endianness> = MemoryFactory<E, Box<[u32]>>;
    type Offsets = MemCase<EF>;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        _flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        MemoryFactory::<E, _>::new_mem(graph)
    }

    fn load_offsets<P: AsRef<Path>>(offsets: P, _flags: MemoryFlags) -> Result<Self::Offsets> {
        let path = offsets.as_ref();
        unsafe {
            EF::load_mem(path)
                .with_context(|| format!("Cannot load Elias-Fano pointer list {}", path.display()))
        }
    }
}

/// The graph and offsets are loaded into memory obtained via `mmap()`.
///
/// You can [set memory-mapping flags](LoadConfig::flags).
#[derive(Debug, Clone)]
pub struct LoadMmap {}
#[sealed]
impl LoadMode<EF> for LoadMmap {
    type Factory<E: Endianness> = MemoryFactory<E, MmapHelper<u32>>;
    type Offsets = MemCase<EF>;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        MemoryFactory::<E, _>::new_mmap(graph, flags)
    }

    fn load_offsets<P: AsRef<Path>>(offsets: P, flags: MemoryFlags) -> Result<Self::Offsets> {
        let path = offsets.as_ref();
        unsafe {
            EF::load_mmap(path, flags.into())
                .with_context(|| format!("Cannot load Elias-Fano pointer list {}", path.display()))
        }
    }
}

/// A load configuration for a [`BvGraph`]/[`BvGraphSeq`].
///
/// A basic configuration is returned by
/// [`BvGraph::with_basename`]/[`BvGraphSeq::with_basename`]. The configuration
/// can then be customized using the methods of this struct.
#[derive(Debug, Clone)]
pub struct LoadConfig<
    E: Endianness,
    A: Access,
    D: Dispatch,
    GLM: LoadMode<O>,
    OLM: LoadMode<O>,
    O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize> = EF,
> {
    pub(crate) basename: PathBuf,
    pub(crate) graph_load_flags: MemoryFlags,
    pub(crate) offsets_load_flags: MemoryFlags,
    pub(crate) _marker: std::marker::PhantomData<(E, A, D, GLM, OLM, O)>,
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<O>, OLM: LoadMode<O>, O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>>
    LoadConfig<E, A, D, GLM, OLM, O>
{
    /// Set the endianness of the graph and offsets file.
    pub fn endianness<E2: Endianness>(self) -> LoadConfig<E2, A, D, GLM, OLM, O>
    where
        GLM: LoadMode<O>,
        OLM: LoadMode<O>,
    {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<O>, OLM: LoadMode<O>, O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>>
    LoadConfig<E, A, D, GLM, OLM, O>
{
    /// Choose between [`Static`] and [`Dynamic`] dispatch.
    pub fn dispatch<D2: Dispatch>(self) -> LoadConfig<E, A, D2, GLM, OLM, O> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<O>, OLM: LoadMode<O>, O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>>
    LoadConfig<E, A, D, GLM, OLM, O>
{
    /// Choose the [`LoadMode`] for the graph and offsets.
    pub fn mode<LM: LoadMode<O>>(self) -> LoadConfig<E, A, D, LM, LM, O> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch> LoadConfig<E, A, D, Mmap, Mmap, EF> {
    /// Set flags for memory-mapping (both graph and offsets).
    pub fn flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, Mmap, Mmap, EF> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch> LoadConfig<E, A, D, LoadMmap, LoadMmap, EF> {
    /// Set flags for memory obtained from `mmap()` (both graph and offsets).
    pub fn flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, LoadMmap, LoadMmap, EF> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode<O>, OLM: LoadMode<O>, O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>>
    LoadConfig<E, A, D, GLM, OLM, O>
{
    /// Choose the [`LoadMode`] for the graph only.
    pub fn graph_mode<NGLM: LoadMode<O>>(self) -> LoadConfig<E, A, D, NGLM, OLM, O> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, OLM: LoadMode<EF>> LoadConfig<E, A, D, Mmap, OLM, EF> {
    /// Set flags for memory-mapping the graph.
    pub fn graph_flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, Mmap, OLM, EF> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, OLM: LoadMode<EF>>
    LoadConfig<E, A, D, LoadMmap, OLM, EF>
{
    /// Set flags for memory obtained from `mmap()` for the graph.
    pub fn graph_flags(self, flags: MemoryFlags) -> LoadConfig<E, A, D, LoadMmap, OLM, EF> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode<O>, OLM: LoadMode<O>, O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>> LoadConfig<E, Random, D, GLM, OLM, O> {
    /// Choose the [`LoadMode`] for the graph only.
    pub fn offsets_mode<NOLM: LoadMode<O>>(self) -> LoadConfig<E, Random, D, GLM, NOLM, O> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode<EF>> LoadConfig<E, Random, D, GLM, Mmap, EF> {
    /// Set flags for memory-mapping the offsets.
    pub fn offsets_flags(self, flags: MemoryFlags) -> LoadConfig<E, Random, D, GLM, Mmap, EF> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode<EF>> LoadConfig<E, Random, D, GLM, LoadMmap, EF> {
    /// Set flags for memory obtained from `mmap()` for the graph.
    pub fn offsets_flags(self, flags: MemoryFlags) -> LoadConfig<E, Random, D, GLM, LoadMmap, EF> {
        LoadConfig {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: flags,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<
        E: Endianness,
        GLM: LoadMode<O>,
        OLM: LoadMode<O>,
        O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>,
    > LoadConfig<E, Random, Dynamic, GLM, OLM, O>
{
    /// Load a random-access graph with dynamic dispatch.
    #[allow(clippy::type_complexity)]
    pub fn load(
        mut self,
    ) -> anyhow::Result<BvGraph<DynCodesDecoderFactory<E, GLM::Factory<E>, OLM::Offsets, O>>>
    where
        <GLM as LoadMode<O>>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM, O>: CodesRead<E> + BitSeek,
    {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)
            .with_context(|| {
                format!("Could not load properties file {}", self.basename.display())
            })?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)
            .with_context(|| format!("Could not graph file {}", self.basename.display()))?;
        self.basename.set_extension(EF_EXTENSION);
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)
            .with_context(|| format!("Could not offsets file {}", self.basename.display()))?;

        Ok(BvGraph::new(
            DynCodesDecoderFactory::new(factory, offsets, comp_flags)?,
            num_nodes,
            num_arcs,
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

impl<
        E: Endianness,
        GLM: LoadMode<O>,
        OLM: LoadMode<O>,
        O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>,
    > LoadConfig<E, Sequential, Dynamic, GLM, OLM, O>
{
    /// Load a sequential graph with dynamic dispatch.
    #[allow(clippy::type_complexity)]
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BvGraphSeq<
            DynCodesDecoderFactory<
                E,
                GLM::Factory<E>,
                Identity<EmptyDict<usize, usize>>,
                EmptyDict<usize, usize>,
            >,
        >,
    >
    where
        <GLM as LoadMode<O>>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM, O>: CodesRead<E>,
    {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BvGraphSeq::new(
            DynCodesDecoderFactory::new(factory, Identity(EmptyDict::default()), comp_flags)?,
            num_nodes,
            Some(num_arcs),
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

impl<
        E: Endianness,
        GLM: LoadMode<O>,
        OLM: LoadMode<O>,
        O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
    >
    LoadConfig<
        E,
        Random,
        Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>,
        GLM,
        OLM,
        O,
    >
{
    /// Load a random-access graph with static dispatch.
    #[allow(clippy::type_complexity)]
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BvGraph<
            ConstCodesDecoderFactory<
                E,
                GLM::Factory<E>,
                OLM::Offsets,
                O,
                OUTDEGREES,
                REFERENCES,
                BLOCKS,
                INTERVALS,
                RESIDUALS,
            >,
        >,
    >
    where
        <GLM as LoadMode<O>>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM, O>: CodesRead<E> + BitSeek,
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
        GLM: LoadMode<O>,
        OLM: LoadMode<O>,
        O: for<'a> IndexedSeq<Input = usize, Output<'a> = usize>,
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
        O,
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
                GLM::Factory<E>,
                Identity<EmptyDict<usize, usize>>,
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
        <GLM as LoadMode<O>>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM, O>: CodesRead<E>,
    {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)?;

        Ok(BvGraphSeq::new(
            ConstCodesDecoderFactory::new(factory, Identity(EmptyDict::default()), comp_flags)?,
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
    let f =
        std::fs::File::open(&path).with_context(|| format!("Cannot open property file {name}"))?;
    let map = java_properties::read(BufReader::new(f))
        .with_context(|| format!("cannot parse {name} as a java properties file"))?;

    let num_nodes = map
        .get("nodes")
        .with_context(|| format!("Missing 'nodes' property in {name}"))?
        .parse::<usize>()
        .with_context(|| format!("Cannot parse 'nodes' as usize in {name}"))?;
    let num_arcs = map
        .get("arcs")
        .with_context(|| format!("Missing 'arcs' property in {name}"))?
        .parse::<u64>()
        .with_context(|| format!("Cannot parse arcs as usize in {name}"))?;

    let comp_flags = CompFlags::from_properties::<E>(&map)
        .with_context(|| format!("Cannot parse compression flags from {name}"))?;
    Ok((num_nodes, num_arcs, comp_flags))
}
