/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use super::*;
use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use dsi_bitstream::{dispatch::code_consts, dispatch::factory::CodesReaderFactoryHelper};
use epserde::deser::Owned;
use epserde::prelude::*;
use sealed::sealed;
use std::{
    collections::HashMap,
    io::BufReader,
    path::{Path, PathBuf},
    str::FromStr,
};

/// Sequential or random access.
#[doc(hidden)]
#[sealed]
pub trait Access: 'static {}

#[derive(Debug, Clone)]
pub struct Sequential;
#[sealed]
impl Access for Sequential {}

#[derive(Debug, Clone)]
pub struct Random;
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
>;

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
pub struct Dynamic;

#[sealed]
impl Dispatch for Dynamic {}

/// Load mode.
///
/// The load mode is the way the graph data is accessed. Each load mode has
/// a corresponding strategy to access the graph and the offsets.
///
/// You can set both modes with [`LoadConf::mode`], or set them separately with
/// [`LoadConf::graph_mode`] and [`LoadConf::offsets_mode`].
#[sealed]
pub trait LoadMode: 'static {
    type Factory<E: Endianness>;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        flags: codecs::MemoryFlags,
    ) -> Result<Self::Factory<E>>;

    type Offsets: Offsets;

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>>;
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
/// LoadModeFactory<E, Mmap>: CodesReaderFactoryHelper<E>
/// ```
///
/// Additional trait bounds on the [`CodesRead`] associated with the factory
/// can be imposed by using the [`LoadModeCodesReader`] type alias.
pub type LoadModeFactory<E, LM> = <LM as LoadMode>::Factory<E>;
/// A type alias for the code reader returned by the [`CodesReaderFactory`]
/// associated with a [`LoadMode`].
///
/// This type can be used in client methods that abstract over endianness to
/// impose bounds on the code reader associated to the factory associated with
/// the load mode, usually in conjunction with [`LoadModeFactory`]. For example,
/// for the [`Mmap`] load mode:
/// ```ignore
/// LoadModeFactory<E, Mmap>: CodesReaderFactoryHelper<E>
/// LoadModeCodesReader<'a, E, Mmap>: BitSeek
/// ```
pub type LoadModeCodesReader<'a, E, LM> =
    <LoadModeFactory<E, LM> as CodesReaderFactory<E>>::CodesReader<'a>;

/// The graph is read from a file; offsets are fully deserialized in memory.
///
/// Note that you must guarantee that the graph file is padded with enough
/// zeroes so that it can be read one `u32` at a time.
#[derive(Debug, Clone)]
pub struct File;
#[sealed]
impl LoadMode for File {
    type Factory<E: Endianness> = FileFactory<E>;
    type Offsets = Owned<EF>;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        _flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        FileFactory::<E>::new(graph)
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        _flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();

        unsafe {
            EF::load_full(path)
                .with_context(|| format!("Cannot load Elias–Fano pointer list {}", path.display()))
                .map(Into::into)
        }
    }
}

/// The graph and offsets are memory mapped.
///
/// This is the default mode. You can [set memory-mapping flags].
///
/// [set memory-mapping flags]: LoadConf::flags
#[derive(Debug, Clone)]
pub struct Mmap;
#[sealed]
impl LoadMode for Mmap {
    type Factory<E: Endianness> = MmapHelper<u32>;
    type Offsets = EF;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        MmapHelper::mmap(graph, flags.into())
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();

        unsafe {
            EF::mmap(path, flags.into())
                .with_context(|| format!("Cannot map Elias–Fano pointer list {}", path.display()))
        }
    }
}

/// The graph and offsets are loaded into allocated memory.
#[derive(Debug, Clone)]
pub struct LoadMem;
#[sealed]
impl LoadMode for LoadMem {
    type Factory<E: Endianness> = MemoryFactory<E, Box<[u32]>>;
    type Offsets = EF;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        _flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        MemoryFactory::<E, _>::new_mem(graph)
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        _flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();

        unsafe {
            EF::load_mem(path)
                .with_context(|| format!("Cannot load Elias–Fano pointer list {}", path.display()))
        }
    }
}

/// The graph and offsets are loaded into memory obtained via `mmap()`.
///
/// You can [set memory-mapping flags].
///
/// [set memory-mapping flags]: LoadConf::flags
#[derive(Debug, Clone)]
pub struct LoadMmap;
#[sealed]
impl LoadMode for LoadMmap {
    type Factory<E: Endianness> = MemoryFactory<E, MmapHelper<u32>>;
    type Offsets = EF;

    fn new_factory<E: Endianness, P: AsRef<Path>>(
        graph: P,
        flags: MemoryFlags,
    ) -> Result<Self::Factory<E>> {
        MemoryFactory::<E, _>::new_mmap(graph, flags)
    }

    fn load_offsets<P: AsRef<Path>>(
        offsets: P,
        flags: MemoryFlags,
    ) -> Result<MemCase<Self::Offsets>> {
        let path = offsets.as_ref();

        unsafe {
            EF::load_mmap(path, flags.into())
                .with_context(|| format!("Cannot load Elias–Fano pointer list {}", path.display()))
        }
    }
}

/// A [`LoadMode`] using `mmap()`, and thus accepting [memory flags]: [`Mmap`]
/// or [`LoadMmap`].
///
/// [memory flags]: MemoryFlags
#[sealed]
pub trait MmapMode: LoadMode {}

#[sealed]
impl MmapMode for Mmap {}

#[sealed]
impl MmapMode for LoadMmap {}

/// A load configuration for a [`BvGraph`]/[`BvGraphSeq`].
///
/// A basic configuration is returned by
/// [`BvGraph::with_basename`]/[`BvGraphSeq::with_basename`]. The configuration
/// can then be customized using the setter methods of this struct, chained in
/// builder style, and finalized by calling [`load`].
///
/// # Defaults
///
/// The default configuration returned by `with_basename` uses:
/// - big endianness ([`BE`]);
/// - [dynamic dispatch];
/// - [memory mapping] for both the graph and the offsets.
///
/// # Configuration Axes
///
/// ## Access Mode
///
/// - [`BvGraph::with_basename`] returns a configuration for **random access**,
///   which requires the Elias–Fano offsets file (`.ef`). The resulting graph
///   supports both random access and sequential iteration.
/// - [`BvGraphSeq::with_basename`] returns a configuration for **sequential
///   access**, which only needs the graph file (`.graph`). The resulting graph
///   supports only sequential iteration.
///
/// ## Endianness
///
/// - [`endianness`]: sets the endianness of the graph file. Use
///   `endianness::<BE>()` for big-endian (the default and the Java convention)
///   or `endianness::<LE>()` for little-endian.
///
/// ## Code Dispatch
///
/// - [`dispatch`]: chooses between:
///   - [`Dynamic`] (default): reads the codes from the properties file;
///     slightly slower due to indirect dispatch, but works with any graph.
///   - [`Static`]: the codes are fixed at compile time via const generics,
///     enabling more aggressive optimization. The defaults match the Java
///     defaults (γ for outdegrees, unary for references, γ for blocks, γ for
///     intervals, ζ₃ for residuals). If your graph uses non-default codes,
///     you must specify them explicitly.
///
/// ## Load Mode
///
/// Controls how the graph bitstream and the offsets are accessed.
///
/// - [`mode`]: sets the load mode for **both** the graph and the offsets. You
///   can also set them independently:
///   - [`graph_mode`]: sets the mode for the graph only;
///   - [`offsets_mode`]: sets the mode for the offsets only (random access
///     only).
///
/// The available modes are:
///
/// - [`Mmap`] (default): memory maps the file. This is the most
///   memory-efficient mode, as the OS manages paging. It is the recommended
///   mode for large graphs.
/// - [`LoadMem`]: reads the file into allocated memory.
/// - [`LoadMmap`]: reads the file into memory obtained via `mmap`, rather than
///   the standard allocator.
/// - [`File`]: reads the graph from a file stream. The offsets are fully
///   deserialized in memory using [ε-serde]'s [`load_full`]. Note that the
///   graph file must be padded correctly for this mode.
///
/// ## Memory flags
///
/// When using [`Mmap`] or [`LoadMmap`], you can set [`MemoryFlags`] to
/// request transparent huge pages, etc.:
///
/// - [`flags`]: sets flags for both the graph and offsets.
/// - [`graph_flags`]: sets flags for the graph only.
/// - [`offsets_flags`]: sets flags for the offsets only (random access only).
///
/// # Examples
///
/// Load with all defaults (big-endian, dynamic dispatch, memory-mapped):
/// ```ignore
/// let graph = BvGraph::with_basename("BASENAME").load()?;
/// ```
///
/// Load a little-endian graph:
/// ```ignore
/// let graph = BvGraph::with_basename("BASENAME")
///     .endianness::<LE>()
///     .load()?;
/// ```
///
/// Load with static dispatch (using default codes):
/// ```ignore
/// let graph = BvGraph::with_basename("BASENAME")
///     .dispatch::<Static>()
///     .load()?;
/// ```
///
/// Load into memory rather than memory-mapping:
/// ```ignore
/// let graph = BvGraph::with_basename("BASENAME")
///     .mode::<LoadMem>()
///     .load()?;
/// ```
///
/// Load a sequential-access graph (no `.ef` file needed):
/// ```ignore
/// let graph = BvGraphSeq::with_basename("BASENAME").load()?;
/// ```
///
/// Combine options:
/// ```ignore
/// let graph = BvGraph::with_basename("BASENAME")
///     .endianness::<LE>()
///     .dispatch::<Static>()
///     .mode::<LoadMem>()
///     .load()?;
/// ```
///
/// [`load`]: LoadConf::load
/// [dynamic dispatch]: `Dynamic`
/// [memory mapping]: `Mmap`
/// [`endianness`]: LoadConf::endianness
/// [`dispatch`]: LoadConf::dispatch
/// [`mode`]: LoadConf::mode
/// [`graph_mode`]: LoadConf::graph_mode
/// [`offsets_mode`]: LoadConf::offsets_mode
/// [`load_full`]: epserde::deser::Deserialize::load_full
/// [`flags`]: LoadConf::flags
/// [`graph_flags`]: LoadConf::graph_flags
/// [`offsets_flags`]: LoadConf::offsets_flags
/// [ε-serde]: <https://docs.rs/epserde/latest/epserde/>
#[derive(Debug, Clone)]
pub struct LoadConf<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode, OLM: LoadMode> {
    pub(crate) basename: PathBuf,
    pub(crate) graph_load_flags: MemoryFlags,
    pub(crate) offsets_load_flags: MemoryFlags,
    pub(crate) _marker: std::marker::PhantomData<(E, A, D, GLM, OLM)>,
}

impl<A: Access> LoadConf<BE, A, Dynamic, Mmap, Mmap> {
    /// Returns the default configuration for the given basename.
    pub(crate) fn new(basename: impl AsRef<Path>) -> Self {
        LoadConf {
            basename: basename.as_ref().to_owned(),
            graph_load_flags: MemoryFlags::empty(),
            offsets_load_flags: MemoryFlags::empty(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: LoadMode, OLM: LoadMode>
    LoadConf<E, A, D, GLM, OLM>
{
    /// Changes the type parameters, keeping the basename and the flags.
    fn retype<E2: Endianness, A2: Access, D2: Dispatch, GLM2: LoadMode, OLM2: LoadMode>(
        self,
    ) -> LoadConf<E2, A2, D2, GLM2, OLM2> {
        LoadConf {
            basename: self.basename,
            graph_load_flags: self.graph_load_flags,
            offsets_load_flags: self.offsets_load_flags,
            _marker: std::marker::PhantomData,
        }
    }

    /// Sets the endianness of the graph and offsets file.
    pub fn endianness<E2: Endianness>(self) -> LoadConf<E2, A, D, GLM, OLM> {
        self.retype()
    }

    /// Chooses between [`Static`] and [`Dynamic`] dispatch.
    pub fn dispatch<D2: Dispatch>(self) -> LoadConf<E, A, D2, GLM, OLM> {
        self.retype()
    }

    /// Chooses the [`LoadMode`] for the graph and offsets.
    pub fn mode<LM: LoadMode>(self) -> LoadConf<E, A, D, LM, LM> {
        self.retype()
    }

    /// Chooses the [`LoadMode`] for the graph only.
    pub fn graph_mode<NGLM: LoadMode>(self) -> LoadConf<E, A, D, NGLM, OLM> {
        self.retype()
    }

    /// Reads the properties file and creates the factory for the graph file.
    fn load_graph(&mut self) -> Result<(usize, u64, CompFlags, GLM::Factory<E>)> {
        self.basename.set_extension(PROPERTIES_EXTENSION);
        let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&self.basename)
            .with_context(|| {
                format!("Could not load properties file {}", self.basename.display())
            })?;
        self.basename.set_extension(GRAPH_EXTENSION);
        let factory = GLM::new_factory(&self.basename, self.graph_load_flags)
            .with_context(|| format!("Could not load graph file {}", self.basename.display()))?;
        Ok((num_nodes, num_arcs, comp_flags, factory))
    }

    /// Loads the offsets file, checking that it contains `num_nodes` + 1
    /// offsets.
    fn load_offsets(&mut self, num_nodes: usize) -> Result<MemCase<OLM::Offsets>> {
        self.basename.set_extension(EF_EXTENSION);
        let offsets = OLM::load_offsets(&self.basename, self.offsets_load_flags)
            .with_context(|| format!("Could not load offsets file {}", self.basename.display()))?;
        // The decoders index the offsets without bounds checks, so a stale
        // or truncated file must be rejected here.
        let num_offsets = value_traits::slices::SliceByValue::len(offsets.uncase());
        anyhow::ensure!(
            num_offsets == num_nodes + 1,
            "The offsets file {} contains {} offsets, but the graph has {} nodes",
            self.basename.display(),
            num_offsets,
            num_nodes
        );
        Ok(offsets)
    }
}

impl<E: Endianness, A: Access, D: Dispatch, LM: MmapMode> LoadConf<E, A, D, LM, LM> {
    /// Sets flags for memory-mapping, or for memory obtained from `mmap()`
    /// (both graph and offsets).
    pub fn flags(mut self, flags: MemoryFlags) -> Self {
        self.graph_load_flags = flags;
        self.offsets_load_flags = flags;
        self
    }
}

impl<E: Endianness, A: Access, D: Dispatch, GLM: MmapMode, OLM: LoadMode>
    LoadConf<E, A, D, GLM, OLM>
{
    /// Sets flags for memory-mapping, or for memory obtained from `mmap()`,
    /// for the graph.
    pub fn graph_flags(mut self, flags: MemoryFlags) -> Self {
        self.graph_load_flags = flags;
        self
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode, OLM: LoadMode> LoadConf<E, Random, D, GLM, OLM> {
    /// Chooses the [`LoadMode`] for the offsets only.
    pub fn offsets_mode<NOLM: LoadMode>(self) -> LoadConf<E, Random, D, GLM, NOLM> {
        self.retype()
    }
}

impl<E: Endianness, D: Dispatch, GLM: LoadMode, OLM: MmapMode> LoadConf<E, Random, D, GLM, OLM> {
    /// Sets flags for memory-mapping, or for memory obtained from `mmap()`,
    /// for the offsets.
    pub fn offsets_flags(mut self, flags: MemoryFlags) -> Self {
        self.offsets_load_flags = flags;
        self
    }
}

impl<E: Endianness, GLM: LoadMode, OLM: LoadMode> LoadConf<E, Random, Dynamic, GLM, OLM> {
    /// Loads a random-access graph with dynamic dispatch.
    pub fn load(
        mut self,
    ) -> anyhow::Result<BvGraph<DynCodesDecoderFactory<E, GLM::Factory<E>, OLM::Offsets>>>
    where
        <GLM as LoadMode>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM>: CodesRead<E> + BitSeek,
    {
        warn_if_ef_stale(&self.basename);
        let (num_nodes, num_arcs, comp_flags, factory) = self.load_graph()?;
        let offsets = self.load_offsets(num_nodes)?;
        Ok(BvGraph::new(
            DynCodesDecoderFactory::new(factory, offsets, comp_flags)?,
            num_nodes,
            num_arcs,
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

impl<E: Endianness, GLM: LoadMode, OLM: LoadMode> LoadConf<E, Sequential, Dynamic, GLM, OLM> {
    /// Loads a sequential graph with dynamic dispatch.
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BvGraphSeq<DynCodesDecoderFactory<E, GLM::Factory<E>, Owned<EmptyDict<u64, u64>>>>,
    >
    where
        <GLM as LoadMode>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM>: CodesRead<E>,
    {
        let (num_nodes, num_arcs, comp_flags, factory) = self.load_graph()?;
        Ok(BvGraphSeq::new(
            DynCodesDecoderFactory::new(factory, EmptyDict::default().into(), comp_flags)?,
            num_nodes,
            Some(num_arcs),
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

impl<
    E: Endianness,
    GLM: LoadMode,
    OLM: LoadMode,
    const OUTDEGREES: usize,
    const REFERENCES: usize,
    const BLOCKS: usize,
    const INTERVALS: usize,
    const RESIDUALS: usize,
> LoadConf<E, Random, Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>, GLM, OLM>
{
    /// Loads a random-access graph with static dispatch.
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BvGraph<
            ConstCodesDecoderFactory<
                E,
                GLM::Factory<E>,
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
        <GLM as LoadMode>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM>: CodesRead<E> + BitSeek,
    {
        warn_if_ef_stale(&self.basename);
        let (num_nodes, num_arcs, comp_flags, factory) = self.load_graph()?;
        let offsets = self.load_offsets(num_nodes)?;
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
    GLM: LoadMode,
    OLM: LoadMode,
    const OUTDEGREES: usize,
    const REFERENCES: usize,
    const BLOCKS: usize,
    const INTERVALS: usize,
    const RESIDUALS: usize,
> LoadConf<E, Sequential, Static<OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS>, GLM, OLM>
{
    /// Loads a sequential graph with static dispatch.
    pub fn load(
        mut self,
    ) -> anyhow::Result<
        BvGraphSeq<
            ConstCodesDecoderFactory<
                E,
                GLM::Factory<E>,
                Owned<EmptyDict<u64, u64>>,
                OUTDEGREES,
                REFERENCES,
                BLOCKS,
                INTERVALS,
                RESIDUALS,
            >,
        >,
    >
    where
        <GLM as LoadMode>::Factory<E>: CodesReaderFactoryHelper<E>,
        for<'a> LoadModeCodesReader<'a, E, GLM>: CodesRead<E>,
    {
        let (num_nodes, num_arcs, comp_flags, factory) = self.load_graph()?;
        Ok(BvGraphSeq::new(
            ConstCodesDecoderFactory::new(factory, EmptyDict::default().into(), comp_flags)?,
            num_nodes,
            Some(num_arcs),
            comp_flags.compression_window,
            comp_flags.min_interval_length,
        ))
    }
}

/// Checks if the `.ef` file is older than the .graph file and log a warning if so.
///
/// This is important because if the graph has been recompressed, the `.ef` file
/// will be stale and needs to be rebuilt. This is a very common scenario, in
/// particular when testing compression techniques.
fn warn_if_ef_stale(basename: &Path) {
    if std::env::var_os("DO_NOT_CHECK_MOD_TIMES").is_some() {
        return;
    }
    let graph_path = basename.with_extension(GRAPH_EXTENSION);
    let ef_path = basename.with_extension(EF_EXTENSION);

    let graph_modified = match std::fs::metadata(&graph_path).and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(_) => return, // Can't check, skip warning
    };

    let ef_modified = match std::fs::metadata(&ef_path).and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(_) => return, // Can't check, skip warning
    };

    if ef_modified < graph_modified {
        log::warn!(
            "The Elias–Fano file {} is older than the graph file {}; \
             this may indicate that the graph has been modified and the .ef file is stale. \
             Consider rebuilding it with \"webgraph build ef {}\", just touch it if this warning is spurious, \
             or set the environment variable DO_NOT_CHECK_MOD_TIMES to disable this check.",
            ef_path.display(),
            graph_path.display(),
            basename.display()
        );
    }
}

/// Reads a `.properties` file.
fn read_properties(path: &Path) -> Result<HashMap<String, String>> {
    let f = std::fs::File::open(path)
        .with_context(|| format!("Cannot open property file {}", path.display()))?;
    java_properties::read(BufReader::new(f))
        .with_context(|| format!("Cannot parse {} as a Java properties file", path.display()))
}

/// Parses the mandatory property `key` of the `.properties` file at `path`.
fn parse_property<T: FromStr>(map: &HashMap<String, String>, key: &str, path: &Path) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let name = path.display();
    map.get(key)
        .with_context(|| format!("Missing '{key}' property in {name}"))?
        .parse::<T>()
        .with_context(|| {
            format!(
                "Cannot parse '{key}' as {} in {name}",
                std::any::type_name::<T>()
            )
        })
}

/// Returns the value of the `endianness` property, which defaults to big
/// endian as in the Java version.
pub(crate) fn endianness_property(map: &HashMap<String, String>) -> &str {
    map.get("endianness")
        .map_or(BigEndian::NAME, String::as_str)
}

/// Reads the `.properties` file and returns the endianness.
pub fn get_endianness<P: AsRef<Path>>(basename: P) -> Result<String> {
    let map = read_properties(&basename.as_ref().with_extension(PROPERTIES_EXTENSION))?;
    Ok(endianness_property(&map).to_string())
}

/// Metadata read from a label `.properties` file.
#[derive(Debug, Clone)]
pub struct LabelProperties {
    /// Number of nodes in the graph.
    pub num_nodes: usize,
    /// Number of arcs (labels) in the graph.
    pub num_arcs: u64,
    /// Stable name of the serializer that produced the labels.
    pub serializer: String,
}

/// Reads the label `.properties` file for the given label basename and
/// returns the metadata. Checks that the endianness matches `E`.
pub fn parse_label_properties<E: Endianness>(
    label_basename: impl AsRef<Path>,
) -> Result<LabelProperties> {
    let path = label_basename.as_ref().with_extension(PROPERTIES_EXTENSION);
    let map = read_properties(&path)?;

    let endianness = endianness_property(&map);
    anyhow::ensure!(
        endianness == E::NAME,
        "Label endianness mismatch in {}: found {endianness}, expected {}",
        path.display(),
        E::NAME
    );

    Ok(LabelProperties {
        num_nodes: parse_property(&map, "nodes", &path)?,
        num_arcs: parse_property(&map, "arcs", &path)?,
        serializer: parse_property(&map, "serializer", &path)?,
    })
}

/// Reads the `.properties` file and returns the number of nodes, number of arcs, and compression
/// flags for the graph. The endianness is checked against the expected one.
pub fn parse_properties<E: Endianness>(path: impl AsRef<Path>) -> Result<(usize, u64, CompFlags)> {
    let path = path.as_ref();
    let map = read_properties(path)?;
    let comp_flags = CompFlags::from_properties::<E>(&map)
        .with_context(|| format!("Cannot parse compression flags from {}", path.display()))?;
    Ok((
        parse_property(&map, "nodes", path)?,
        parse_property(&map, "arcs", path)?,
        comp_flags,
    ))
}
