/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::prelude::*;
use anyhow::{Context, Result, ensure};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use rayon::{current_num_threads, in_place_scope};
use std::fs::File;
use std::io::Write;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

/// A queue that pulls jobs with ids in a contiguous initial segment of the
/// natural numbers from an iterator out of order and implement an iterator in
/// which they can be pulled in order.
///
/// Jobs must be ordered by their job id, and must implement [`Eq`] with a
/// [`usize`] using their job id.
struct TaskQueue<I: Iterator> {
    iter: I,
    jobs: Vec<Option<I::Item>>,
    next_id: usize,
}

trait JobId {
    fn id(&self) -> usize;
}

impl<I: Iterator> TaskQueue<I> {
    fn new(iter: I) -> Self {
        Self {
            iter,
            jobs: vec![],
            next_id: 0,
        }
    }
}

impl<I: Iterator> Iterator for TaskQueue<I>
where
    I::Item: JobId,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.jobs.get_mut(self.next_id) {
                if item.is_some() {
                    self.next_id += 1;
                    return item.take();
                }
            }
            let item = self.iter.next()?;
            let id = item.id();
            if id >= self.jobs.len() {
                self.jobs.resize_with(id + 1, || None);
            }
            self.jobs[id] = Some(item);
        }
    }
}

/// Threshold mask for periodic compression statistics logging.
///
/// Statistics are logged every `STATS_THRESHOLD + 1` nodes (approximately 4M).
const STATS_THRESHOLD: usize = (1 << 22) - 1;

/// Logs compression statistics periodically (every ~4M nodes) or
/// unconditionally when `force` is true.
#[inline]
fn log_comp_stats(stats: &super::CompStats, force: bool) {
    if force || (stats.num_nodes & STATS_THRESHOLD == 0 && stats.num_nodes > 0) {
        let bits_per_link = stats.written_bits as f64
            / if stats.num_arcs > 0 {
                stats.num_arcs as f64
            } else {
                1.0
            };
        let n = stats.num_nodes as f64;
        log::info!(
            "bits/link: {:.3}; bits/node: {:.3}; avgref: {:.3}; avgdist: {:.3}",
            bits_per_link,
            stats.written_bits as f64 / n,
            stats.tot_ref as f64 / n,
            stats.tot_dist as f64 / n,
        );
    }
}

/// Writes the `.properties` file for a label bitstream.
fn write_label_properties<E: dsi_bitstream::traits::Endianness>(
    labels_basename: &Path,
    serializer_name: &str,
    stats: &super::CompStats,
) -> Result<()> {
    let (num_nodes, num_arcs, labels_written_bits) =
        (stats.num_nodes, stats.num_arcs, stats.labels_written_bits);
    let mut s = String::new();
    s.push_str("#Label properties\n");
    s.push_str(&format!("endianness={}\n", E::NAME));
    s.push_str(&format!("nodes={num_nodes}\n"));
    s.push_str(&format!("arcs={num_arcs}\n"));
    s.push_str(&format!("serializer={serializer_name}\n"));
    s.push_str(&format!("length={labels_written_bits}\n"));
    if num_arcs > 0 {
        s.push_str(&format!(
            "bitsperlink={:.3}\n",
            labels_written_bits as f64 / num_arcs as f64
        ));
    }
    if num_nodes > 0 {
        s.push_str(&format!(
            "bitspernode={:.3}\n",
            labels_written_bits as f64 / num_nodes as f64
        ));
    }
    let props_path = labels_basename.with_extension(PROPERTIES_EXTENSION);
    std::fs::write(&props_path, &s)
        .with_context(|| format!("Could not write {}", props_path.display()))?;
    Ok(())
}

/// Appends the first `bits` bits of the bitstream in `part_path` to `writer`,
/// which writes to `path`, and then deletes `part_path`.
fn append_part<E: Endianness, W: BitWrite<E>>(
    writer: &mut W,
    path: &Path,
    part_path: &Path,
    bits: u64,
) -> Result<()>
where
    BufBitReader<E, WordAdapter<u32, BufReader<File>>>: BitRead<E>,
{
    log::debug!(
        "Copying {bits} bits from {} to {}",
        part_path.display(),
        path.display()
    );
    let mut reader = buf_bit_reader::from_path::<E, u32>(part_path)
        .with_context(|| format!("Could not open {}", part_path.display()))?;
    writer.copy_from(&mut reader, bits).with_context(|| {
        format!(
            "Could not copy from {} to {}",
            part_path.display(),
            path.display()
        )
    })?;
    std::fs::remove_file(part_path)
        .with_context(|| format!("Could not remove {}", part_path.display()))?;
    Ok(())
}

/// A compression job.
#[derive(Debug, Clone)]
struct Job {
    job_id: usize,
    first_node: usize,
    last_node: usize,
    chunk_graph_path: PathBuf,
    chunk_offsets_path: PathBuf,
    part_labels_path: PathBuf,
    part_label_offsets_path: PathBuf,
    stats: super::CompStats,
}

impl JobId for Job {
    fn id(&self) -> usize {
        self.job_id
    }
}

/// The message sent by a compression worker to the ordered merge: the
/// outcome of one chunk, in job-id order.
///
/// `Ok(None)` reports an empty lender (a legal empty segment), so that the
/// merge can keep pulling later jobs in order; `Err` carries a worker
/// failure, surfaced by the merge in job order.
struct WorkerResult {
    job_id: usize,
    result: Result<Option<Job>>,
}

impl JobId for WorkerResult {
    fn id(&self) -> usize {
        self.job_id
    }
}

/// Writes γ-coded delta offsets to a bitstream.
///
/// Used internally by [`BvComp`] and [`BvCompConf`] to produce the
/// `.offsets` file during compression.
#[derive(Debug)]
#[repr(transparent)]
pub struct OffsetsWriter<W: Write>(BufBitWriter<BigEndian, WordAdapter<usize, BufWriter<W>>>);

impl OffsetsWriter<File> {
    /// Creates a new writer and writes the first offset value (0) if requested.
    ///
    /// Usually, parallel compressor will write autonomously the first offset
    /// when copying the partial offsets files into the final offsets file.
    pub fn from_path(path: impl AsRef<Path>, write_zero: bool) -> Result<Self> {
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.as_ref().display()))?;
        Self::from_write(file, write_zero)
    }
}

impl<W: Write> OffsetsWriter<W> {
    /// Creates a new writer and writes the first offset value (0) if requested.
    pub fn from_write(writer: W, write_zero: bool) -> Result<Self> {
        let mut buffer = BufBitWriter::new(WordAdapter::new(BufWriter::new(writer)));
        if write_zero {
            buffer.write_gamma(0)?;
        }
        Ok(Self(buffer))
    }

    /// Pushes a new delta offset.
    pub fn push(&mut self, delta: u64) -> Result<usize> {
        Ok(self.0.write_gamma(delta)?)
    }

    /// Flushes the buffer.
    pub fn flush(&mut self) -> Result<()> {
        BitWrite::flush(&mut self.0)?;
        Ok(())
    }
}

/// Configures and runs BvGraph compression.
///
/// A `BvCompConf` is normally obtained via the convenience methods
/// [`BvComp::with_basename`] (for the standard compressor) or
/// [`BvCompZ::with_basename`] (for the [Zuckerli-based] compressor). It
/// can then be customized using the builder methods below and finally used
/// to compress a graph.
///
/// # Configuration
///
/// - [`comp_flags`]: sets [`CompFlags`] (compression window, maximum
///   reference count, minimum interval length, and the instantaneous codes
///   used for each component);
/// - [`bvgraphz`]: switches to the [Zuckerli-based]
///   reference-selection algorithm;
/// - [`chunk_size`]: sets the chunk size for [`BvCompZ`] (implies
///   `bvgraphz`);
/// - [`labels_basename`]: overrides the default label-file basename
///   (e.g., to write labels to a different disk);
/// - [`tmp_dir`]: sets the temporary directory for parallel
///   compression;
/// - [`merge_labels`]: sets a [`MergeLabel`] that merges the labels of
///   repeated successors (by default, repeated successors cause an
///   error).
///
/// # Compression Methods
///
/// ## Unlabeled
///
/// - [`comp_graph`]: compresses a [`SequentialGraph`] sequentially;
/// - [`comp_lender`]: compresses a [`NodeLabelsLender`] sequentially;
/// - [`par_comp`]: compresses an [`IntoParLenders`] in parallel.
///
/// ## Labeled
///
/// - [`comp_labeled_graph`]: compresses a [`LabeledSequentialGraph`] and
///   its labels sequentially;
/// - [`comp_labeled_lender`]: compresses a labeled [`NodeLabelsLender`]
///   sequentially;
/// - [`par_comp_labeled`]: compresses a labeled [`IntoParLenders`] in
///   parallel.
///
/// The labeled variants accept a [`StoreLabelsConf`] parameter (e.g.,
/// [`BitStreamStoreLabelsConf`]) that controls how labels are written.
/// The unlabeled methods are thin wrappers that delegate to the labeled
/// ones with `()` as label configuration.
///
/// All methods produce the `.graph`, `.offsets`, and `.properties` files
/// and return the total number of bits written to the graph bitstream.
/// Labeled methods additionally produce label files (by default under the
/// basename `<basename>-labels`; see [`default_labels_basename`]).
///
/// After generating the files, you can use [`store_ef`] or
/// [`store_ef_with_data`] (or the command `webgraph build ef`) to generate
/// the `.ef` file necessary for random access.
///
/// # Examples
///
/// ```ignore
/// // Standard compression with default settings
/// BvComp::with_basename("output").comp_graph::<BE>(&graph)?;
///
/// // Standard compression with custom flags
/// BvComp::with_basename("output")
///     .comp_flags(CompFlags {
///         compression_window: 10,
///         min_interval_length: 2,
///         ..Default::default()
///     })
///     .comp_graph::<BE>(&graph)?;
///
/// // Parallel compression
/// BvComp::with_basename("output").par_comp::<BE, _>(&graph)?;
///
/// // Parallel compression with labels
/// let label_config =
///     BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u32>::new());
/// BvComp::with_basename("output")
///     .par_comp_labeled::<BE, _, _, _>(&labeled_graph, label_config)?;
///
/// // Zuckerli-based compression
/// BvCompZ::with_basename("output").comp_graph::<BE>(&graph)?;
/// ```
///
/// [Zuckerli-based]: BvCompZ
/// [`comp_flags`]: Self::comp_flags
/// [`bvgraphz`]: Self::bvgraphz
/// [`chunk_size`]: Self::chunk_size
/// [`labels_basename`]: Self::labels_basename
/// [`default_labels_basename`]: Self::default_labels_basename
/// [`tmp_dir`]: Self::tmp_dir
/// [`merge_labels`]: Self::merge_labels
/// [`comp_graph`]: Self::comp_graph
/// [`comp_lender`]: Self::comp_lender
/// [`comp_labeled_graph`]: Self::comp_labeled_graph
/// [`comp_labeled_lender`]: Self::comp_labeled_lender
/// [`NodeLabelsLender`]: crate::traits::NodeLabelsLender
/// [`par_comp`]: Self::par_comp
/// [`par_comp_labeled`]: Self::par_comp_labeled
/// [`store_ef`]: crate::graphs::bvgraph::store_ef
/// [`store_ef_with_data`]: crate::graphs::bvgraph::store_ef_with_data
/// [`BitStreamStoreLabelsConf`]: crate::labels::BitStreamStoreLabelsConf
#[derive(Debug)]
pub struct BvCompConf<PL = Option<ProgressLogger>, P = NoMerge> {
    /// The basename of the output files.
    basename: PathBuf,
    /// Compression flags for BvComp/BvCompZ.
    comp_flags: CompFlags,
    /// Selects the Zuckerli-based BVGraph compressor
    bvgraphz: bool,
    /// The chunk size for the Zuckerli-based compressor
    chunk_size: usize,
    /// Custom basename for label files, overriding the default derivation.
    labels_basename: Option<PathBuf>,
    /// Base directory for temporary files; a fresh subdirectory is created
    /// inside it (or inside the system temporary directory if unset) by each
    /// parallel compression.
    tmp_dir: Option<PathBuf>,
    /// Progress logger for compression methods.
    pl: PL,
    /// The merge policy for the labels of repeated successors.
    merge_policy: P,
}

impl BvCompConf {
    /// Creates a new compression configuration with the given basename and
    /// default options.
    ///
    /// Note that the convenience methods [`BvComp::with_basename`] and
    /// [`BvCompZ::with_basename`] can be used to create a configuration with
    /// default options.
    ///
    /// [`BvComp::with_basename`]: crate::graphs::bvgraph::comp::BvComp::with_basename
    /// [`BvCompZ::with_basename`]: crate::graphs::bvgraph::comp::BvCompZ::with_basename
    pub fn new(basename: impl AsRef<Path>) -> Self {
        Self {
            basename: basename.as_ref().into(),
            comp_flags: CompFlags::default(),
            bvgraphz: false,
            chunk_size: 10_000,
            labels_basename: None,
            tmp_dir: None,
            pl: None,
            merge_policy: NoMerge,
        }
    }

    /// Returns the default basename for label files given a graph basename.
    ///
    /// By convention, label files use a basename derived from the graph
    /// basename by appending [`LABELS_BASENAME_SUFFIX`] (e.g., `graph-labels`
    /// for a graph with basename `graph`). The label files then use standard
    /// extensions: `.labels`, `.offsets`, `.properties`, `.ef`.
    pub fn default_labels_basename(basename: impl AsRef<Path>) -> PathBuf {
        let mut name = basename.as_ref().as_os_str().to_owned();
        name.push(LABELS_BASENAME_SUFFIX);
        PathBuf::from(name)
    }
}

impl<PL, P> BvCompConf<PL, P> {
    /// Sets the [`CompFlags`] controlling the compression parameters
    /// (compression window, maximum reference count, minimum interval length,
    /// and the instantaneous codes used for each component of the successor
    /// list).
    pub fn comp_flags(mut self, compression_flags: CompFlags) -> Self {
        self.comp_flags = compression_flags;
        self
    }

    /// Sets the base directory used by [`par_comp`] for partial bitstreams.
    /// Each compression stores them in a fresh subdirectory of the given
    /// directory, which is removed when the compression completes. If not
    /// set, a system temporary directory is used.
    ///
    /// [`par_comp`]: Self::par_comp
    pub fn tmp_dir(mut self, tmp_dir: impl AsRef<Path>) -> Self {
        self.tmp_dir = Some(tmp_dir.as_ref().into());
        self
    }

    /// Switches to the [`BvCompZ`] (Zuckerli-based) reference-selection
    /// algorithm.
    pub fn bvgraphz(mut self) -> Self {
        self.bvgraphz = true;
        self
    }

    /// Sets the chunk size for [`BvCompZ`] and enables the Zuckerli-based
    /// compressor. The chunk size controls how many consecutive nodes are
    /// buffered before running the dynamic-programming reference-selection
    /// algorithm; larger chunks can yield better compression at the cost of
    /// more memory. Implies [`bvgraphz`].
    ///
    /// [`bvgraphz`]: Self::bvgraphz
    pub fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.bvgraphz = true;
        self.chunk_size = chunk_size;
        self
    }

    /// Sets a custom basename for label files, overriding the
    /// [`default_labels_basename`] convention.
    ///
    /// This is useful, for example, to write labels to a different disk.
    ///
    /// [`default_labels_basename`]: Self::default_labels_basename
    pub fn labels_basename(mut self, labels_basename: impl AsRef<Path>) -> Self {
        self.labels_basename = Some(labels_basename.as_ref().into());
        self
    }

    /// Sets the progress logger for the compression methods.
    ///
    /// Only the [`item_name`] and [`expected_updates`] are set by the
    /// compression methods; all other properties (e.g., display options,
    /// log interval) should be configured by the caller.
    ///
    /// [`item_name`]: ProgressLog::item_name
    /// [`expected_updates`]: ProgressLog::expected_updates
    pub fn progress_logger<PL2>(self, pl: PL2) -> BvCompConf<PL2, P> {
        BvCompConf {
            basename: self.basename,
            comp_flags: self.comp_flags,
            bvgraphz: self.bvgraphz,
            chunk_size: self.chunk_size,
            labels_basename: self.labels_basename,
            tmp_dir: self.tmp_dir,
            pl,
            merge_policy: self.merge_policy,
        }
    }

    /// Sets a [`MergeLabel`] merging the labels of repeated successors.
    ///
    /// By default, labeled compression methods reject repeated successors
    /// with an error. After calling this method, each run of equal
    /// successors is instead compressed as a single successor, and its label
    /// is obtained by merging the labels of the run with `merger`. The
    /// [`StoreLabelsConf`] passed to the labeled compression methods must
    /// then store labels of type [`MergeLabel::Merged`], which can be
    /// different from that of the input labels.
    ///
    /// The unlabeled compression methods ignore the merger, and always
    /// reject repeated successors.
    ///
    /// # Examples
    ///
    /// Compresses a graph built from pairs containing repeated arcs, using
    /// as label of each arc the number of its occurrences:
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use dsi_bitstream::prelude::*;
    /// # use webgraph::prelude::*;
    /// # use webgraph::graphs::vec_graph::LabeledVecGraph;
    /// # use webgraph::labels::BitStreamStoreLabelsConf;
    /// # use webgraph::traits::FixedWidth;
    /// # fn main() -> Result<()> {
    /// # if cfg!(miri) { return Ok(()); }
    /// # let tmp = tempfile::TempDir::new()?;
    /// # let basename = tmp.path().join("example");
    /// let pairs = [
    ///     ((0, 1), 'a'),
    ///     ((0, 1), 'b'),
    ///     ((0, 2), 'c'),
    ///     ((1, 2), 'd'),
    ///     ((2, 0), 'e'),
    ///     ((2, 0), 'f'),
    ///     ((2, 0), 'g'),
    /// ];
    /// // Labels are sorted along with the pairs, so they must be integers
    /// let graph = ParSortedLabeledGraph::from_pairs(
    ///     3,
    ///     FixedWidth::<u32>::new(),
    ///     pairs.map(|(arc, label)| (arc, label as u32)),
    /// )?;
    ///
    /// BvComp::with_basename(&basename)
    ///     .merge_labels(CountRepeats)
    ///     .par_comp_labeled::<BE, _, _, _>(
    ///         &graph,
    ///         BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u64>::new()),
    ///     )?;
    ///
    /// let seq = BvGraphSeq::with_basename(&basename)
    ///     .endianness::<BE>()
    ///     .load()?;
    /// let labeling = BitStreamLabelingSeq::<BE, _, _>::load(
    ///     BvCompConf::default_labels_basename(&basename),
    ///     FixedWidth::<u64>::new(),
    /// )?;
    /// let expected =
    ///     LabeledVecGraph::from_arcs([((0, 1), 2), ((0, 2), 1), ((1, 2), 1), ((2, 0), 3)]);
    /// graph::eq_labeled(&expected, &Zip(seq, labeling))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn merge_labels<M>(self, merger: M) -> BvCompConf<PL, WithMerge<M>> {
        BvCompConf {
            basename: self.basename,
            comp_flags: self.comp_flags,
            bvgraphz: self.bvgraphz,
            chunk_size: self.chunk_size,
            labels_basename: self.labels_basename,
            tmp_dir: self.tmp_dir,
            pl: self.pl,
            merge_policy: WithMerge(merger),
        }
    }

    /// Returns a copy of this configuration borrowing the progress logger
    /// and rejecting repeated successors, for the unlabeled methods.
    fn no_merge(&mut self) -> BvCompConf<&mut PL, NoMerge> {
        BvCompConf {
            basename: self.basename.clone(),
            comp_flags: self.comp_flags,
            bvgraphz: self.bvgraphz,
            chunk_size: self.chunk_size,
            labels_basename: self.labels_basename.clone(),
            tmp_dir: self.tmp_dir.clone(),
            pl: &mut self.pl,
            merge_policy: NoMerge,
        }
    }

    fn resolve_labels_basename(&self) -> PathBuf {
        self.labels_basename
            .clone()
            .unwrap_or_else(|| BvCompConf::default_labels_basename(&self.basename))
    }

    /// Creates the temporary directory for one parallel compression: a fresh
    /// subdirectory of the configured base (or of the system temporary
    /// directory), so that cleanup cannot touch preexisting content of a
    /// caller-supplied directory.
    fn resolve_tmp_dir(&self) -> Result<tempfile::TempDir> {
        match &self.tmp_dir {
            Some(base) => {
                std::fs::create_dir_all(base)
                    .with_context(|| format!("Could not create {}", base.display()))?;
                tempfile::tempdir_in(base).with_context(|| {
                    format!(
                        "Could not create a temporary directory in {}",
                        base.display()
                    )
                })
            }
            None => tempfile::tempdir().context("Could not create a temporary directory"),
        }
    }
}

/// Writes the `.properties` file of the graph with given basename and, if
/// labels are stored (i.e., if `label_serializer_name` is not `()`), the
/// `.properties` file of the labels.
fn write_properties<E: Endianness>(
    comp_flags: &CompFlags,
    basename: &Path,
    stats: &super::CompStats,
    labels_basename: &Path,
    label_serializer_name: &str,
) -> Result<()> {
    log::info!("Writing the .properties file");
    let properties = comp_flags
        .to_properties::<E>(stats)
        .context("Could not serialize properties")?;
    let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
    std::fs::write(&properties_path, properties)
        .with_context(|| format!("Could not write {}", properties_path.display()))?;
    if label_serializer_name != "()" {
        write_label_properties::<E>(labels_basename, label_serializer_name, stats)?;
    }
    Ok(())
}

impl<PL: ProgressLog, P> BvCompConf<PL, P> {
    /// Compresses sequentially a [`SequentialGraph`] and returns
    /// the number of bits written to the graph bitstream.
    ///
    /// This is a convenience wrapper around [`comp_labeled_graph`] with no
    /// label storage.
    ///
    /// [`comp_labeled_graph`]: Self::comp_labeled_graph
    pub fn comp_graph<E: Endianness>(&mut self, graph: impl SequentialGraph) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        self.no_merge()
            .comp_labeled_graph::<E, (), ()>(UnitLabelGraph(graph), ())
    }

    /// Compresses sequentially a [`NodeLabelsLender`] and returns
    /// the number of bits written to the graph bitstream.
    ///
    /// This is a convenience wrapper around [`comp_labeled_lender`] with no
    /// label storage. The optional `expected_num_nodes` parameter will be
    /// used to provide forecasts on the progress logger.
    ///
    /// [`comp_labeled_lender`]: Self::comp_labeled_lender
    pub fn comp_lender<E, L>(&mut self, iter: L, expected_num_nodes: Option<usize>) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        self.no_merge().comp_labeled_lender::<E, _, _, _>(
            UnitLabelLender(iter.into_lender()),
            (),
            expected_num_nodes,
        )
    }

    /// Compresses sequentially a [`LabeledSequentialGraph`] and returns
    /// the number of bits written to the graph bitstream.
    ///
    /// The `store_labels_config` parameter provides the factory for creating
    /// label storage instances alongside graph compression (e.g.,
    /// [`BitStreamStoreLabelsConf`]). Since labels are written directly to
    /// the final output files, the factory must implement
    /// [`SeqStoreLabelsConf`].
    ///
    /// Successors must be sorted. Repeated successors cause an error, unless
    /// a [`MergeLabel`] has been set with [`merge_labels`], in which case the
    /// labels of each run of equal successors are merged into a single label.
    ///
    /// [`BitStreamStoreLabelsConf`]: crate::labels::BitStreamStoreLabelsConf
    /// [`merge_labels`]: Self::merge_labels
    pub fn comp_labeled_graph<E: Endianness, L, SLC: SeqStoreLabelsConf>(
        &mut self,
        graph: impl LabeledSequentialGraph<L>,
        store_labels_config: SLC,
    ) -> Result<u64>
    where
        P: MergePolicy<L, <SLC::StoreLabels as StoreLabels>::Label>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        let num_nodes = graph.num_nodes();
        self.comp_labeled_lender::<E, _, _, _>(graph.iter(), store_labels_config, Some(num_nodes))
    }

    /// Compresses sequentially a labeled [`NodeLabelsLender`] and returns
    /// the number of bits written to the graph bitstream.
    ///
    /// The `store_labels_config` parameter provides the factory for creating
    /// label storage instances alongside graph compression. Use `()` for
    /// unlabeled graphs. Since labels are written directly to the final
    /// output files, the factory must implement [`SeqStoreLabelsConf`].
    ///
    /// The optional `expected_num_nodes` parameter will be used to provide
    /// forecasts on the progress logger.
    ///
    /// Successors must be sorted. Repeated successors cause an error, unless
    /// a [`MergeLabel`] has been set with [`merge_labels`], in which case the
    /// labels of each run of equal successors are merged into a single label.
    ///
    /// [`merge_labels`]: Self::merge_labels
    pub fn comp_labeled_lender<E, L, SLC, T>(
        &mut self,
        iter: L,
        store_labels_config: SLC,
        expected_num_nodes: Option<usize>,
    ) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        SLC: SeqStoreLabelsConf,
        SLC::StoreLabels: StoreLabels,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = (usize, T)>,
        P: MergePolicy<T, <SLC::StoreLabels as StoreLabels>::Label>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        let graph_path = self.basename.with_extension(GRAPH_EXTENSION);
        let labels_basename = self.resolve_labels_basename();
        let labels_path = labels_basename.with_extension(LABELS_EXTENSION);
        let label_offsets_path = labels_basename.with_extension(OFFSETS_EXTENSION);

        // Compress the graph
        let bit_write = buf_bit_writer::from_path::<E, usize>(&graph_path)
            .with_context(|| format!("Could not create {}", graph_path.display()))?;

        let codes_writer = DynCodesEncoder::new(bit_write, &self.comp_flags)?;

        // create a file for offsets
        let offsets_path = self.basename.with_extension(OFFSETS_EXTENSION);
        let offset_writer = OffsetsWriter::from_path(offsets_path, true)?;

        let mut store_labels =
            store_labels_config.new_storage(&labels_path, &label_offsets_path)?;
        store_labels.init()?;

        self.pl
            .item_name("node")
            .expected_updates(expected_num_nodes);
        self.pl.start("Compressing successors...");
        let comp_stats = if self.bvgraphz {
            let mut bvcompz = BvCompZ::new(
                codes_writer,
                offset_writer,
                self.comp_flags.compression_window,
                self.chunk_size,
                self.comp_flags.max_ref_count,
                self.comp_flags.min_interval_length,
                0,
                store_labels,
                &self.merge_policy,
            );

            for_! ( (_node_id, successors) in iter {
                bvcompz.push(successors).context("Could not push successors")?;
                log_comp_stats(&bvcompz.stats(), false);
                self.pl.update();
            });
            log_comp_stats(&bvcompz.stats(), true);
            self.pl.done();

            bvcompz.flush()?
        } else {
            let mut bvcomp = BvComp::new(
                codes_writer,
                offset_writer,
                self.comp_flags.compression_window,
                self.comp_flags.max_ref_count,
                self.comp_flags.min_interval_length,
                0,
                store_labels,
                &self.merge_policy,
            );

            for_! ( (_node_id, successors) in iter {
                bvcomp.push(successors).context("Could not push successors")?;
                log_comp_stats(&bvcomp.stats(), false);
                self.pl.update();
            });
            log_comp_stats(&bvcomp.stats(), true);
            self.pl.done();

            bvcomp.flush()?
        };

        if let Some(num_nodes) = expected_num_nodes {
            if num_nodes != comp_stats.num_nodes {
                log::warn!(
                    "The expected number of nodes is {num_nodes} but the actual number of nodes is {}",
                    comp_stats.num_nodes,
                );
            }
        }

        write_properties::<E>(
            &self.comp_flags,
            &self.basename,
            &comp_stats,
            &labels_basename,
            &store_labels_config.label_serializer_name(),
        )?;

        Ok(comp_stats.written_bits)
    }

    /// Compresses an [`IntoParLenders`] in parallel and returns the length
    /// in bits of the graph bitstream.
    ///
    /// This is a convenience wrapper around [`par_comp_labeled`] with no
    /// label storage. See [`par_comp_labeled`] for details on the parallel
    /// compression strategy.
    ///
    /// [`par_comp_labeled`]: Self::par_comp_labeled
    pub fn par_comp<E: Endianness, G>(&mut self, graph: G) -> Result<u64>
    where
        G: for<'a> IntoParLenders<ParLender: NodeLabelsLender<'a, Label = usize>>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        self.no_merge()
            .par_comp_labeled::<E, _, _, _>(UnitLabelParLenders(graph), ())
    }

    /// Compresses a labeled [`IntoParLenders`] in parallel and returns the
    /// length in bits of the graph bitstream.
    ///
    /// The method calls [`into_par_lenders`] to obtain lenders and
    /// boundaries, then compresses each lender in a separate thread. Each
    /// worker thread also writes per-chunk label and label-offset files via
    /// a [`StoreLabels`] instance obtained from `store_labels_config` (use
    /// `()` for unlabeled graphs). After all threads finish, the main thread
    /// concatenates the chunk files using
    /// [`StoreLabelsConf::concat_part`].
    ///
    /// The number of parallel compression threads will be
    /// [`current_num_threads`]. It is your responsibility to ensure that the
    /// number of threads is appropriate for the number of lenders returned
    /// by [`into_par_lenders`], possibly using [`install`].
    ///
    /// A concrete implementation of [`StoreLabelsConf`] for
    /// bitstream-based labels is [`BitStreamStoreLabelsConf`].
    ///
    /// Successors must be sorted. Repeated successors cause an error, unless
    /// a [`MergeLabel`] has been set with [`merge_labels`], in which case the
    /// labels of each run of equal successors are merged into a single label.
    ///
    /// # Examples
    ///
    /// Compresses a labeled graph in parallel, then loads and verifies the
    /// result using [`BitStreamLabelingSeq::load`]:
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use dsi_bitstream::prelude::*;
    /// # use webgraph::prelude::*;
    /// # use webgraph::graphs::bvgraph::*;
    /// # use webgraph::labels::BitStreamLabelingSeq;
    /// # use webgraph::graphs::vec_graph::LabeledVecGraph;
    /// # use webgraph::labels::BitStreamStoreLabelsConf;
    /// # use webgraph::traits::FixedWidth;
    /// # fn main() -> Result<()> {
    /// # if cfg!(miri) { return Ok(()); }
    /// # let tmp = tempfile::TempDir::new()?;
    /// # let basename = tmp.path().join("example");
    /// let graph = LabeledVecGraph::from_arcs([
    ///     ((0, 1), 10u32),
    ///     ((0, 2), 20),
    ///     ((1, 3), 30),
    ///     ((2, 3), 40),
    ///     ((3, 0), 50),
    /// ]);
    ///
    /// let label_config =
    ///     BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u32>::new());
    ///
    /// BvComp::with_basename(&basename)
    ///     .par_comp_labeled::<BE, _, _, _>(&graph, label_config)?;
    ///
    /// let labels_basename = BvCompConf::default_labels_basename(&basename);
    ///
    /// // Load the compressed graph and labeling, then verify
    /// let seq = BvGraphSeq::with_basename(&basename)
    ///     .endianness::<BE>()
    ///     .load()?;
    /// let labeling = BitStreamLabelingSeq::<BE, _, _>::load(
    ///     &labels_basename,
    ///     FixedWidth::<u32>::new(),
    /// )?;
    ///
    /// graph::eq_labeled(&graph, &Zip(seq, labeling))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`into_par_lenders`]: IntoParLenders::into_par_lenders
    /// [`install`]: rayon::ThreadPool::install
    /// [`BitStreamStoreLabelsConf`]: crate::labels::BitStreamStoreLabelsConf
    /// [`par_comp`]: Self::par_comp
    /// [`BitStreamLabelingSeq::load`]: crate::labels::BitStreamLabelingSeq::load
    /// [`merge_labels`]: Self::merge_labels
    pub fn par_comp_labeled<E: Endianness, G, SLC, T>(
        &mut self,
        graph: G,
        mut store_labels_config: SLC,
    ) -> Result<u64>
    where
        G: for<'a> IntoParLenders<ParLender: NodeLabelsLender<'a, Label = (usize, T)>>,
        SLC: StoreLabelsConf + Send + Sync,
        SLC::StoreLabels: Send,
        P: MergePolicy<T, <SLC::StoreLabels as StoreLabels>::Label> + Sync,
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        let (lenders, boundaries) = graph.into_par_lenders();
        let num_nodes = *boundaries.last().unwrap_or(&0);
        let tmp_dir_guard = self.resolve_tmp_dir()?;
        let tmp_dir = tmp_dir_guard.path().to_owned();

        let graph_path = self.basename.with_extension(GRAPH_EXTENSION);
        let offsets_path = self.basename.with_extension(OFFSETS_EXTENSION);
        let label_base = self.resolve_labels_basename();
        let labels_path = label_base.with_extension(LABELS_EXTENSION);
        let label_offsets_path = label_base.with_extension(OFFSETS_EXTENSION);

        let (tx, rx) = crossbeam_channel::unbounded();

        let thread_path = |thread_id: usize| tmp_dir.join(format!("{thread_id:016x}.bitstream"));

        let mut comp_pl = self.pl.concurrent();
        comp_pl
            .item_name("node")
            .expected_updates(num_nodes)
            .local_speed(true);
        comp_pl.start(format!(
            "Compressing successors in parallel using {} threads...",
            current_num_threads()
        ));
        let cp_flags = &self.comp_flags;
        let merge_policy = &self.merge_policy;
        let bvgraphz = self.bvgraphz;
        let chunk_size = self.chunk_size;

        in_place_scope(|s| {
            for (thread_id, mut thread_lender) in Vec::from(lenders).into_iter().enumerate() {
                let tmp_path = thread_path(thread_id);
                let chunk_graph_path = tmp_path.with_extension(GRAPH_EXTENSION);
                let chunk_offsets_path = tmp_path.with_extension(OFFSETS_EXTENSION);
                let label_tmp_stem = tmp_dir.join(format!("{thread_id:016x}-labels"));
                let part_labels_path = label_tmp_stem.with_extension(LABELS_EXTENSION);
                let part_label_offsets_path = label_tmp_stem.with_extension(OFFSETS_EXTENSION);
                let store_labels =
                    store_labels_config.new_storage(&part_labels_path, &part_label_offsets_path)?;
                let tx = tx.clone();
                let mut comp_pl = comp_pl.clone();
                s.spawn(move |_| {
                    log::debug!("Thread {thread_id} started");

                    let result = (|| -> Result<Option<Job>> {
                        // An empty lender is a legal empty segment.
                        let Some((node_id, successors)) = thread_lender.next() else {
                            return Ok(None);
                        };

                        let first_node = node_id;
                        let writer = buf_bit_writer::from_path::<E, usize>(&chunk_graph_path)
                            .with_context(|| {
                                format!("Could not create {}", chunk_graph_path.display())
                            })?;
                        let codes_encoder = <DynCodesEncoder<E, _>>::new(writer, cp_flags)?;

                        let mut last_node;
                        let stats = if bvgraphz {
                            let mut bvcomp = BvCompZ::new(
                                codes_encoder,
                                OffsetsWriter::from_path(&chunk_offsets_path, false)?,
                                cp_flags.compression_window,
                                chunk_size,
                                cp_flags.max_ref_count,
                                cp_flags.min_interval_length,
                                node_id,
                                store_labels,
                                merge_policy,
                            );
                            bvcomp.push(successors)?;
                            last_node = first_node;
                            let mut iter_nodes =
                                thread_lender.inspect(|(x, _)| last_node = *x);
                            while let Some((_, succ)) = iter_nodes.next() {
                                bvcomp.push(succ)?;
                                log_comp_stats(&bvcomp.stats(), false);
                                comp_pl.update();
                            }
                            bvcomp.flush()?
                        } else {
                            let mut bvcomp = BvComp::new(
                                codes_encoder,
                                OffsetsWriter::from_path(&chunk_offsets_path, false)?,
                                cp_flags.compression_window,
                                cp_flags.max_ref_count,
                                cp_flags.min_interval_length,
                                node_id,
                                store_labels,
                                merge_policy,
                            );
                            bvcomp.push(successors)?;
                            last_node = first_node;
                            let mut iter_nodes =
                                thread_lender.inspect(|(x, _)| last_node = *x);
                            while let Some((_, succ)) = iter_nodes.next() {
                                bvcomp.push(succ)?;
                                log_comp_stats(&bvcomp.stats(), false);
                                comp_pl.update();
                            }
                            bvcomp.flush()?
                        };

                        log::debug!(
                            "Finished Compression thread {thread_id} and wrote {} bits for the graph and {} bits for the offsets",
                            stats.written_bits, stats.offsets_written_bits,
                        );
                        Ok(Some(Job {
                            job_id: thread_id,
                            first_node,
                            last_node,
                            chunk_graph_path,
                            chunk_offsets_path,
                            part_labels_path,
                            part_label_offsets_path,
                            stats,
                        }))
                    })();

                    tx.send(WorkerResult {
                        job_id: thread_id,
                        result,
                    })
                    .ok(); // If channel is closed, main thread already has an error
                });
            }

            drop(tx);

            self.pl.item_name("node").expected_updates(num_nodes);
            self.pl
                .start("Copying compressed successors to final graph");

            let mut graph_writer = buf_bit_writer::from_path::<E, usize>(&graph_path)
                .with_context(|| format!("Could not create graph {}", graph_path.display()))?;

            let mut offsets_writer = buf_bit_writer::from_path::<BE, usize>(&offsets_path)
                .with_context(|| format!("Could not create offsets {}", offsets_path.display()))?;
            offsets_writer.write_gamma(0)?;

            store_labels_config.init_concat(&labels_path, &label_offsets_path)?;

            let mut total_stats = super::CompStats::default();

            let mut next_node = 0;
            // glue together the bitstreams as they finish, this allows us to do
            // task pipelining for better performance
            for worker_result in TaskQueue::new(rx.into_rayon_iter()) {
                let job = worker_result.result.with_context(|| {
                    format!("Compression thread {} failed", worker_result.job_id)
                })?;
                // An empty lender contributes nothing.
                let Some(job) = job else {
                    continue;
                };
                ensure!(
                    job.first_node == next_node,
                    "Non-adjacent lenders: lender {} has first node {} instead of {}",
                    job.job_id,
                    job.first_node,
                    next_node
                );

                next_node = job.last_node + 1;
                let stats = job.stats;
                append_part(
                    &mut graph_writer,
                    &graph_path,
                    &job.chunk_graph_path,
                    stats.written_bits,
                )?;
                append_part::<BE, _>(
                    &mut offsets_writer,
                    &offsets_path,
                    &job.chunk_offsets_path,
                    stats.offsets_written_bits,
                )?;
                store_labels_config.concat_part(
                    &job.part_labels_path,
                    stats.labels_written_bits,
                    &job.part_label_offsets_path,
                    stats.label_offsets_written_bits,
                )?;

                let chunk_num_nodes = job.last_node - job.first_node + 1;
                total_stats += super::CompStats {
                    num_nodes: chunk_num_nodes,
                    ..stats
                };
                self.pl.update_with_count(chunk_num_nodes);
            }

            // A worker that fails before sending (or a lender skipping part
            // of its segment) must not silently truncate the output graph.
            ensure!(
                next_node == num_nodes,
                "The lenders covered only nodes [0..{next_node}), but the graph has {num_nodes} nodes"
            );

            store_labels_config.flush_concat()?;

            log::info!("Flushing the merged bitstreams");
            graph_writer.flush()?;
            BitWrite::flush(&mut offsets_writer)?;

            // Use the authoritative num_nodes from the boundaries
            total_stats.num_nodes = num_nodes;
            log_comp_stats(&total_stats, true);
            comp_pl.done();
            self.pl.done();

            write_properties::<E>(
                cp_flags,
                &self.basename,
                &total_stats,
                &label_base,
                &store_labels_config.label_serializer_name(),
            )?;

            log::info!(
                "Compressed {} arcs into {} bits at {:.3} bits/arc",
                total_stats.num_arcs,
                total_stats.written_bits,
                total_stats.written_bits as f64 / total_stats.num_arcs as f64
            );
            log::info!(
                "Created offsets file with {} bits at {:.3} bits/node",
                total_stats.offsets_written_bits,
                total_stats.offsets_written_bits as f64 / num_nodes as f64
            );

            // cleanup the temp files
            std::fs::remove_dir_all(&tmp_dir).with_context(|| {
                format!("Could not clean temporary directory {}", tmp_dir.display())
            })?;
            Ok(total_stats.written_bits)
        })
    }
}
