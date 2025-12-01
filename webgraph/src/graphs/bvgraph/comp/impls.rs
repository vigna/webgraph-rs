/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{ensure, Context, Result};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
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
            if let Some(item) = self.iter.next() {
                let id = item.id();
                if id >= self.jobs.len() {
                    self.jobs.resize_with(id + 1, || None);
                }
                self.jobs[id] = Some(item);
            } else {
                return None;
            }
        }
    }
}

/// A compression job.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
struct Job {
    job_id: usize,
    first_node: usize,
    last_node: usize,
    chunk_graph_path: PathBuf,
    written_bits: u64,
    chunk_offsets_path: PathBuf,
    offsets_written_bits: u64,
    num_arcs: u64,
}

impl JobId for Job {
    fn id(&self) -> usize {
        self.job_id
    }
}

/// A writer for offsets.
///
/// TODO: This currently uses Write which requires std. To support no_std we will want to make W a WordWriter
#[derive(Debug)]
#[repr(transparent)]
pub struct OffsetsWriter<W: Write> {
    buffer: BufBitWriter<BigEndian, WordAdapter<usize, BufWriter<W>>>,
}

impl OffsetsWriter<File> {
    /// Creates a new writer and writes the first offset value (0).
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.as_ref().display()))?;
        Self::from_write(file)
    }
}

impl<W: Write> OffsetsWriter<W> {
    /// Creates a new writer and writes the first offset value (0).
    pub fn from_write(writer: W) -> Result<Self> {
        let mut buffer = BufBitWriter::new(WordAdapter::new(BufWriter::new(writer)));
        // the first offset is always zero
        buffer.write_gamma(0)?;
        Ok(Self { buffer })
    }

    /// Pushes a new delta offset.
    pub fn push(&mut self, delta: u64) -> Result<usize> {
        Ok(self.buffer.write_gamma(delta)?)
    }

    /// Flushes the buffer.
    pub fn flush(&mut self) -> Result<()> {
        BitWrite::flush(&mut self.buffer)? as u64;
        Ok(())
    }
}

/// Like [`std::borrow::Cow`] but does not require `T: ToOwned`
#[derive(Debug)]
enum MaybeOwned<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

#[derive(Debug)]
pub struct BvCompBuilder<'t> {
    basename: PathBuf,
    compression_flags: CompFlags,
    threads: Option<MaybeOwned<'t, ThreadPool>>,
    /// Selects the Zuckerli-based BVGraph compressor
    bvgraphz: bool,
    /// The chunk size for the Zuckerli-based compressor
    chunk_size: usize,
    tmp_dir: Option<PathBuf>,
    /// owns the TempDir that [`Self::tmp_dir`] refers to, if it was created by default
    owned_tmp_dir: Option<tempfile::TempDir>,
}

impl BvCompBuilder<'static> {
    pub fn new(basename: impl AsRef<Path>) -> Self {
        Self {
            basename: basename.as_ref().into(),
            compression_flags: CompFlags::default(),
            threads: None,
            bvgraphz: false,
            chunk_size: 1000,
            tmp_dir: None,
            owned_tmp_dir: None,
        }
    }
}

impl<'t> BvCompBuilder<'t> {
    pub fn with_compression_flags(mut self, compression_flags: CompFlags) -> Self {
        self.compression_flags = compression_flags;
        self
    }

    pub fn with_tmp_dir(mut self, tmp_dir: impl AsRef<Path>) -> Self {
        self.tmp_dir = Some(tmp_dir.as_ref().into());
        self
    }

    pub fn with_threads(self, threads: &'_ ThreadPool) -> BvCompBuilder<'_> {
        BvCompBuilder {
            threads: Some(MaybeOwned::Borrowed(threads)),
            ..self
        }
    }

    pub fn with_zuckerli(self) -> Self {
        BvCompBuilder {
            bvgraphz: true,
            ..self
        }
    }

    pub fn with_chunk_size(self, chunk_size: usize) -> Self {
        BvCompBuilder {
            bvgraphz: true,
            chunk_size,
            ..self
        }
    }

    fn tmp_dir(&mut self) -> Result<PathBuf> {
        if self.tmp_dir.is_none() {
            let tmp_dir = tempfile::tempdir()?;
            self.tmp_dir = Some(tmp_dir.path().to_owned());
            self.owned_tmp_dir = Some(tmp_dir);
        }

        let tmp_dir = self.tmp_dir.clone().unwrap();
        if !std::fs::exists(&tmp_dir)
            .with_context(|| format!("Could not check whether {} exists", tmp_dir.display()))?
        {
            std::fs::create_dir_all(&tmp_dir)
                .with_context(|| format!("Could not create {}", tmp_dir.display()))?;
        }
        Ok(tmp_dir)
    }

    fn ensure_threads(&mut self) -> Result<()> {
        if self.threads.is_none() {
            self.threads = Some(MaybeOwned::Owned(
                ThreadPoolBuilder::default()
                    .build()
                    .context("Could not build default thread pool")?,
            ));
        }

        Ok(())
    }

    fn threads(&self) -> &ThreadPool {
        match self.threads.as_ref().unwrap() {
            MaybeOwned::Owned(threads) => threads,
            MaybeOwned::Borrowed(threads) => threads,
        }
    }

    /// Compresses s [`NodeLabelsLender`] and returns the length in bits of the
    /// graph bitstream.
    pub fn single_thread<E, L>(&mut self, iter: L, num_nodes: Option<usize>) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        let graph_path = self.basename.with_extension(GRAPH_EXTENSION);

        // Compress the graph
        let bit_write = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
            File::create(&graph_path)
                .with_context(|| format!("Could not create {}", graph_path.display()))?,
        )));

        let comp_flags = CompFlags {
            ..Default::default()
        };

        let codes_writer = DynCodesEncoder::new(bit_write, &comp_flags)?;

        // create a file for offsets
        let offsets_path = self.basename.with_extension(OFFSETS_EXTENSION);
        let offset_writer = OffsetsWriter::from_path(offsets_path)?;

        let mut pl = progress_logger![
            display_memory = true,
            item_name = "node",
            expected_updates = num_nodes,
        ];
        pl.start("Compressing successors...");
        let comp_stats = if self.bvgraphz {
            let mut bvcompz = BvCompZ::new(
                codes_writer,
                offset_writer,
                self.compression_flags.compression_window,
                self.chunk_size,
                self.compression_flags.max_ref_count,
                self.compression_flags.min_interval_length,
                0,
            );

            for_! ( (_node_id, successors) in iter {
                bvcompz.push(successors).context("Could not push successors")?;
                pl.update();
            });
            pl.done();

            bvcompz.flush()?
        } else {
            let mut bvcomp = BvComp::new(
                codes_writer,
                offset_writer,
                self.compression_flags.compression_window,
                self.compression_flags.max_ref_count,
                self.compression_flags.min_interval_length,
                0,
            );

            for_! ( (_node_id, successors) in iter {
                bvcomp.push(successors).context("Could not push successors")?;
                pl.update();
            });
            pl.done();

            bvcomp.flush()?
        };

        if let Some(num_nodes) = num_nodes {
            if num_nodes != comp_stats.num_nodes {
                log::warn!(
                    "The expected number of nodes is {num_nodes} but the actual number of nodes is {}",
                    comp_stats.num_nodes,
                );
            }
        }

        log::info!("Writing the .properties file");
        let properties = self
            .compression_flags
            .to_properties::<E>(
                comp_stats.num_nodes,
                comp_stats.num_arcs,
                comp_stats.written_bits,
            )
            .context("Could not serialize properties")?;
        let properties_path = self.basename.with_extension(PROPERTIES_EXTENSION);
        std::fs::write(&properties_path, properties)
            .with_context(|| format!("Could not write {}", properties_path.display()))?;

        Ok(comp_stats.written_bits)
    }

    /// A wrapper over [`parallel_graph`](Self::parallel_graph) that takes the
    /// endianness as a string.
    ///
    /// Endianness can only be [`BE::NAME`](BE) or [`LE::NAME`](LE).
    ///
    ///  A given endianness is enabled only if the corresponding feature is
    /// enabled, `be_bins` for big endian and `le_bins` for little endian, or if
    /// neither features are enabled.
    pub fn parallel_endianness<G: SplitLabeling + SequentialGraph>(
        &mut self,
        graph: &G,
        num_nodes: usize,
        endianness: &str,
    ) -> Result<u64>
    where
        for<'a> <G as SplitLabeling>::SplitLender<'a>: ExactSizeLender + Send + Sync,
    {
        self.ensure_threads()?;
        let num_threads = self.threads().current_num_threads();

        match endianness {
            #[cfg(any(
                feature = "be_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            BE::NAME => {
                // compress the transposed graph
                self.parallel_iter::<BigEndian, _>(graph.split_iter(num_threads), num_nodes)
            }
            #[cfg(any(
                feature = "le_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            LE::NAME => {
                // compress the transposed graph
                self.parallel_iter::<LittleEndian, _>(graph.split_iter(num_threads), num_nodes)
            }
            x => anyhow::bail!("Unknown endianness {}", x),
        }
    }

    /// Compresses a graph in parallel and returns the length in bits of the graph bitstream.
    pub fn parallel_graph<E: Endianness>(
        &mut self,
        graph: &(impl SequentialGraph + for<'a> SplitLabeling<SplitLender<'a>: ExactSizeLender>),
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        self.ensure_threads()?;
        let num_threads = self.threads().current_num_threads();
        self.parallel_iter(graph.split_iter(num_threads), graph.num_nodes())
    }

    /// Compresses multiple [`NodeLabelsLender`] in parallel and returns the length in bits
    /// of the graph bitstream.
    pub fn parallel_iter<
        E: Endianness,
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize> + ExactSizeLender + Send,
    >(
        &mut self,
        iter: impl IntoIterator<Item = L>,
        num_nodes: usize,
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        self.ensure_threads()?;
        let tmp_dir = self.tmp_dir()?;
        let threads = self.threads();

        let graph_path = self.basename.with_extension(GRAPH_EXTENSION);
        let offsets_path = self.basename.with_extension(OFFSETS_EXTENSION);

        let (tx, rx) = std::sync::mpsc::channel();

        let thread_path = |thread_id: usize| tmp_dir.join(format!("{thread_id:016x}.bitstream"));

        let mut comp_pl = concurrent_progress_logger![
            log_target = "webgraph::graphs::bvgraph::comp::impls::parallel_iter::comp",
            display_memory = true,
            item_name = "node",
            local_speed = true,
            expected_updates = Some(num_nodes),
        ];
        comp_pl.start("Compressing successors in parallel...");
        let mut expected_first_node = 0;
        let cp_flags = &self.compression_flags;
        let bvgraphz = self.bvgraphz;
        let chunk_size = self.chunk_size;

        threads.in_place_scope(|s| {
            for (thread_id, mut thread_lender) in iter.into_iter().enumerate() {
                let tmp_path = thread_path(thread_id);
                let chunk_graph_path = tmp_path.with_extension(GRAPH_EXTENSION);
                let chunk_offsets_path = tmp_path.with_extension(OFFSETS_EXTENSION);
                let tx = tx.clone();
                let mut comp_pl = comp_pl.clone();
                let lender_len = thread_lender.len();
                // Spawn the thread
                s.spawn(move |_| {
                    log::debug!("Thread {thread_id} started");

                    let Some((node_id, successors)) = thread_lender.next() else {
                        return;
                    };

                    let first_node = node_id;
                    if first_node != expected_first_node {
                        panic!(
                            "Lender {} expected to start from node {} but started from {}",
                            thread_id,
                            expected_first_node,
                            first_node
                        );
                    }

                    let writer = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(
                        BufWriter::new(File::create(&chunk_graph_path).unwrap()),
                    ));
                    let codes_encoder = <DynCodesEncoder<E, _>>::new(writer, cp_flags).unwrap();

                    let stats;
                    let mut last_node;
                    if bvgraphz {
                        let mut bvcomp = BvCompZ::new(
                            codes_encoder,
                            OffsetsWriter::from_path(&chunk_offsets_path).unwrap(),
                            cp_flags.compression_window,
                            chunk_size,
                            cp_flags.max_ref_count,
                            cp_flags.min_interval_length,
                            node_id,
                        );
                        bvcomp.push(successors).unwrap();
                        last_node = first_node;
                        let iter_nodes = thread_lender.inspect(|(x, _)| last_node = *x);
                        for_! ( (_, succ) in iter_nodes {
                            bvcomp.push(succ.into_iter()).unwrap();
                        });
                        stats = bvcomp.flush().unwrap();
                    } else {
                        let mut bvcomp = BvComp::new(
                            codes_encoder,
                            OffsetsWriter::from_path(&chunk_offsets_path).unwrap(),
                            cp_flags.compression_window,
                            cp_flags.max_ref_count,
                            cp_flags.min_interval_length,
                            node_id,
                        );
                        bvcomp.push(successors).unwrap();
                        last_node = first_node;
                        let iter_nodes = thread_lender.inspect(|(x, _)| last_node = *x);
                        for_! ( (_, succ) in iter_nodes {
                            bvcomp.push(succ.into_iter()).unwrap();
                        });
                        stats = bvcomp.flush().unwrap();
                    }

                    comp_pl.update_with_count(last_node - first_node + 1);

                    log::debug!(
                        "Finished Compression thread {thread_id} and wrote {} bits for the graph and {} bits for the offsets",
                        stats.written_bits, stats.offsets_written_bits,
                    );
                    tx.send(Job {
                        job_id: thread_id,
                        first_node,
                        last_node,
                        chunk_graph_path,
                        written_bits: stats.written_bits,
                        chunk_offsets_path,
                        offsets_written_bits: stats.offsets_written_bits,
                        num_arcs: stats.num_arcs,
                    })
                    .unwrap()
                });

                expected_first_node += lender_len;
            }

            if num_nodes != expected_first_node {
                panic!(
                    "The lenders were supposed to return {} nodes but returned {} instead",
                    num_nodes,
                    expected_first_node
                );
            }

            drop(tx);

            let mut copy_pl = progress_logger![
                log_target = "webgraph::graphs::bvgraph::comp::impls::parallel_iter::copy",
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(num_nodes),
            ];
            copy_pl.start("Copying compressed successors to final graph");

            let file = File::create(&graph_path)
                .with_context(|| format!("Could not create graph {}", graph_path.display()))?;
            let mut graph_writer =
                <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(file)));

            let file = File::create(&offsets_path)
                .with_context(|| format!("Could not create offsets {}", offsets_path.display()))?;
            let mut offsets_writer =
                <BufBitWriter<BigEndian, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(file)));
            offsets_writer.write_gamma(0)?;

            let mut total_written_bits: u64 = 0;
            let mut total_offsets_written_bits: u64 = 0;
            let mut total_arcs: u64 = 0;

            let mut next_node = 0;
            // glue together the bitstreams as they finish, this allows us to do
            // task pipelining for better performance
            for Job {
                job_id,
                first_node,
                last_node,
                chunk_graph_path,
                written_bits,
                chunk_offsets_path,
                offsets_written_bits,
                num_arcs,
            } in TaskQueue::new(rx.iter())
            {
                ensure!(
                    first_node == next_node,
                    "Non-adjacent lenders: lender {} has first node {} instead of {}",
                    job_id,
                    first_node,
                    next_node
                );

                next_node = last_node + 1;
                total_arcs += num_arcs;
                log::debug!(
                    "Copying {} [{}..{}) bits from {} to {}",
                    written_bits,
                    total_written_bits,
                    total_written_bits + written_bits,
                    chunk_graph_path.display(),
                    graph_path.display()
                );
                total_written_bits += written_bits;

                let mut reader =
                    <BufBitReader<E, _>>::new(<WordAdapter<u32, _>>::new(BufReader::new(
                        File::open(&chunk_graph_path)
                            .with_context(|| format!("Could not open {}", chunk_graph_path.display()))?,
                    )));
                graph_writer
                    .copy_from(&mut reader, written_bits)
                    .with_context(|| {
                        format!(
                            "Could not copy from {} to {}",
                            chunk_graph_path.display(),
                            graph_path.display()
                        )
                    })?;

                log::debug!(
                    "Copying offsets {} [{}..{}) bits from {} to {}",
                    offsets_written_bits,
                    total_offsets_written_bits,
                    total_offsets_written_bits + offsets_written_bits,
                    chunk_offsets_path.display(),
                    offsets_path.display()
                );
                total_offsets_written_bits += offsets_written_bits;

                let mut reader =
                    <BufBitReader<BigEndian, _>>::new(<WordAdapter<u32, _>>::new(BufReader::new(
                        File::open(&chunk_offsets_path)
                            .with_context(|| format!("Could not open {}", chunk_offsets_path.display()))?,
                    )));
                offsets_writer
                    .copy_from(&mut reader, offsets_written_bits)
                    .with_context(|| {
                        format!(
                            "Could not copy from {} to {}",
                            chunk_offsets_path.display(),
                            offsets_path.display()
                        )
                    })?;

                copy_pl.update_with_count(last_node - first_node + 1);
            }


            log::info!("Flushing the merged bitstreams");
            graph_writer.flush()?;
            BitWrite::flush(&mut offsets_writer)?;

            comp_pl.done();
            copy_pl.done();

            log::info!("Writing the .properties file");
            let properties = self.compression_flags
                .to_properties::<E>(num_nodes, total_arcs, total_written_bits)
                .context("Could not serialize properties")?;
            let properties_path = self.basename.with_extension(PROPERTIES_EXTENSION);
            std::fs::write(&properties_path, properties).with_context(|| {
                format!(
                    "Could not write properties to {}",
                    properties_path.display()
                )
            })?;

            log::info!(
                "Compressed {} arcs into {} bits for {:.4} bits/arc",
                total_arcs,
                total_written_bits,
                total_written_bits as f64 / total_arcs as f64
            );
            log::info!(
                "Created offsets file with {} bits for {:.4} bits/node",
                total_offsets_written_bits,
                total_offsets_written_bits as f64 / num_nodes as f64
            );

            // cleanup the temp files
            std::fs::remove_dir_all(&tmp_dir).with_context(|| {
                format!("Could not clean temporary directory {}", tmp_dir.display())
            })?;
            Ok(total_written_bits)
        })
    }
}

/*
// TODO: duplicated from BVComp how can we generalize that for any available compressor.
//       Actually I am not able to pass a type (like BVComp or BVCompZ) without
//       specialization on the type parameter ahead of time.
impl BvCompZ<()> {
    pub fn single_thread_endianness<L>(
        basename: impl AsRef<Path>,
        iter: L,
        chunk_size: usize,
        compression_flags: CompFlags,
        num_nodes: Option<usize>,
        endianness: &str,
    ) -> Result<u64>
    where
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        match endianness {
            #[cfg(any(
                feature = "be_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            BE::NAME => Self::single_thread::<BigEndian, _>(
                basename,
                iter,
                chunk_size,
                compression_flags,
                num_nodes,
            ),
            #[cfg(any(
                feature = "le_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            LE::NAME => Self::single_thread::<LittleEndian, _>(
                basename,
                iter,
                chunk_size,
                compression_flags,
                num_nodes,
            ),
            x => anyhow::bail!("Unknown endianness {}", x),
        }
    }

    /// Compresses s [`NodeLabelsLender`] and returns the length in bits of the
    /// graph bitstream.
    pub fn single_thread<E, L>(
        basename: impl AsRef<Path>,
        iter: L,
        chunk_size: usize,
        compression_flags: CompFlags,
        num_nodes: Option<usize>,
    ) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        let basename = basename.as_ref();
        let graph_path = basename.with_extension(GRAPH_EXTENSION);

        // Compress the graph
        let bit_write = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
            File::create(&graph_path)
                .with_context(|| format!("Could not create {}", graph_path.display()))?,
        )));

        let comp_flags = CompFlags {
            ..Default::default()
        };

        let codes_writer = DynCodesEncoder::new(bit_write, &comp_flags)?;

        let mut bvcomp = BvCompZ::new(
            codes_writer,
            compression_flags.compression_window,
            chunk_size,
            compression_flags.max_ref_count,
            compression_flags.min_interval_length,
            0,
        );

        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("node")
            .expected_updates(num_nodes);
        pl.start("Compressing successors...");
        let mut bitstream_len = 0;

        let mut real_num_nodes = 0;
        for_! ( (_node_id, successors) in iter {
            bitstream_len += bvcomp.push(successors).context("Could not push successors")?;
            pl.update();
            real_num_nodes += 1;
        });
        pl.done();

        if let Some(num_nodes) = num_nodes {
            if num_nodes != real_num_nodes {
                log::warn!(
                    "The expected number of nodes is {} but the actual number of nodes is {}",
                    num_nodes,
                    real_num_nodes
                );
            }
        }
        let num_arcs = bvcomp.arcs;
        bitstream_len += bvcomp.flush().context("Could not flush bvcompz")? as u64;

        log::info!("Writing the .properties file");
        let properties = compression_flags
            .to_properties::<BE>(real_num_nodes, num_arcs, bitstream_len)
            .context("Could not serialize properties")?;
        let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
        std::fs::write(&properties_path, properties)
            .with_context(|| format!("Could not write {}", properties_path.display()))?;

        Ok(bitstream_len)
    }
}
*/
