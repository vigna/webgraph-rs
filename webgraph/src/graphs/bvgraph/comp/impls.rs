/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
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

/// An iterator over items received from a channel that uses
/// rayon's yield_now to avoid blocking worker threads.
struct ChannelIter<T> {
    rx: std::sync::mpsc::Receiver<T>,
}

impl<T> Iterator for ChannelIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.rx.try_recv() {
                Ok(item) => return Some(item),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return None,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    rayon::yield_now();
                }
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
    /// Creates a new writer and writes the first offset value (0).
    pub fn from_write(writer: W, write_zero: bool) -> Result<Self> {
        let mut buffer = BufBitWriter::new(WordAdapter::new(BufWriter::new(writer)));
        if write_zero {
            // the first offset (of the first parallel offsets file) is always zero
            buffer.write_gamma(0)?;
        }
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

/// A configuration for BvGraph compression.
///
/// Once set up, the methods [`comp_graph`](Self::comp_graph) (sequential graph
/// compression), [`comp_lender`](Self::comp_lender) (sequential lender
/// compression), [`par_comp_graph`](Self::par_comp_graph) (parallel compression
/// of a splittable graph), and [`par_comp_lenders`](Self::par_comp_lenders)
/// (parallel compression of multiple lenders) can be used to compress a graph.
#[derive(Debug)]
pub struct BvCompConfig {
    /// The basename of the output files.
    basename: PathBuf,
    /// Compression flags for BvComp/BvCompZ.
    comp_flags: CompFlags,
    /// Selects the Zuckerli-based BVGraph compressor
    bvgraphz: bool,
    /// The chunk size for the Zuckerli-based compressor
    chunk_size: usize,
    /// Temporary directory for all operations.
    tmp_dir: Option<PathBuf>,
    /// Owns the TempDir that [`Self::tmp_dir`] refers to, if it was created by default.
    owned_tmp_dir: Option<tempfile::TempDir>,
}

impl BvCompConfig {
    /// Creates a new compression configuration with the given basename and
    /// default options.
    ///
    /// Note that the convenience methods
    /// [`BvComp::with_basename`](crate::graphs::bvgraph::comp::BvComp::with_basename)
    /// and
    /// [`BvCompZ::with_basename`](crate::graphs::bvgraph::comp::BvCompZ::with_basename)
    /// can be used to create a configuration with default options.
    pub fn new(basename: impl AsRef<Path>) -> Self {
        Self {
            basename: basename.as_ref().into(),
            comp_flags: CompFlags::default(),
            bvgraphz: false,
            chunk_size: 10_000,
            tmp_dir: None,
            owned_tmp_dir: None,
        }
    }
}

impl BvCompConfig {
    pub fn with_comp_flags(mut self, compression_flags: CompFlags) -> Self {
        self.comp_flags = compression_flags;
        self
    }

    pub fn with_tmp_dir(mut self, tmp_dir: impl AsRef<Path>) -> Self {
        self.tmp_dir = Some(tmp_dir.as_ref().into());
        self
    }

    pub fn with_bvgraphz(mut self) -> Self {
        self.bvgraphz = true;
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.bvgraphz = true;
        self.chunk_size = chunk_size;
        self
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

    /// Compresses sequentially a [`SequentialGraph`] and returns
    /// the number of bits written to the graph bitstream.
    pub fn comp_graph<E: Endianness>(&mut self, graph: impl SequentialGraph) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        self.comp_lender::<E, _>(graph.iter(), Some(graph.num_nodes()))
    }

    /// Compresses sequentially a [`NodeLabelsLender`] and returns
    /// the number of bits written to the graph bitstream.
    ///
    /// The optional `expected_num_nodes` parameter will be used to provide
    /// forecasts on the progress logger.
    pub fn comp_lender<E, L>(&mut self, iter: L, expected_num_nodes: Option<usize>) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodesWrite<E>,
    {
        let graph_path = self.basename.with_extension(GRAPH_EXTENSION);

        // Compress the graph
        let bit_write = buf_bit_writer::from_path::<E, usize>(&graph_path)
            .with_context(|| format!("Could not create {}", graph_path.display()))?;

        let codes_writer = DynCodesEncoder::new(bit_write, &self.comp_flags)?;

        // create a file for offsets
        let offsets_path = self.basename.with_extension(OFFSETS_EXTENSION);
        let offset_writer = OffsetsWriter::from_path(offsets_path, true)?;

        let mut pl = progress_logger![
            display_memory = true,
            item_name = "node",
            expected_updates = expected_num_nodes,
        ];
        pl.start("Compressing successors...");
        let comp_stats = if self.bvgraphz {
            let mut bvcompz = BvCompZ::new(
                codes_writer,
                offset_writer,
                self.comp_flags.compression_window,
                self.chunk_size,
                self.comp_flags.max_ref_count,
                self.comp_flags.min_interval_length,
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
                self.comp_flags.compression_window,
                self.comp_flags.max_ref_count,
                self.comp_flags.min_interval_length,
                0,
            );

            for_! ( (_node_id, successors) in iter {
                bvcomp.push(successors).context("Could not push successors")?;
                pl.update();
            });
            pl.done();

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

        log::info!("Writing the .properties file");
        let properties = self
            .comp_flags
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

    /// A wrapper over [`par_comp_lenders`](Self::par_comp_lenders) that takes the
    /// endianness as a string.
    ///
    /// Endianness can only be [`BE::NAME`](BE) or [`LE::NAME`](LE).
    ///
    ///  A given endianness is enabled only if the corresponding feature is
    /// enabled, `be_bins` for big endian and `le_bins` for little endian, or if
    /// neither features are enabled.
    pub fn par_comp_lenders_endianness<G: SplitLabeling + SequentialGraph>(
        &mut self,
        graph: &G,
        num_nodes: usize,
        endianness: &str,
    ) -> Result<u64>
    where
        for<'a> <G as SplitLabeling>::SplitLender<'a>: ExactSizeLender + Send + Sync,
    {
        let num_threads = current_num_threads();

        match endianness {
            #[cfg(any(
                feature = "be_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            BE::NAME => {
                // compress the transposed graph
                self.par_comp_lenders::<BigEndian, _>(graph.split_iter(num_threads), num_nodes)
            }
            #[cfg(any(
                feature = "le_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            LE::NAME => {
                // compress the transposed graph
                self.par_comp_lenders::<LittleEndian, _>(graph.split_iter(num_threads), num_nodes)
            }
            x => anyhow::bail!("Unknown endianness {}", x),
        }
    }

    /// Compresses a splittable sequential graph in parallel and returns the
    /// length in bits of the graph bitstream.
    ///
    /// Note that the number of parallel compression threads will be
    /// [`current_num_threads`]. The graph will be split into as many
    /// lenders as there are threads.
    pub fn par_comp_graph<E: Endianness>(
        &mut self,
        graph: &(impl SequentialGraph + for<'a> SplitLabeling<SplitLender<'a>: ExactSizeLender>),
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        let num_threads = current_num_threads();
        self.par_comp_lenders(graph.split_iter(num_threads), graph.num_nodes())
    }

    /// Compresses multiple [`NodeLabelsLender`] in parallel and returns
    /// a [`CompStats`] instance.
    ///
    /// The first lender must start from node 0, and each lender must
    /// continue from where the previous lender stopped. All in all,
    /// they must return a total of `num_nodes` nodes.
    ///
    /// Note that the number of parallel compression threads will be
    /// [`current_num_threads`]. It is your responsibility to ensure that the
    /// number of threads is appropriate for the number of lenders you pass,
    /// possibly using [`install`](rayon::ThreadPool::install).
    ///
    /// This method is useful to compress graphs that can be iterated upon using
    /// multiple lenders, but such lenders do not derive from splitting. For
    /// example, this happens when using
    /// [`ParSortIters`].
    pub fn par_comp_lenders<
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
        let tmp_dir = self.tmp_dir()?;

        let graph_path = self.basename.with_extension(GRAPH_EXTENSION);
        let offsets_path = self.basename.with_extension(OFFSETS_EXTENSION);

        let (tx, rx) = std::sync::mpsc::channel();

        let thread_path = |thread_id: usize| tmp_dir.join(format!("{thread_id:016x}.bitstream"));

        let mut comp_pl = concurrent_progress_logger![
            log_target = "webgraph::graphs::bvgraph::comp::impls::par_comp_lenders::comp",
            display_memory = true,
            item_name = "node",
            local_speed = true,
            expected_updates = Some(num_nodes),
        ];
        comp_pl.start("Compressing successors in parallel...");
        let mut expected_first_node = 0;
        let cp_flags = &self.comp_flags;
        let bvgraphz = self.bvgraphz;
        let chunk_size = self.chunk_size;

        in_place_scope(|s| {
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

                    let writer = buf_bit_writer::from_path::<E, usize>(&chunk_graph_path).unwrap();
                    let codes_encoder = <DynCodesEncoder<E, _>>::new(writer, cp_flags).unwrap();

                    let stats;
                    let mut last_node;
                    if bvgraphz {
                        let mut bvcomp = BvCompZ::new(
                            codes_encoder,
                            OffsetsWriter::from_path(&chunk_offsets_path, false).unwrap(),
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
                            OffsetsWriter::from_path(&chunk_offsets_path, false).unwrap(),
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
                            comp_pl.update();
                        });
                        stats = bvcomp.flush().unwrap();
                    }

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
                    num_nodes, expected_first_node
                );
            }

            drop(tx);

            let mut copy_pl = progress_logger![
                log_target = "webgraph::graphs::bvgraph::comp::impls::par_comp_lenders::copy",
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(num_nodes),
            ];
            copy_pl.start("Copying compressed successors to final graph");

            let mut graph_writer = buf_bit_writer::from_path::<E, usize>(&graph_path)
                .with_context(|| format!("Could not create graph {}", graph_path.display()))?;

            let mut offsets_writer = buf_bit_writer::from_path::<BE, usize>(&offsets_path)
                .with_context(|| format!("Could not create offsets {}", offsets_path.display()))?;
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
            } in TaskQueue::new(ChannelIter { rx })
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
                let mut reader = buf_bit_reader::from_path::<E, u32>(&chunk_graph_path)?;
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

                let mut reader = <BufBitReader<BigEndian, _>>::new(<WordAdapter<u32, _>>::new(
                    BufReader::new(File::open(&chunk_offsets_path).with_context(|| {
                        format!("Could not open {}", chunk_offsets_path.display())
                    })?),
                ));
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
            let properties = self
                .comp_flags
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
