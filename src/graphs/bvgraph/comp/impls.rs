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
use std::borrow::Borrow;
use std::fs::File;
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

impl BvComp<()> {
    /// Compresses s [`NodeLabelsLender`] and returns the length in bits of the
    /// graph bitstream.
    pub fn single_thread<E, L>(
        basename: impl AsRef<Path>,
        iter: L,
        compression_flags: CompFlags,
        build_offsets: bool,
        num_nodes: Option<usize>,
    ) -> Result<u64>
    where
        E: Endianness,
        L: IntoLender,
        L::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: CodeWrite<E>,
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

        let codes_writer = DynCodesEncoder::new(bit_write, &comp_flags);

        let mut bvcomp = BvComp::new(
            codes_writer,
            compression_flags.compression_window,
            compression_flags.max_ref_count,
            compression_flags.min_interval_length,
            0,
        );

        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("node")
            .expected_updates(num_nodes);
        pl.start("Compressing successors...");
        let mut result = 0;

        let mut real_num_nodes = 0;
        if build_offsets {
            let offsets_path = basename.with_extension(OFFSETS_EXTENSION);
            let file = std::fs::File::create(&offsets_path)
                .with_context(|| format!("Could not create {}", offsets_path.display()))?;
            // create a bit writer on the file
            let mut writer = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(
                BufWriter::with_capacity(1 << 20, file),
            ));

            writer
                .write_gamma(0)
                .context("Could not write initial delta")?;
            for_! ( (_node_id, successors) in iter {
                let delta = bvcomp.push(successors).context("Could not push successors")?;
                result += delta;
                writer.write_gamma(delta).context("Could not write delta")?;
                pl.update();
                real_num_nodes += 1;
            });
        } else {
            for_! ( (_node_id, successors) in iter {
                result += bvcomp.push(successors).context("Could not push successors")?;
                pl.update();
                real_num_nodes += 1;
            });
        }
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

        log::info!("Writing the .properties file");
        let properties = compression_flags
            .to_properties::<BE>(real_num_nodes, bvcomp.arcs)
            .context("Could not serialize properties")?;
        let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
        std::fs::write(&properties_path, properties)
            .with_context(|| format!("Could not write {}", properties_path.display()))?;

        bvcomp.flush().context("Could not flush bvcomp")?;
        Ok(result)
    }

    /// A wrapper over [`parallel_graph`](Self::parallel_graph) that takes the
    /// endianness as a string.
    ///
    /// Endianness can only be [`BE::NAME`](BE) or [`LE::NAME`](LE).
    ///
    ///  A given endianness is enabled only if the corresponding feature is
    /// enabled, `be_bins` for big endian and `le_bins` for little endian, or if
    /// neither features are enabled.
    pub fn parallel_endianness<P: AsRef<Path>, G: SplitLabeling + SequentialGraph>(
        basename: impl AsRef<Path> + Send + Sync,
        graph: &G,
        num_nodes: usize,
        compression_flags: CompFlags,
        threads: impl Borrow<rayon::ThreadPool>,
        tmp_dir: P,
        endianness: &str,
    ) -> Result<u64>
    where
        for<'a> <G as SplitLabeling>::SplitLender<'a>: Send + Sync,
    {
        match endianness {
            #[cfg(any(
                feature = "be_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            BE::NAME => {
                // compress the transposed graph
                Self::parallel_iter::<BigEndian, _>(
                    basename,
                    graph
                        .split_iter(threads.borrow().current_num_threads())
                        .into_iter(),
                    num_nodes,
                    compression_flags,
                    threads,
                    tmp_dir,
                )
            }
            #[cfg(any(
                feature = "le_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            LE::NAME => {
                // compress the transposed graph
                Self::parallel_iter::<LittleEndian, _>(
                    basename,
                    graph
                        .split_iter(threads.borrow().current_num_threads())
                        .into_iter(),
                    num_nodes,
                    compression_flags,
                    threads,
                    tmp_dir,
                )
            }
            x => anyhow::bail!("Unknown endianness {}", x),
        }
    }

    /// Compresses a graph in parallel and returns the length in bits of the graph bitstream.
    pub fn parallel_graph<E: Endianness>(
        basename: impl AsRef<Path> + Send + Sync,
        graph: &(impl SequentialGraph + SplitLabeling),
        compression_flags: CompFlags,
        threads: impl Borrow<rayon::ThreadPool>,
        tmp_dir: impl AsRef<Path>,
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodeWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        Self::parallel_iter(
            basename,
            graph
                .split_iter(threads.borrow().current_num_threads())
                .into_iter(),
            graph.num_nodes(),
            compression_flags,
            threads,
            tmp_dir,
        )
    }

    /// Compresses multiple [`NodeLabelsLender`] in parallel and returns the length in bits
    /// of the graph bitstream.
    pub fn parallel_iter<
        E: Endianness,
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize> + Send,
    >(
        basename: impl AsRef<Path> + Send + Sync,
        iter: impl Iterator<Item = L>,
        num_nodes: usize,
        compression_flags: CompFlags,
        threads: impl Borrow<rayon::ThreadPool>,
        tmp_dir: impl AsRef<Path>,
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodeWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        let thread_pool = threads.borrow();
        let tmp_dir = tmp_dir.as_ref();
        let basename = basename.as_ref();

        let graph_path = basename.with_extension(GRAPH_EXTENSION);
        let offsets_path = basename.with_extension(OFFSETS_EXTENSION);

        let (tx, rx) = std::sync::mpsc::channel();

        let thread_path = |thread_id: usize| tmp_dir.join(format!("{:016x}.bitstream", thread_id));

        thread_pool.in_place_scope(|s| {
            let cp_flags = &compression_flags;

            for (thread_id, mut thread_lender) in iter.enumerate() {
                let tmp_path = thread_path(thread_id);
                let chunk_graph_path = tmp_path.with_extension(GRAPH_EXTENSION);
                let chunk_offsets_path = tmp_path.with_extension(OFFSETS_EXTENSION);
                let tx = tx.clone();
                // Spawn the thread
                s.spawn(move |_| {
                    log::info!("Thread {} started", thread_id);
                    let first_node;
                    let mut bvcomp;
                    let mut offsets_writer;
                    let mut written_bits;
                    let mut offsets_written_bits;

                    match thread_lender.next() {
                        None => return,
                        Some((node_id, successors)) => {
                            first_node = node_id;

                            offsets_writer = <BufBitWriter<BigEndian, _>>::new(<WordAdapter<usize, _>>::new(
                                BufWriter::new(File::create(&chunk_offsets_path).unwrap()),
                            ));

                            let writer = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(
                                BufWriter::new(File::create(&chunk_graph_path).unwrap()),
                            ));
                            let codes_encoder = <DynCodesEncoder<E, _>>::new(writer, cp_flags);

                            bvcomp = BvComp::new(
                                codes_encoder,
                                cp_flags.compression_window,
                                cp_flags.max_ref_count,
                                cp_flags.min_interval_length,
                                node_id,
                            );
                            written_bits = bvcomp.push(successors).unwrap();
                            offsets_written_bits = offsets_writer.write_gamma(written_bits).unwrap() as u64;
                        }
                    };

                    let mut last_node = first_node;
                    let iter_nodes = thread_lender.inspect(|(x, _)| last_node = *x);
                    for_! ( (_, succ) in iter_nodes {
                        let node_bits = bvcomp.push(succ.into_iter()).unwrap();
                        written_bits += node_bits;
                        offsets_written_bits += offsets_writer.write_gamma(node_bits).unwrap() as u64;
                    });

                    let num_arcs = bvcomp.arcs;
                    bvcomp.flush().unwrap();
                    offsets_writer.flush().unwrap();

                    log::info!(
                        "Finished Compression thread {} and wrote {} bits for the graph and {} bits for the offsets",
                        thread_id,
                        written_bits,
                        offsets_written_bits,
                    );
                    tx.send(Job {
                        job_id: thread_id,
                        first_node,
                        last_node,
                        chunk_graph_path,
                        written_bits,
                        chunk_offsets_path,
                        offsets_written_bits,
                        num_arcs,
                    })
                    .unwrap()
                });
            }

            drop(tx);

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
                log::info!(
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

                log::info!(
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
            }

            log::info!("Flushing the merged bitstreams");
            graph_writer.flush()?;
            offsets_writer.flush()?;

            log::info!("Writing the .properties file");
            let properties = compression_flags
                .to_properties::<BE>(num_nodes, total_arcs)
                .context("Could not serialize properties")?;
            let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
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
            std::fs::remove_dir_all(tmp_dir).with_context(|| {
                format!("Could not clean temporary directory {}", tmp_dir.display())
            })?;
            Ok(total_written_bits)
        })
    }
}
