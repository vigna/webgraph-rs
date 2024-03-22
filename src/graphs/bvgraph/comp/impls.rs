/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// A queue that pulls jobs with ids in a contiguous initial segment of the
/// natural numbers from an iterator out of order and implement an iterator in
/// which they can be pulled in order.
///
/// Jobs must be ordered by their job id, and must implement [`Eq`] with a
/// [`usize`] using their job id.
struct TaskQueue<I: std::iter::Iterator> {
    iter: I,
    jobs: Vec<Option<I::Item>>,
    next_id: usize,
}

trait JobId {
    fn id(&self) -> usize;
}

impl<I: std::iter::Iterator> TaskQueue<I> {
    fn new(iter: I) -> Self {
        Self {
            iter,
            jobs: vec![],
            next_id: 0,
        }
    }
}

impl<I: std::iter::Iterator> std::iter::Iterator for TaskQueue<I>
where
    I::Item: JobId + Copy,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(Some(item)) = self.jobs.get(self.next_id) {
                self.next_id += 1;
                return Some(*item);
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
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
struct Job {
    job_id: usize,
    written_bits: u64,
    num_arcs: u64,
}

impl JobId for Job {
    fn id(&self) -> usize {
        self.job_id
    }
}

impl BVComp<()> {
    /// Compresses s [`NodeLabelsLender`] and returns the lenght in bits of the
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

        let mut bvcomp = BVComp::new(
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
                writer.write_gamma(delta as u64).context("Could not write delta")?;
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
    /// Endianess can only be [`BE::NAME`](BE) or [`LE::NAME`](LE).
    ///
    ///  A given endianess is enabled only if the corresponding feature is
    /// enabled, `be_bins` for big endian and `le_bins` for little endian, or if
    /// neither features are enabled.
    pub fn parallel_endianness<P: AsRef<Path>, G: SplitLabeling + SequentialGraph>(
        basename: impl AsRef<Path> + Send + Sync,
        graph: &G,
        num_nodes: usize,
        compression_flags: CompFlags,
        mut threads: impl AsMut<rayon::ThreadPool>,
        tmp_dir: P,
        endianess: &str,
    ) -> Result<u64>
    where
        for<'a> <G as SplitLabeling>::Lender<'a>: Send + Sync,
    {
        match endianess {
            #[cfg(any(
                feature = "be_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            BE::NAME => {
                // compress the transposed graph
                Self::parallel_iter::<BigEndian, _>(
                    basename,
                    graph.split_iter(threads.as_mut().current_num_threads()).into_iter(),
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
                    graph.split_iter(threads.as_mut().current_num_threads()).into_iter(),
                    num_nodes,
                    compression_flags,
                    threads,
                    tmp_dir,
                )
            }
            x => anyhow::bail!("Unknown endianness {}", x),
        }
    }

    /// Compresses a graph in parallel and returns the lenght in bits of the graph bitstream.
    pub fn parallel_graph<E: Endianness>(
        basename: impl AsRef<Path> + Send + Sync,
        graph: &(impl SequentialGraph + SplitLabeling),
        compression_flags: CompFlags,
        mut threads: impl AsMut<rayon::ThreadPool>,
        tmp_dir: impl AsRef<Path>,
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodeWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        Self::parallel_iter(
            basename,
            graph.split_iter(threads.as_mut().current_num_threads()).into_iter(),
            graph.num_nodes(),
            compression_flags,
            threads,
            tmp_dir,
        )
    }

    /// Compresses multiple [`NodeLabelsLender`] in parallel and returns the lenght in bits
    /// of the graph bitstream.
    pub fn parallel_iter<
        E: Endianness,
        L: Lender + for<'next> NodeLabelsLender<'next, Label = usize> + Send,
    >(
        basename: impl AsRef<Path> + Send + Sync,
        iter: impl std::iter::Iterator<Item = L>,
        num_nodes: usize,
        compression_flags: CompFlags,
        mut threads: impl AsMut<rayon::ThreadPool>,
        tmp_dir: impl AsRef<Path>,
    ) -> Result<u64>
    where
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodeWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        let thread_pool = threads.as_mut();

        let tmp_dir = tmp_dir.as_ref();
        let basename = basename.as_ref();

        let graph_path = basename.with_extension(GRAPH_EXTENSION);

        let (tx, rx) = std::sync::mpsc::channel();

        let thread_path = |thread_id: usize| tmp_dir.join(format!("{:016x}.bitstream", thread_id));

        thread_pool.in_place_scope(|s| {
            let cp_flags = &compression_flags;

            for (thread_id, mut thread_lender) in iter.enumerate() {
                let file_path = thread_path(thread_id);
                let tx = tx.clone();
                // Spawn the thread
                s.spawn(move |_| {
                    log::info!("Thread {} started", thread_id);

                    let (mut bvcomp, mut written_bits) =
                        if let Some((node_id, successors)) = thread_lender.next() {
                            let writer = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(
                                BufWriter::new(File::create(&file_path).unwrap()),
                            ));
                            let codes_encoder = <DynCodesEncoder<E, _>>::new(writer, cp_flags);

                            let mut bvcomp = BVComp::new(
                                codes_encoder,
                                cp_flags.compression_window,
                                cp_flags.max_ref_count,
                                cp_flags.min_interval_length,
                                node_id,
                            );
                            let written_bits = bvcomp.push(successors).unwrap() as u64;
                            (bvcomp, written_bits)
                        } else {
                            return;
                        };

                    written_bits += bvcomp.extend(thread_lender).unwrap();
                    let num_arcs = bvcomp.arcs;
                    bvcomp.flush().unwrap();
                    // TODO written_bits += bvcomp.flush().unwrap();
                    log::info!(
                        "Finished Compression thread {} and wrote {} bits",
                        thread_id,
                        written_bits
                    );
                    tx.send(Job {
                        job_id: thread_id,
                        written_bits,
                        num_arcs,
                    })
                    .unwrap()
                });
            }

            drop(tx);

            // setup the final bitstream from the end, because the first thread
            // already wrote the first chunk
            let file = File::create(&graph_path)
                .with_context(|| format!("Could not create graph {}", graph_path.display()))?;

            let mut result_writer =
                <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(file)));

            let mut total_written_bits: u64 = 0;
            let mut total_arcs: u64 = 0;

            // glue toghether the bitstreams as they finish, this allows us to do
            // task pipelining for better performance
            for Job {
                job_id,
                written_bits,
                num_arcs,
            } in TaskQueue::new(rx.iter())
            {
                total_arcs += num_arcs;
                // compute the path of the bitstream created by this thread
                let file_path = thread_path(job_id);
                log::info!(
                    "Copying {} [{}..{}) bits from {} to {}",
                    written_bits,
                    total_written_bits,
                    total_written_bits + written_bits,
                    file_path.display(),
                    basename.display()
                );
                total_written_bits += written_bits;

                let mut reader =
                    <BufBitReader<E, _>>::new(<WordAdapter<u32, _>>::new(BufReader::new(
                        File::open(&file_path)
                            .with_context(|| format!("Could not open {}", file_path.display()))?,
                    )));
                result_writer
                    .copy_from(&mut reader, written_bits as u64)
                    .with_context(|| {
                        format!(
                            "Could not copy from {} to {}",
                            file_path.display(),
                            graph_path.display()
                        )
                    })?;
            }

            log::info!("Flushing the merged Compression bitstream");
            result_writer.flush()?;

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

            // cleanup the temp files
            std::fs::remove_dir_all(tmp_dir).with_context(|| {
                format!("Could not clean temporary directory {}", tmp_dir.display())
            })?;
            Ok(total_written_bits)
        })
    }
}
