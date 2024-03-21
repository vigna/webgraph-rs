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
use std::sync::{Arc, Mutex};
use std::thread::ScopedJoinHandle;

impl BVComp<()> {
    /// Build a BVGraph by compressing an iterator of nodes and successors and
    /// return the length of the produced bitstream (in bits).
    pub fn single_thread<E, L>(
        basename: impl AsRef<Path>,
        iter: L,
        compression_flags: CompFlags,
        build_offsets: bool,
        num_nodes: Option<usize>,
    ) -> Result<usize>
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

    /// A wrapper over [`parallel_compress_sequential_iter`] that takes the
    /// endianness as a string.
    ///
    /// Endianess can only be [`BE::NAME`](BE) or [`LE::NAME`](LE)
    /// A given endianess is enabled only if the corresponding feature is enabled,
    /// `be_bins` for big endian and `le_bins` for little endian, or if neither
    /// features are enabled.
    pub fn parallel_endianness<L: IntoLender, P: AsRef<Path>>(
        basename: impl AsRef<Path> + Send + Sync,
        into_lender: L,
        num_nodes: usize,
        compression_flags: CompFlags,
        num_threads: usize,
        tmp_dir: P,
        endianess: &str,
    ) -> Result<usize>
    where
        L::Lender: Clone + Send + for<'next> NodeLabelsLender<'next, Label = usize>,
    {
        match endianess {
            #[cfg(any(
                feature = "be_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            BE::NAME => {
                // compress the transposed graph
                Self::parallel::<BigEndian, _>(
                    basename,
                    into_lender,
                    num_nodes,
                    compression_flags,
                    num_threads,
                    tmp_dir,
                )
            }
            #[cfg(any(
                feature = "le_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            LE::NAME => {
                // compress the transposed graph
                Self::parallel::<LittleEndian, _>(
                    basename,
                    into_lender,
                    num_nodes,
                    compression_flags,
                    num_threads,
                    tmp_dir,
                )
            }
            x => anyhow::bail!("Unknown endianness {}", x),
        }
    }
    /// Compress an iterator of nodes and successors in parllel and return the
    /// lenght in bits of the produced file
    pub fn parallel<E: Endianness, L: IntoLender>(
        basename: impl AsRef<Path> + Send + Sync,
        into_lender: L,
        num_nodes: usize,
        compression_flags: CompFlags,
        num_threads: usize,
        tmp_dir: impl AsRef<Path>,
    ) -> Result<usize>
    where
        L::Lender: Clone + Send + for<'next> NodeLabelsLender<'next, Label = usize>,
        BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodeWrite<E>,
        BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    {
        let tmp_dir = tmp_dir.as_ref();
        let basename = basename.as_ref();
        let mut iter = into_lender.into_lender();
        let graph_path = basename.with_extension(GRAPH_EXTENSION);
        assert_ne!(num_threads, 0);
        let nodes_per_thread = num_nodes / num_threads;

        std::thread::scope(|s| {
            // collect the handles in vec, otherwise the handles will be dropped
            // in-place calling a join and making the algorithm sequential.
            #[allow(clippy::type_complexity)]
            let mut handles: Vec<Mutex<Option<ScopedJoinHandle<(usize, u64)>>>> = vec![];
            handles.resize_with(num_threads, || Mutex::new(None));
            let handles = Arc::new(handles);

            let cp_flags = &compression_flags;

            // spawn a the thread for the last chunk that will spawn all the previous ones
            // this will be the longest running thread
            let last_thread_id = num_threads - 1;
            // handle the case when this is the only available thread
            let last_file_path = tmp_dir.join(format!("{:016x}.bitstream", last_thread_id));

            log::info!(
                "Spawning the main compression thread {} writing on {} nodes [{}..{})",
                last_thread_id,
                last_file_path.display(),
                last_thread_id * nodes_per_thread,
                num_nodes,
            );
            let sub_handles = handles.clone();
            let handle = s.spawn(move || {
                // for the first N - 1 threads, clone the iter and skip to the next
                // splitting point, then start a new compression thread
                for thread_id in 0..num_threads.saturating_sub(1) {
                    // the first thread can directly write to the result bitstream
                    let file_path = tmp_dir.join(format!("{:016x}.bitstream", thread_id));

                    // spawn the thread
                    log::info!(
                        "Spawning compression thread {} writing on {} nodes [{}..{})",
                        thread_id,
                        file_path.display(),
                        nodes_per_thread * thread_id,
                        nodes_per_thread * (thread_id + 1),
                    );
                    // Spawn the thread
                    let thread_iter = iter.clone().take(nodes_per_thread);
                    let handle = s.spawn(move || {
                        log::info!("Thread {} started", thread_id,);
                        let writer = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(
                            BufWriter::new(File::create(&file_path).unwrap()),
                        ));
                        let codes_writer = <DynCodesEncoder<E, _>>::new(writer, cp_flags);
                        let mut bvcomp = BVComp::new(
                            codes_writer,
                            cp_flags.compression_window,
                            cp_flags.max_ref_count,
                            cp_flags.min_interval_length,
                            nodes_per_thread * thread_id,
                        );
                        let mut written_bits = 0;
                        for_! [(_node_id, successors) in thread_iter {
                            written_bits += bvcomp.push(successors).unwrap();
                        }];
                        let arcs = bvcomp.arcs;
                        written_bits += bvcomp.flush().unwrap();

                        log::info!(
                            "Finished Compression thread {} and wrote {} bits [{}..{})",
                            thread_id,
                            written_bits,
                            nodes_per_thread * thread_id,
                            nodes_per_thread * (thread_id + 1),
                        );

                        (written_bits, arcs)
                    });
                    {
                        *(sub_handles[thread_id]).lock().unwrap() = Some(handle);
                    }
                    log::info!("Skipping {} nodes from the iterator", nodes_per_thread);

                    // skip the next nodes_per_thread nodes
                    for _ in 0..nodes_per_thread {
                        iter.next();
                    }
                }

                // handle the case when this is the only available thread
                let last_file_path = tmp_dir.join(format!("{:016x}.bitstream", last_thread_id));
                // complete the last chunk
                let writer = <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(
                    BufWriter::new(File::create(last_file_path).unwrap()),
                ));
                let codes_writer = <DynCodesEncoder<E, _>>::new(writer, &compression_flags);
                let mut bvcomp = BVComp::new(
                    codes_writer,
                    compression_flags.compression_window,
                    compression_flags.max_ref_count,
                    compression_flags.min_interval_length,
                    last_thread_id * nodes_per_thread,
                );
                let written_bits = bvcomp.extend(iter).unwrap();

                log::info!(
                    "Finished compression thread {} and wrote {} bits [{}..{})",
                    last_thread_id,
                    written_bits,
                    last_thread_id * nodes_per_thread,
                    num_nodes,
                );
                (written_bits, bvcomp.arcs)
            });
            {
                *(handles[last_thread_id]).lock().unwrap() = Some(handle);
            }
            // setup the final bitstream from the end, because the first thread
            // already wrote the first chunk
            let file = File::create(&graph_path)
                .with_context(|| format!("Could not create graph {}", graph_path.display()))?;

            // create hte buffered writer
            let mut result_writer =
                <BufBitWriter<E, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(file)));

            let mut result_len = 0;
            let mut total_arcs = 0;
            // glue toghether the bitstreams as they finish, this allows us to do
            // task pipelining for better performance
            for thread_id in 0..num_threads {
                log::info!("Waiting for thread {}", thread_id);
                // wait for the thread to finish
                let (bits_to_copy, n_arcs) = loop {
                    {
                        let mut maybe_handle = handles[thread_id].lock().unwrap();
                        if maybe_handle.is_some() {
                            break maybe_handle.take().unwrap().join().unwrap();
                        }
                    }
                    std::thread::yield_now();
                    std::thread::sleep(std::time::Duration::from_millis(100));
                };
                total_arcs += n_arcs;
                // compute the path of the bitstream created by this thread
                let file_path = tmp_dir.join(format!("{:016x}.bitstream", thread_id));
                log::info!(
                    "Copying {} [{}..{}) bits from {} to {}",
                    bits_to_copy,
                    result_len,
                    result_len + bits_to_copy,
                    file_path.display(),
                    basename.display()
                );
                result_len += bits_to_copy;

                let mut reader =
                    <BufBitReader<E, _>>::new(<WordAdapter<u32, _>>::new(BufReader::new(
                        File::open(&file_path)
                            .with_context(|| format!("Could not open {}", file_path.display()))?,
                    )));
                result_writer
                    .copy_from(&mut reader, bits_to_copy as u64)
                    .with_context(|| {
                        format!(
                            "Could not copy from {} to {}",
                            file_path.display(),
                            graph_path.display()
                        )
                    })?;
            }

            log::info!("Flushing the merged Compression bitstream");
            result_writer.flush().unwrap();

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
                result_len,
                result_len as f64 / total_arcs as f64
            );

            // cleanup the temp files
            std::fs::remove_dir_all(tmp_dir).with_context(|| {
                format!("Could not clean temporary directory {}", tmp_dir.display())
            })?;
            Ok(result_len)
        })
    }
}
