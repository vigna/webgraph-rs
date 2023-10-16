/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::*;

use anyhow::Result;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use hrtb_lending_iterator::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::ScopedJoinHandle;
use tempfile::tempdir;

/// Build a BVGraph by compressing an iterator of nodes and successors and
/// return the lenght of the produced bitstream (in bits).
pub fn compress_sequential_iter<
    P: AsRef<Path>,
    I: ExactSizeIterator<Item = (usize, J)>,
    J: Iterator<Item = usize>,
>(
    basename: P,
    iter: I,
    compression_flags: CompFlags,
    build_offsets: bool,
) -> Result<usize> {
    let basename = basename.as_ref();
    let graph_path = format!("{}.graph", basename.to_string_lossy());

    // Compress the graph
    let bit_write =
        <BufBitWriter<BE, _>>::new(WordAdapter::new(BufWriter::new(File::create(&graph_path)?)));

    let comp_flags = CompFlags {
        ..Default::default()
    };

    let codes_writer = DynamicCodesWriter::new(bit_write, &comp_flags);

    let mut bvcomp = BVComp::new(
        codes_writer,
        compression_flags.compression_window,
        compression_flags.min_interval_length,
        compression_flags.max_ref_count,
        0,
    );
    let num_nodes = iter.len();

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "node";
    pr.expected_updates = Some(num_nodes);
    pr.start("Compressing successors...");
    let mut result = 0;

    if build_offsets {
        let file = std::fs::File::create(&format!("{}.offsets", basename.to_string_lossy()))?;
        // create a bit writer on the file
        let mut writer = <BufBitWriter<BE, _>>::new(<WordAdapter<u64, _>>::new(
            BufWriter::with_capacity(1 << 20, file),
        ));

        writer.write_gamma(0)?;
        for (_node_id, successors) in iter {
            let delta = bvcomp.push(successors)?;
            result += delta;
            writer.write_gamma(delta as u64)?;
            pr.update();
        }
    } else {
        for (_node_id, successors) in iter {
            result += bvcomp.push(successors)?;
            pr.update();
        }
    }
    pr.done();

    log::info!("Writing the .properties file");
    let properties = compression_flags.to_properties(num_nodes, bvcomp.arcs);
    std::fs::write(
        format!("{}.properties", basename.to_string_lossy()),
        properties,
    )?;

    bvcomp.flush()?;
    Ok(result)
}

/// Compress an iterator of nodes and successors in parllel and return the
/// lenght in bits of the produced file
pub fn parallel_compress_sequential_iter<L: LendingIterator + Clone + Send>(
    basename: impl AsRef<Path> + Send + Sync,
    iter: &mut L,
    num_nodes: usize,
    compression_flags: CompFlags,
    num_threads: usize,
) -> Result<usize>
where
    L: LendingIterator,
    for<'next> Item<'next, L>: Tuple2<_0 = usize>,
    for<'next> <Item<'next, L> as Tuple2>::_1: IntoIterator<Item = usize>,
{
    let basename = basename.as_ref();
    let graph_path = format!("{}.graph", basename.to_string_lossy());
    assert_ne!(num_threads, 0);
    let nodes_per_thread = num_nodes / num_threads;
    let dir = tempdir()?.into_path();
    let tmp_dir = dir.clone();

    std::thread::scope(|s| {
        // collect the handles in vec, otherwise the handles will be dropped
        // in-place calling a join and making the algorithm sequential.
        #[allow(clippy::type_complexity)]
        let mut handles: Vec<Mutex<Option<ScopedJoinHandle<(usize, usize)>>>> = vec![];
        handles.resize_with(num_threads, || Mutex::new(None));
        let handles = Arc::new(handles);

        let cp_flags = &compression_flags;

        // spawn a the thread for the last chunk that will spawn all the previous ones
        // this will be the longest running thread
        let last_thread_id = num_threads - 1;
        // handle the case when this is the only available thread
        let last_file_path = tmp_dir.join(format!("{:016x}.bitstream", last_thread_id));

        log::info!(
            "Spawning the main compression thread {} writing on {} writing from node_id {} to {}",
            last_thread_id,
            last_file_path.to_string_lossy(),
            last_thread_id * nodes_per_thread,
            num_nodes,
        );
        let sub_handles = handles.clone();
        let handle = s.spawn(move || {
            // for the first N - 1 threads, clone the iter and skip to the next
            // splitting point, then start a new compression thread
            for thread_id in 0..num_threads.saturating_sub(1) {
                // the first thread can directly write to the result bitstream
                let file_path = tmp_dir
                    .clone()
                    .join(format!("{:016x}.bitstream", thread_id));

                // spawn the thread
                log::info!(
                    "Spawning compression thread {} writing on {} form node id {} to {}",
                    thread_id,
                    file_path.to_string_lossy(),
                    nodes_per_thread * thread_id,
                    nodes_per_thread * (thread_id + 1),
                );
                // Spawn the thread
                let mut thread_iter = iter.clone();
                let handle = s.spawn(move || {
                    log::info!("Thread {} started", thread_id,);
                    let writer = <BufBitWriter<BE, _>>::new(WordAdapter::new(BufWriter::new(
                        File::create(&file_path).unwrap(),
                    )));
                    let codes_writer = <DynamicCodesWriter<BE, _>>::new(writer, cp_flags);
                    let mut bvcomp = BVComp::new(
                        codes_writer,
                        cp_flags.compression_window,
                        cp_flags.min_interval_length,
                        cp_flags.max_ref_count,
                        nodes_per_thread * thread_id,
                    );
                    let written_bits = bvcomp.extend::<L>(&mut thread_iter).unwrap();
                    log::info!(
                        "Finished Compression thread {} and wrote {} bits bits [{}, {})",
                        thread_id,
                        written_bits,
                        nodes_per_thread * thread_id,
                        nodes_per_thread * (thread_id + 1),
                    );

                    (written_bits, bvcomp.arcs)
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
            let writer = <BufBitWriter<BE, _>>::new(WordAdapter::new(BufWriter::new(
                File::create(last_file_path).unwrap(),
            )));
            let codes_writer = <DynamicCodesWriter<BE, _>>::new(writer, &compression_flags);
            let mut bvcomp = BVComp::new(
                codes_writer,
                compression_flags.compression_window,
                compression_flags.min_interval_length,
                compression_flags.max_ref_count,
                last_thread_id * nodes_per_thread,
            );
            let written_bits = bvcomp.extend(iter).unwrap();

            log::info!(
                "Finished Compression thread {} and wrote {} bits [{}, {})",
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
        let file = File::create(graph_path)?;

        // create hte buffered writer
        let mut result_writer = <BufBitWriter<BE, _>>::new(WordAdapter::new(BufWriter::new(file)));

        let mut result_len = 0;
        let mut total_arcs = 0;
        // glue toghether the bitstreams as they finish, this allows us to do
        // task pipelining for better performance
        for thread_id in 0..num_threads {
            log::info!("Waiting for thread {}", thread_id);
            // wait for the thread to finish
            let (mut bits_to_copy, n_arcs) = loop {
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
            let file_path = dir.clone().join(format!("{:016x}.bitstream", thread_id));
            log::info!(
                "Copying {} [{}, {}) bits from {} to {}",
                bits_to_copy,
                result_len,
                result_len + bits_to_copy,
                file_path.to_string_lossy(),
                basename.to_string_lossy()
            );
            result_len += bits_to_copy;

            let mut reader = <BufBitReader<BE, u64, _>>::new(<WordAdapter<u32, _>>::new(
                BufReader::new(File::open(&file_path).unwrap()),
            ));
            // copy all the data
            while bits_to_copy > 0 {
                let bits = bits_to_copy.min(64);
                let word = reader.read_bits(bits)?;
                result_writer.write_bits(word, bits)?;
                bits_to_copy -= bits;
            }
        }

        log::info!("Flushing the merged Compression bitstream");
        result_writer.flush().unwrap();

        log::info!("Writing the .properties file");
        let properties = compression_flags.to_properties(num_nodes, total_arcs);
        std::fs::write(
            format!("{}.properties", basename.to_string_lossy()),
            properties,
        )?;

        log::info!(
            "Compressed {} arcs into {} bits for {:.4} bits/arc",
            total_arcs,
            result_len,
            result_len as f64 / total_arcs as f64
        );

        // cleanup the temp files
        std::fs::remove_dir_all(dir)?;
        Ok(result_len)
    })
}
