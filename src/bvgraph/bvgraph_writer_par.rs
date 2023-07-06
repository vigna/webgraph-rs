use super::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
//use tempfile::tempdir;

pub fn parallel_compress_sequential_iter<
    P: AsRef<Path> + Send + Sync,
    I: Iterator<Item = (usize, J)> + Clone + ExactSizeIterator + Send,
    J: Iterator<Item = usize>,
>(
    result_bitstream_path: P,
    mut iter: I,
    compression_flags: CompFlags,
) -> Result<()> {
    let result_bitstream_path = result_bitstream_path.as_ref();
    let num_threads = rayon::current_num_threads();
    assert_ne!(num_threads, 0);
    let num_nodes = iter.len();
    let nodes_per_thread = num_nodes / num_threads;
    let dir = std::path::PathBuf::from("tests/data/"); //tempdir()?.into_path();
    let tmp_dir = dir.clone();
    // vec of atomic usize where we store the size in bits of the compressed
    // portion of the graph, usize::MAX represent that the task is not finished
    let semaphores: Vec<_> = (0..num_threads)
        .map(|_| AtomicUsize::new(usize::MAX))
        .collect::<Vec<_>>();
    // borrow to make the compiler happy
    let semaphores_ref = &semaphores;

    std::thread::scope(|s| {
        // spawn a the thread for the last chunk that will spawn all the previous ones
        // this will be the longest running thread
        let last_thread_id = num_threads - 1;
        // handle the case when this is the only available thread
        let last_file_path = tmp_dir
            .clone()
            .join(format!("{:016x}.bitstream", last_thread_id));

        log::info!(
            "Spawning the main compression thread {} writing on {} writing from node_id {} to {}",
            num_threads - 1,
            last_file_path.to_string_lossy(),
            nodes_per_thread * (num_threads - 1),
            num_nodes,
        );
        s.spawn(move || {
            // for the first N - 1 threads, clone the iter and skip to the next
            // splitting point, then start a new compression thread
            for thread_id in 0..num_threads.saturating_sub(1) {
                // the first thread can directly write to the result bitstream
                let file_path = tmp_dir
                    .clone()
                    .join(format!("{:016x}.bitstream", thread_id));

                let cpflags = compression_flags.clone();
                // spawn the thread
                log::info!(
                    "Spawning compression thread {} writing on {} form node id {} to {}",
                    thread_id,
                    file_path.to_string_lossy(),
                    nodes_per_thread * thread_id,
                    nodes_per_thread * (thread_id + 1),
                );
                // Spawn the thread
                let thread_iter = iter.clone().take(nodes_per_thread);
                s.spawn(move || {
                    let writer = <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(
                        BufWriter::new(File::create(&file_path).unwrap()),
                    ));
                    let codes_writer = <DynamicCodesWriter<BE, _>>::new(writer, &cpflags);
                    let mut bvcomp = BVComp::new(
                        codes_writer,
                        cpflags.compression_window,
                        cpflags.min_interval_length,
                        cpflags.max_ref_count,
                        thread_id * nodes_per_thread,
                    );
                    let written_bits = bvcomp.extend(thread_iter).unwrap();

                    log::info!(
                        "Finished Compression thread {} and wrote {} bits",
                        thread_id,
                        written_bits
                    );
                    semaphores_ref[thread_id].store(written_bits, Ordering::Release);
                });

                // skip the next nodes_per_thread nodes
                for _ in 0..nodes_per_thread {
                    iter.next();
                }
            }

            // complete the last chunk
            let writer = <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(BufWriter::new(
                File::create(&last_file_path).unwrap(),
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
                "Finished Compression thread {} and wrote {} bits",
                last_thread_id,
                written_bits
            );
            semaphores_ref[last_thread_id].store(written_bits, Ordering::Release);
        });

        // setup the final bitstream from the end, because the first thread
        // already wrote the first chunk
        let file = File::create(&result_bitstream_path)?;

        // create hte buffered writer
        let mut result_writer =
            <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(BufWriter::new(file)));

        // glue toghether the bitstreams as they finish, this allows us to do
        // task pipelining for better performance
        for thread_id in 0..num_threads {
            log::info!("Waiting for thread {}", thread_id);
            // wait for the thread to finish
            let mut bits_to_copy = loop {
                let bits_to_copy = semaphores[thread_id].load(Ordering::Acquire);
                if bits_to_copy != usize::MAX {
                    break bits_to_copy;
                }
                std::thread::yield_now();
                std::thread::sleep(std::time::Duration::from_millis(100));
            };
            // compute the path of the bitstream created by this thread
            let file_path = dir.clone().join(format!("{:016x}.bitstream", thread_id));
            log::info!(
                "Copying {} bits from {} to {}",
                bits_to_copy,
                file_path.to_string_lossy(),
                result_bitstream_path.to_string_lossy()
            );

            let mut reader = <BufferedBitStreamRead<BE, u64, _>>::new(<FileBackend<u32, _>>::new(
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
        Ok(())
    })
}
