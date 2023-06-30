use super::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

fn compress_iter<
    I: Iterator<Item = (usize, J)> + Clone + ExactSizeIterator + Send,
    J: Iterator<Item = usize>,
>(
    file_path: &Path,
    cpflags: CompFlags,
    thread_iter: I,
    start_node: usize,
) -> Result<usize> {
    let writer = <DynamicCodesWriter<BE, _>>::new(
        <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(BufWriter::new(
            File::create(&file_path).unwrap(),
        ))),
        &cpflags,
    );
    let mut bvcomp = BVComp::new(
        writer,
        cpflags.compression_window,
        cpflags.min_interval_length,
        cpflags.max_ref_count,
        start_node,
    );
    let written_bits = bvcomp.extend(thread_iter).unwrap();
    Ok(written_bits)
}

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
    let num_threads = 3; //rayon::current_num_threads();
    assert_ne!(num_threads, 0);
    let nodes_per_thread = iter.len() / num_threads;
    let dir = tempdir()?.into_path();
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
        let last_file_path = tmp_dir
            .clone()
            .join(format!("{:016x}.bitstream", last_thread_id));
        log::info!(
            "Spawning the main compression thread {} writing on {}",
            num_threads - 1,
            last_file_path.to_string_lossy(),
        );
        s.spawn(move || {
            // for the first N - 1 threads, clone the iter and skip to the next
            // splitting point, then start a new compression thread
            for thread_id in 0..num_threads.saturating_sub(1) {
                // the first thread can directly write to the result bitstream
                let file_path = if thread_id == 0 {
                    result_bitstream_path.to_path_buf()
                } else {
                    tmp_dir
                        .clone()
                        .join(format!("{:016x}.bitstream", thread_id))
                };

                let cpflags = compression_flags.clone();
                // spawn the thread
                log::info!(
                    "Spawning compression thread {} writing on {}",
                    thread_id,
                    file_path.to_string_lossy()
                );
                // Spawn the thread
                let thread_iter = iter.clone().take(nodes_per_thread);
                s.spawn(move || {
                    let res = compress_iter(
                        &file_path,
                        cpflags,
                        thread_iter,
                        thread_id * nodes_per_thread,
                    )
                    .unwrap();
                    log::info!("Finished Compression thread {}", thread_id);
                    semaphores_ref[thread_id].store(res, Ordering::Release);
                });

                // skip the next nodes_per_thread nodes
                for _ in 0..nodes_per_thread {
                    iter.next();
                }
            }

            // spawn the last thread without skipping and cloning
            semaphores_ref[last_thread_id].store(
                compress_iter(
                    &last_file_path,
                    compression_flags,
                    iter,
                    last_thread_id * nodes_per_thread,
                )
                .unwrap(),
                Ordering::Release,
            );
            log::info!("Finished Compression thread {}", last_thread_id);
        });

        // wait for the thread_0 to finish
        log::info!("Waiting for thread 0");
        loop {
            let bits_to_copy = semaphores[0].load(Ordering::Acquire);
            if bits_to_copy != usize::MAX {
                break;
            }
            std::thread::yield_now();
        }

        // setup the final bitstream from the end, because the first thread
        // already wrote the first chunk
        let mut file = File::options()
            .read(true)
            .write(true)
            .open(&result_bitstream_path)
            .unwrap();
        file.seek(std::io::SeekFrom::End(0)).unwrap();
        let mut writer =
            <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(BufWriter::new(file)));

        // glue toghether the bitstreams as they finish, this allows us to do
        // task pipelining for better performance
        for thread_id in 0..num_threads {
            log::info!("Waiting for {}", thread_id);
            // wait for the thread to finish
            let bits_to_copy = loop {
                let bits_to_copy = semaphores[thread_id].load(Ordering::Acquire);
                if bits_to_copy != usize::MAX {
                    break bits_to_copy;
                }
                std::thread::yield_now();
                std::thread::sleep(std::time::Duration::from_millis(100));
            };
            // the first thread writes inplace in the result
            if thread_id == 0 {
                continue;
            }
            // create a bitstream reader
            let file_path = dir.clone().join(format!("{:016x}.bitstream", thread_id));
            let mut reader = <BufferedBitStreamRead<BE, u64, _>>::new(<FileBackend<u32, _>>::new(
                BufReader::new(File::open(&file_path).unwrap()),
            ));

            // copy at chunks of the loading word size
            for _ in 0..(bits_to_copy / 32) {
                let word = reader.read_bits(32).unwrap();
                writer.write_bits(word, 32).unwrap();
            }

            // and write the last bits
            let reminder = bits_to_copy % 32;
            let word = reader.read_bits(reminder).unwrap();
            writer.write_bits(word, reminder).unwrap();
        }

        log::info!("Flushing the merged Compression bitstream");
        writer.flush().unwrap();
        Ok(())
    })
}
