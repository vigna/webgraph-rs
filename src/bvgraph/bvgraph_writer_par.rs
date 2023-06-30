use super::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use tempfile::tempdir;

fn compress_iter<
    I: Iterator<Item = (usize, J)> + Clone + ExactSizeIterator + Send,
    J: Iterator<Item = usize>,
>(
    file_path: &Path,
    cpflags: CompFlags,
    thread_iter: I,
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
    );
    let written_bits = bvcomp.extend(thread_iter).unwrap();
    Ok(written_bits)
}

pub fn parallel_compress_sequential_iter<
    I: Iterator<Item = (usize, J)> + Clone + ExactSizeIterator + Send,
    J: Iterator<Item = usize>,
>(
    mut iter: I,
    compression_flags: CompFlags,
) -> Result<()> {
    let num_threads = rayon::current_num_threads();
    let nodes_per_thread = iter.len() / num_threads;
    let dir = tempdir()?.into_path();

    std::thread::scope(|s| {
        let mut threads = Vec::with_capacity(num_threads);

        // for the first N - 1 threads, clone the iter and skip to the next
        // splitting point, then start a new compression thread
        for thread_id in 0..num_threads.saturating_sub(1) {
            let thread_iter = iter.clone().take(nodes_per_thread);
            let file_path = dir.clone().join(format!("{:016x}.bitstream", thread_id));
            let cpflags = compression_flags.clone();
            // spawn the thread
            threads.push((
                file_path.clone(),
                s.spawn(move || compress_iter(&file_path, cpflags, thread_iter)),
            ));

            // skip the next nodes_per_thread nodes
            for _ in 0..nodes_per_thread {
                iter.next();
            }
        }

        let file_path = dir.clone().join(format!("{:016x}.bitstream", num_threads));
        // spawn the last thread without skipping and cloning
        threads.push((
            file_path.clone(),
            s.spawn(move || compress_iter(&file_path, compression_flags, iter)),
        ));

        // setup the final bitstream
        let result_path = "result.bitstream";
        let mut writer = <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(BufWriter::new(
            File::create(&result_path).unwrap(),
        )));

        // glue toghether the bitstreams as they finish, this allows us to do
        // task pipelining for better performance
        for (file_path, thread_handle) in threads {
            let bits_to_copy = thread_handle.join().unwrap().unwrap();
            // create a bitstream reader
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

        writer.flush().unwrap();
    });
    Ok(())
}
