use super::*;
use anyhow::Result;
use dsi_bitstream::prelude::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use tempfile::tempdir;

macro_rules! parallel_compress_iter {
    (
        $basename: expr,
        $num_nodes: expr,
        $chunks: expr,
        $compression_flags: expr,
        $num_threads: expr
    ) => {{
    let basename = $basename.as_ref();
    let num_nodes = $num_nodes;
    let num_threads = $num_threads;
    let compression_flags = $compression_flags;
    let graph_path = format!("{}.graph", basename.to_string_lossy());
    assert_ne!(num_threads, 0);
    let nodes_per_thread = num_nodes / num_threads;
    let dir = tempdir()?.into_path();
    let tmp_dir = dir.clone();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    pool.install(|| {
        let cp_flags = &compression_flags;

        let thread_results: Vec<(_, usize, _)> = $chunks
            .map(|(thread_id, thread_iter)| {

                let file_path = tmp_dir
                    .clone()
                    .join(format!("{:016x}.bitstream", thread_id));

                log::info!(
                    "Spawning compression thread {} writing on {} form node id {} to {}",
                    thread_id,
                    file_path.to_string_lossy(),
                    nodes_per_thread * thread_id,
                    nodes_per_thread * (thread_id + 1),
                );

                    let writer = <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(
                        BufWriter::new(File::create(&file_path).unwrap()),
                    ));
                    let codes_writer = <DynamicCodesWriter<BE, _>>::new(writer, cp_flags);
                    let mut bvcomp = BVComp::new(
                        codes_writer,
                        cp_flags.compression_window,
                        cp_flags.min_interval_length,
                        cp_flags.max_ref_count,
                        nodes_per_thread * thread_id,
                    );

                    let written_bits = bvcomp.extend(thread_iter.into_iter()).unwrap();

                    log::info!(
                        "Finished Compression thread {} and wrote {} bits bits [{}, {})",
                        thread_id,
                        written_bits,
                        nodes_per_thread * thread_id,
                        nodes_per_thread * (thread_id + 1),
                    );
                    (thread_id, written_bits, bvcomp.arcs)
            })
            .collect();

        // setup the final bitstream from the end, because the first thread
        // already wrote the first chunk
        let file = File::create(graph_path)?;

        // create hte buffered writer
        let mut result_writer =
            <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(BufWriter::new(file)));

        let mut result_len = 0;
        let mut total_arcs = 0;
        // glue toghether the bitstreams as they finish, this allows us to do
        // task pipelining for better performance
        for (thread_id, mut bits_to_copy, n_arcs) in thread_results {
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
    }}
}

/// Compress an iterator of nodes and successors in parallel and return the
/// lenght in bits of the produced file
pub fn parallel_compress_sequential_iter<
    P: AsRef<Path> + Send + Sync,
    I: ExactSizeIterator<Item = (usize, J)> + Send,
    J: Iterator<Item = usize>,
>(
    basename: P,
    iter: I,
    compression_flags: CompFlags,
    num_threads: usize,
) -> Result<usize> {
    use itertools::Itertools;
    let num_nodes = iter.len();
    let nodes_per_thread = num_nodes / num_threads;
    parallel_compress_iter!(
        basename,
        num_nodes,
        iter.chunks(nodes_per_thread).into_iter().enumerate(),
        compression_flags,
        num_threads
    )
}

/// Compress an iterator of nodes and successors in parallel and return the
/// lenght in bits of the produced file
pub fn parallel_compress_parallel_iter<
    P: AsRef<Path> + Send + Sync,
    I: IndexedParallelIterator<Item = (usize, J)>,
    J: Iterator<Item = usize>,
>(
    basename: P,
    iter: I,
    compression_flags: CompFlags,
    num_threads: usize,
) -> Result<usize> {
    let num_nodes = iter.len();
    let nodes_per_thread = num_nodes / num_threads;
    parallel_compress_iter!(
        basename,
        num_nodes,
        iter.chunks(nodes_per_thread).enumerate(),
        compression_flags,
        num_threads
    )
}
