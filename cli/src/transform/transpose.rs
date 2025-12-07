/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;
use tempfile::Builder;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "transpose", about = "Transposes a BvGraph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
    /// The basename of the transposed graph.
    pub dst: PathBuf,

    #[arg(short, long)]
    /// Use the parallel compressor.
    pub parallel: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => {
            if args.parallel {
                par_transpose::<BE>(global_args, args)
            } else {
                transpose::<BE>(global_args, args)
            }
        }
        #[cfg(feature = "le_bins")]
        LE::NAME => {
            if args.parallel {
                par_transpose::<LE>(global_args, args)
            } else {
                transpose::<LE>(global_args, args)
            }
        }
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn transpose<E: Endianness>(_global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    // TODO!: speed it up by using random access graph if possible
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    // transpose the graph
    let sorted =
        webgraph::transform::transpose(&seq_graph, args.memory_usage.memory_usage).unwrap();

    let target_endianness = args.ca.endianness.clone();
    let dir = Builder::new().prefix("transform_transpose_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    thread_pool.install(|| {
        builder.par_comp_lenders_endianness(
            &sorted,
            sorted.num_nodes(),
            &target_endianness.unwrap_or_else(|| BE::NAME.into()),
        )
    })?;

    Ok(())
}

pub fn par_transpose<E: Endianness>(_global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> <MmapHelper<u32> as CodesReaderFactory<E>>::CodesReader<'a>:
        BitSeek + Clone + Send + Sync,
    BufBitReader<E, WordAdapter<u32, BufReader<std::fs::File>>>: BitRead<E>,
    BufBitWriter<E, WordAdapter<usize, BufWriter<std::fs::File>>>: CodesWrite<E>,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    let seq_graph = webgraph::graphs::bvgraph::BvGraph::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    // transpose the graph
    let split = webgraph::transform::transpose_split(&seq_graph, args.memory_usage.memory_usage)?;

    // Convert to (node, lender) pairs
    let pairs: Vec<_> = split.into();

    let dir = Builder::new().prefix("transform_transpose_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    thread_pool
        .install(|| builder.par_comp_lenders::<E, _>(pairs.into_iter(), seq_graph.num_nodes()))?;
    Ok(())
}
