/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use std::path::PathBuf;
use tempfile::Builder;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "transpose", about = "Transposes a graph in the BV format.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub src: PathBuf,
    /// The basename of the transposed graph.​
    pub dst: PathBuf,

    #[arg(short, long)]
    /// Uses the sequential algorithm (does not need offsets).​
    pub sequential: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[arg(long, conflicts_with = "sequential")]
    /// Uses the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,

    #[clap(flatten)]
    pub ca: CompressArgs,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => {
            if args.sequential {
                transpose::<BE>(args)
            } else {
                par_transpose::<BE>(args)
            }
        }
        #[cfg(feature = "le_bins")]
        LE::NAME => {
            if args.sequential {
                transpose::<LE>(args)
            } else {
                par_transpose::<LE>(args)
            }
        }
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn transpose<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    // TODO!: speed it up by using random access graph if possible
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    // transpose the graph
    let sorted = webgraph::transform::transpose(&seq_graph, args.memory_usage.memory_usage)?;

    let target_endianness = args.ca.endianness.clone().unwrap_or_else(|| E::NAME.into());
    let dir = Builder::new().prefix("transform_transpose_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    // Use uniform cutpoints for compression of the transposed graph
    // (the source DCF does not match the transpose's degree distribution)
    thread_pool.install(|| par_comp!(builder, &sorted, target_endianness))?;

    Ok(())
}

pub fn par_transpose<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    let graph = webgraph::graphs::bvgraph::BvGraph::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    let cp = crate::cutpoints(
        &args.src,
        graph.num_nodes(),
        graph.num_arcs_hint(),
        args.dcf,
    )?;

    // transpose the graph
    let split =
        webgraph::transform::transpose_split(&graph, args.memory_usage.memory_usage, Some(cp))?;
    let sorted = SortedGraph::from_parts(split.boundaries, split.iters);

    let target_endianness = args.ca.endianness.clone().unwrap_or_else(|| E::NAME.into());
    let dir = Builder::new().prefix("transform_transpose_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    thread_pool.install(|| par_comp!(builder, &sorted, target_endianness))?;
    Ok(())
}
