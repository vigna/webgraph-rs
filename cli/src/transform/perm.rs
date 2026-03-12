/*
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
#[command(name = "perm", about = "Permutes a graph in the BV format according to a given permutation.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub src: PathBuf,
    /// The basename of the permuted graph.​
    pub dst: PathBuf,

    /// The path to the permutation to apply to the graph.​
    pub permutation: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.​
    pub fmt: IntSliceFormat,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[arg(long)]
    /// Use the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => perm::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => perm::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn perm<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let use_dcf = args.dcf;
    let src = args.src.clone();
    let target_endianness = args.ca.endianness.clone();
    let memory_usage = args.memory_usage.memory_usage;

    let dir = Builder::new().prefix("transform_perm_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    let loaded = args.fmt.load(&args.permutation)?;
    dispatch_int_slice!(loaded, |perm| {
        crate::to::bvgraph::compress_with_perm::<E, _>(
            thread_pool,
            builder,
            &src,
            target_endianness,
            memory_usage,
            use_dcf,
            perm,
        )
    })
}
