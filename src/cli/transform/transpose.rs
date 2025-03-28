/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::*;
use crate::prelude::*;
use anyhow::Result;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use std::path::PathBuf;
use tempfile::Builder;

#[derive(Parser, Debug)]
#[command(name = "transpose", about = "Transposes a BvGraph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
    /// The basename of the transposed graph.
    pub dst: PathBuf,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub batch_size: BatchSizeArg,

    #[clap(flatten)]
    pub ca: CompressArgs,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => transpose::<BE>(global_args, args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => transpose::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn transpose<E: Endianness>(_global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let thread_pool = crate::cli::get_thread_pool(args.num_threads.num_threads);

    // TODO!: speed it up by using random access graph if possible
    let seq_graph = crate::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.src)
        .endianness::<E>()
        .load()?;

    // transpose the graph
    let sorted = crate::transform::transpose(&seq_graph, args.batch_size.batch_size).unwrap();

    let target_endianness = args.ca.endianness.clone();
    let dir = Builder::new().prefix("transform_transpose_").tempdir()?;
    BvComp::parallel_endianness(
        &args.dst,
        &sorted,
        sorted.num_nodes(),
        args.ca.into(),
        &thread_pool,
        dir,
        &target_endianness.unwrap_or_else(|| E::NAME.into()),
    )?;

    Ok(())
}
