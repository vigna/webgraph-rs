/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::*;
use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use std::path::PathBuf;
use tempfile::Builder;

pub const COMMAND_NAME: &str = "transpose";

#[derive(Args, Debug)]
#[command(about = "Transposes a BvGraph.", long_about = None)]
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

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => transpose::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => transpose::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn transpose<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodesRead<E> + BitSeek,
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
