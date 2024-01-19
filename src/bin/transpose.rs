/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use webgraph::prelude::*;
use dsi_bitstream::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Transpose a BVGraph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// The basename of the transposed graph. Defaults to `basename` + `.t`.
    transposed: Option<String>,

    #[clap(flatten)]
    num_cpus: NumCpusArg,

    #[clap(flatten)]
    pa: PermutationArgs,

    #[clap(flatten)]
    ca: CompressArgs,
}

fn transpose<E: Endianness + 'static>(args: Args) -> Result<()> 
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: ZetaRead<E> + DeltaRead<E> + GammaRead<E> + BitSeek
{
    let transposed = args
        .transposed
        .unwrap_or_else(|| args.basename.clone() + ".t");

    let seq_graph = webgraph::graph::bvgraph::load_seq::<E, _>(&args.basename)?;

    // transpose the graph
    let sorted = webgraph::algorithms::transpose(&seq_graph, args.pa.batch_size).unwrap();
    // compress the transposed graph
    parallel_compress_sequential_iter(
        transposed,
        &sorted,
        sorted.num_nodes(),
        args.ca.into(),
        args.num_cpus.num_cpus,
        temp_dir(args.pa.temp_dir),
    )?;

    Ok(())
}

pub fn main() -> Result<()> {
    let args = Args::parse();
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(feature = "be_bins", not(any(feature = "be_bins", feature = "le_bins"))))]
        BE::NAME => transpose::<BE>(args),
        #[cfg(any(feature = "le_bins", not(any(feature = "be_bins", feature = "le_bins"))))]
        LE::NAME => transpose::<LE>(args),
        _ => panic!("Unknown endianness"),
    }
}
