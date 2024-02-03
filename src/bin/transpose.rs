/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Transpose a BVGraph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// The basename of the transposed graph. Defaults to `basename` + `-t`.
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
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let transposed = args
        .transposed
        .unwrap_or_else(|| args.basename.clone() + "-t");

    let seq_graph =
        webgraph::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.basename)
            .endianness::<E>()
            .load()?;

    // transpose the graph
    let sorted = webgraph::algo::transpose(&seq_graph, args.pa.batch_size).unwrap();

    let target_endianness = args.ca.endianess.clone();
    BVComp::parallel_endianness(
        transposed,
        &sorted,
        sorted.num_nodes(),
        args.ca.into(),
        args.num_cpus.num_cpus,
        temp_dir(args.pa.temp_dir),
        &target_endianness.unwrap_or_else(|| E::NAME.into()),
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
