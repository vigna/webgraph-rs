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
#[command(about = "Simplify a BVGraph, i.e. make it undirected and remove duplicates and selfloops", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// The basename of the transposed graph. Defaults to `basename` + `.simple`.
    simplified: Option<String>,

    #[clap(flatten)]
    num_cpus: NumCpusArg,

    #[clap(flatten)]
    pa: PermutationArgs,

    #[clap(flatten)]
    ca: CompressArgs,
}

fn simplify<E: Endianness + 'static>(args: Args) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let simplified = args
        .simplified
        .unwrap_or_else(|| args.basename.clone() + ".simple");

    let seq_graph =
        webgraph::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.basename)
            .endianness::<E>()
            .load()?;

    // transpose the graph
    let sorted = webgraph::algo::simplify(&seq_graph, args.pa.batch_size).unwrap();

    let target_endianness = args.ca.endianess.clone();
    BVComp::parallel_endianness(
        simplified,
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
        .init()
        .unwrap();

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => simplify::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => simplify::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}
