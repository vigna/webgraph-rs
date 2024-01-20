/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use epserde::ser::Serialize;
use rand::prelude::SliceRandom;
use std::io::prelude::*;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Create a random permutation for a given graph", long_about = None)]
struct Args {
    /// The basename of the graph.
    source: String,
    /// The permutation.
    perm: String,

    #[arg(short = 'e', long)]
    /// Load the permutation from ε-serde format.
    epserde: bool,
}

fn rand_perm<E: Endianness + 'static>(args: Args) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let graph = webgraph::graph::bvgraph::load_seq::<E, _>(&args.source)?;

    let mut rng = rand::thread_rng();
    let mut perm = (0..graph.num_nodes()).collect::<Vec<_>>();
    perm.shuffle(&mut rng);

    if args.epserde {
        perm.store(&args.perm)?;
    } else {
        let mut file = std::io::BufWriter::new(std::fs::File::create(args.perm)?);
        for perm in perm {
            file.write_all(&perm.to_be_bytes())?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    match get_endianess(&args.source)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => rand_perm::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => rand_perm::<LE>(args),
        _ => panic!("Unknown endianness"),
    }
}
