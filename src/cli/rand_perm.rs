/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use epserde::ser::Serialize;
use rand::prelude::SliceRandom;
use std::io::prelude::*;
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "rand-perm";

#[derive(Args, Debug)]
#[command(about = "Create a random permutation for a given graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    source: PathBuf,
    /// The permutation.
    perm: PathBuf,

    #[arg(short = 'e', long)]
    /// Load the permutation from ε-serde format.
    epserde: bool,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.source)?.as_str() {
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
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn rand_perm<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.source)
        .endianness::<E>()
        .load()
        .with_context(|| format!("Could not read graph from {}", args.source.display()))?;

    let mut rng = rand::thread_rng();
    let mut perm = (0..graph.num_nodes()).collect::<Vec<_>>();
    perm.shuffle(&mut rng);

    if args.epserde {
        perm.store(&args.perm)
            .with_context(|| format!("Could not store permutation to {}", args.perm.display()))?;
    } else {
        let mut file =
            std::io::BufWriter::new(std::fs::File::create(&args.perm).with_context(|| {
                format!("Could not create permutation at {}", args.perm.display())
            })?);
        for perm in perm {
            file.write_all(&perm.to_be_bytes()).with_context(|| {
                format!("Could not write permutation to {}", args.perm.display())
            })?;
        }
    }

    Ok(())
}
