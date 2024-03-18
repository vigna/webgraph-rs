/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::utils::*;
use crate::graphs::arc_list_graph;
use crate::labels::Left;
use crate::prelude::*;
use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use epserde::prelude::*;
use lender::*;
use std::io::{BufReader, Read};
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "perm";

#[derive(Args, Debug)]
#[command(about = "Apply a permutation to a bvgraph.", long_about = None)]
struct CliArgs {
    /// The basename of the source graph.
    source: PathBuf,
    /// The basename of the destination graph.
    dest: PathBuf,
    /// The permutation.
    perm: PathBuf,

    #[arg(short = 'e', long, default_value_t = false)]
    /// Load the permutation from ε-serde format.
    epserde: bool,

    #[arg(short = 'o', long, default_value_t = false)]
    /// Build the offsets while compressing the graph .
    build_offsets: bool,

    #[clap(flatten)]
    num_cpus: NumCpusArg,

    #[clap(flatten)]
    pa: PermutationArgs,

    #[clap(flatten)]
    ca: CompressArgs,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

fn permute<E: Endianness>(
    args: CliArgs,
    graph: &impl SequentialGraph,
    perm: &[usize],
    num_nodes: usize,
) -> Result<()> {
    // create a stream where to dump the sorted pairs
    let mut sort_pairs = SortPairs::new(args.pa.batch_size, temp_dir(&args.pa.temp_dir)).unwrap();

    // dump the paris
    PermutedGraph { graph, perm }.iter().for_each(|(x, succ)| {
        succ.into_iter().for_each(|s| {
            sort_pairs.push(x, s).unwrap();
        })
    });
    // get a graph on the sorted data
    let edges = sort_pairs
        .iter()
        .context("Could not read arcs")?
        .map(|(src, dst, _)| (src, dst));
    let g = Left(arc_list_graph::ArcListGraph::new(num_nodes, edges));
    // compress it
    let target_endianness = args.ca.endianess.clone();
    BVComp::parallel_endianness(
        args.dest,
        &g,
        g.num_nodes(),
        args.ca.into(),
        args.num_cpus.num_cpus,
        temp_dir(args.pa.temp_dir),
        &target_endianness.unwrap_or_else(|| E::NAME.into()),
    )?;

    Ok(())
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.source)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => perm_impl::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => perm_impl::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn perm_impl<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
    for<'a> BufBitReader<E, MemWordReader<u32, &'a MmapBackend<u32>>>: CodeRead<E> + BitSeek,
{
    let mut glob_pl = ProgressLogger::default();
    glob_pl.display_memory(true).item_name("node");
    glob_pl.start("Permuting the graph...");
    // TODO!: check that batchsize fits in memory, and that print the maximum
    // batch_size usable

    let graph = crate::graphs::bvgraph::sequential::BVGraphSeq::with_basename(&args.source)
        .endianness::<E>()
        .load()
        .with_context(|| format!("Could not read graph from {}", args.source.display()))?;

    let num_nodes = graph.num_nodes();
    // read the permutation

    if args.epserde {
        let perm = <Vec<usize>>::mmap(&args.perm, deser::Flags::default())
            .with_context(|| format!("Could not mmap permutation from {}", args.perm.display()))?;
        permute::<E>(args, &graph, perm.as_ref(), num_nodes)
            .context("Could not compute or write permutation")?;
    } else {
        let mut file = BufReader::new(
            std::fs::File::open(&args.perm)
                .with_context(|| format!("Could not open permutation {}", args.perm.display()))?,
        );
        let mut perm = Vec::with_capacity(num_nodes);
        let mut buf = [0; core::mem::size_of::<usize>()];

        let mut perm_pl = ProgressLogger::default();
        perm_pl.display_memory(true).item_name("node");
        perm_pl.start("Reading the permutation...");

        for _ in 0..num_nodes {
            file.read_exact(&mut buf).with_context(|| {
                format!("Could not read permutation from {}", args.perm.display())
            })?;
            perm.push(usize::from_be_bytes(buf));
            perm_pl.light_update();
        }
        perm_pl.done();
        permute::<E>(args, &graph, perm.as_ref(), num_nodes)
            .context("Could not compute or write permutation")?;
    }
    glob_pl.done();

    Ok(())
}
