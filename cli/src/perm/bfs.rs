/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{create_parent_dir, GlobalArgs};
use common_traits::ToBytes;
use webgraph::prelude::*;
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::Serialize;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "bfs", about = "Computes the permutation induced by a breadth-first visit.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    /// The filename of the permutation in binary big-endian format.
    pub perm: PathBuf,

    #[arg(short, long)]
    /// Save the permutation in ε-serde format.
    pub epserde: bool,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.perm)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => bfs::<BE>(global_args, args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => bfs::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn bfs<E: Endianness + 'static + Send + Sync>(
    global_args: GlobalArgs,
    args: CliArgs,
) -> Result<()>
where
    MemoryFactory<E, MmapHelper<u32>>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, LoadMmap>: BitSeek,
{
    // load the graph
    let graph = BvGraph::with_basename(&args.src)
        .mode::<LoadMmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .endianness::<E>()
        .load()?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("nodes")
        .expected_updates(Some(graph.num_nodes()));
    if let Some(duration) = global_args.log_interval {
        pl.log_interval(duration);
    }

    // create the permutation
    let mut perm = vec![0; graph.num_nodes()];
    pl.start("Computing BFS permutation...");
    for (i, node_id) in crate::algo::BfsOrder::new(&graph).enumerate() {
        perm[node_id] = i;
        pl.light_update();
    }
    pl.done();

    if args.epserde {
        perm.store(&args.perm)
            .with_context(|| format!("Could not write permutation to {}", args.perm.display()))?;
    } else {
        let mut file = std::fs::File::create(&args.perm)
            .with_context(|| format!("Could not create permutation at {}", args.perm.display()))?;
        let mut buf = BufWriter::new(&mut file);
        pl.start(format!("Storing the nodes to {}", args.perm.display()));
        for word in perm.iter() {
            buf.write_all(&word.to_be_bytes()).with_context(|| {
                format!("Could not write permutation to {}", args.perm.display())
            })?;
            pl.light_update();
        }
        pl.done();
    }
    Ok(())
}
