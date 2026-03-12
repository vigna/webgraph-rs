/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{IntSliceFormat, LogIntervalArg, create_parent_dir};
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "bfs", about = "Computes the permutation induced by a breadth-first visit.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    /// The filename of the permutation.​
    pub perm: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.​
    pub fmt: IntSliceFormat,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.perm)?;

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => bfs::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => bfs::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn bfs<E: Endianness + 'static + Send + Sync>(args: CliArgs) -> Result<()>
where
    MemoryFactory<E, MmapHelper<u32>>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, LoadMmap>: BitSeek,
{
    // load the graph
    let graph = BvGraph::with_basename(&args.basename)
        .mode::<LoadMmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .endianness::<E>()
        .load()?;

    let mut pl = progress_logger![
        display_memory = true,
        item_name = "nodes",
        expected_updates = Some(graph.num_nodes()),
    ];
    if let Some(duration) = args.log_interval.log_interval {
        pl.log_interval(duration);
    }

    // create the permutation
    let mut perm = vec![0; graph.num_nodes()];
    pl.start("Computing BFS permutation...");
    let mut visit = webgraph::visits::breadth_first::Seq::new(&graph);
    for (i, event) in visit.into_iter().enumerate() {
        perm[event.node] = i;
        pl.light_update();
    }
    pl.done();

    args.fmt.store(&args.perm, &perm, None)?;

    Ok(())
}
