/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
use sux::prelude::BitVec;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "bf-visit2", about = "Benchmarks a breadth-first visit.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
    /// Static dispatch (default BvGraph parameters).
    #[arg(short = 'S', long = "static")]
    pub _static: bool,
    /// Number of repeats (usually to warm up the cache or memory mapping).
    #[arg(short = 'R', long, default_value_t = 1)]
    pub repeats: usize,

    #[clap(long, default_value = "false")]
    /// Whether to use mmap for the graph, otherwise it will be load in memory
    mmap: bool,
}

pub fn main(_global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    let config = BvGraph::with_basename(&args.src);

    for _ in 0..args.repeats {
        match (get_endianness(&args.src)?.as_str(), args.mmap) {
            #[cfg(feature = "be_bins")]
            (BE::NAME, true) => match args._static {
                true => visit(
                    config
                        .clone()
                        .mode::<Mmap>()
                        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                        .endianness::<BE>()
                        .dispatch::<Static>()
                        .load()?,
                )?,
                false => visit(config.clone().endianness::<BE>().load()?)?,
            },
            #[cfg(feature = "be_bins")]
            (BE::NAME, false) => match args._static {
                true => visit(
                    config
                        .clone()
                        .mode::<LoadMmap>()
                        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                        .endianness::<BE>()
                        .dispatch::<Static>()
                        .load()?,
                )?,
                false => visit(config.clone().endianness::<BE>().load()?)?,
            },
            #[cfg(feature = "le_bins")]
            (LE::NAME, true) => match args._static {
                true => visit(
                    config
                        .clone()
                        .mode::<Mmap>()
                        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                        .endianness::<LE>()
                        .dispatch::<Static>()
                        .load()?,
                )?,
                false => visit(config.clone().endianness::<LE>().load()?)?,
            },
            #[cfg(feature = "le_bins")]
            (LE::NAME, false) => match args._static {
                true => visit(
                    config
                        .clone()
                        .mode::<LoadMmap>()
                        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                        .endianness::<LE>()
                        .dispatch::<Static>()
                        .load()?,
                )?,
                false => visit(config.clone().endianness::<LE>().load()?)?,
            },
            (e, _) => panic!("Unknown endianness: {}", e),
        };
    }
    Ok(())
}

fn visit(graph: BvGraph<impl RandomAccessDecoderFactory>) -> Result<()> {
    let num_nodes = graph.num_nodes();
    let mut seen = BitVec::new(num_nodes);
    let mut visited = BitVec::new(num_nodes);
    let mut frontier = Vec::new();
    let mut next_frontier = Vec::new();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("node")
        .local_speed(true)
        .expected_updates(Some(num_nodes));
    pl.start("Visiting graph...");

    for start in 0..num_nodes {
        if seen[start] {
            continue;
        }

        frontier.push(start as _);
        seen.set(start, true);

        while !frontier.is_empty() {
            while !frontier.is_empty() {
                pl.light_update();
                let current_node = frontier.pop().unwrap();
                for succ in graph.filtered_bfs_successors(current_node, |node| visited[node]) {
                    if !seen[succ] {
                        next_frontier.push(succ);
                        seen.set(succ as _, true);
                    }
                }
                visited.set(current_node as _, true);
            }
            std::mem::swap(&mut frontier, &mut next_frontier);
            next_frontier.clear();
        }
    }

    pl.done();

    Ok(())
}
