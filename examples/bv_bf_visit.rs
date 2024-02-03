/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use bitvec::*;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use std::collections::VecDeque;
use webgraph::prelude::*;
#[derive(Parser, Debug)]
#[command(about = "Breadth-first visits a graph.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// Static dispatch (default BVGraph parameters).
    #[arg(short = 's', long = "static")]
    _static: bool,
    /// Static dispatch (default BVGraph parameters).
    #[arg(short = 'r', long, default_value_t = 1)]
    repeats: usize,
}

fn visit(graph: impl RandomAccessGraph) -> Result<()> {
    let num_nodes = graph.num_nodes();
    let mut visited = bitvec![0; num_nodes];
    let mut queue = VecDeque::new();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("node")
        .local_speed(true)
        .expected_updates(Some(num_nodes));
    pl.start("Visiting graph...");

    for start in 0..num_nodes {
        if visited[start] {
            continue;
        }
        queue.push_back(start as _);
        visited.set(start, true);

        while !queue.is_empty() {
            pl.light_update();
            let current_node = queue.pop_front().unwrap();
            for succ in graph.successors(current_node) {
                if !visited[succ] {
                    queue.push_back(succ);
                    visited.set(succ as _, true);
                }
            }
        }
    }

    pl.done();

    Ok(())
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    let config = BVGraph::with_basename(&args.basename)
        .mode::<Mmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS);

    for _ in 0..args.repeats {
        match get_endianess(&args.basename)?.as_str() {
            #[cfg(any(
                feature = "be_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            BE::NAME => match args._static {
                true => visit(
                    config
                        .clone()
                        .endianness::<BE>()
                        .dispatch::<Static>()
                        .load()?,
                )?,
                false => visit(config.clone().endianness::<BE>().load()?)?,
            },

            #[cfg(any(
                feature = "le_bins",
                not(any(feature = "be_bins", feature = "le_bins"))
            ))]
            LE::NAME => match args._static {
                true => visit(
                    config
                        .clone()
                        .endianness::<LE>()
                        .dispatch::<Static>()
                        .load()?,
                )?,
                false => visit(config.clone().endianness::<LE>().load()?)?,
            },
            e => panic!("Unknown endianness: {}", e),
        };
    }
    Ok(())
}
