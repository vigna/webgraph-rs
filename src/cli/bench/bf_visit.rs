/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use std::collections::VecDeque;
use std::path::PathBuf;
use sux::prelude::BitVec;

pub const COMMAND_NAME: &str = "bf-visit";

#[derive(Args, Debug)]
#[command(about = "Breadth-first visits a graph.", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
    /// Static dispatch (default BVGraph parameters).
    #[arg(short = 's', long = "static")]
    _static: bool,
    /// Static dispatch (default BVGraph parameters).
    #[arg(short = 'r', long, default_value_t = 1)]
    repeats: usize,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    let config = BVGraph::with_basename(&args.basename)
        .mode::<Mmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS);

    for _ in 0..args.repeats {
        match get_endianness(&args.basename)?.as_str() {
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

fn visit(graph: impl RandomAccessGraph) -> Result<()> {
    let num_nodes = graph.num_nodes();
    let mut visited = BitVec::new(num_nodes);
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
