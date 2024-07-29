/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::create_parent_dir;
use crate::cli::*;
use crate::graphs::arc_list_graph::ArcListGraph;
use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::{Endianness, BE};
use dsi_progress_logger::prelude::*;
use itertools::Itertools;
use rayon::prelude::ParallelSliceMut;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use tempfile::Builder;
pub const COMMAND_NAME: &str = "arcs";

#[derive(Args, Debug)]
#[command(
    about = "Creates a new BVGraph from a list of arcs. Each arc is specified by a pair of labels, and numerical identifiers will be assigned to the labels in appearance order. The final list of node labels will be saved in a file with the same basename of the graph and extension .nodes. The option --exact can be used to use the labels directly as node identifiers."
)]
pub struct CliArgs {
    /// The basename of the graph.
    pub dst: PathBuf,

    #[arg(long)]
    /// The number of nodes in the graph.
    pub num_nodes: usize,

    #[arg(long)]
    /// The number of arcs in the graph; if specified, it will be used to estimate the progress.
    pub num_arcs: Option<usize>,

    #[clap(flatten)]
    pub arcs_args: ArcsArgs,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub batch_size: BatchSizeArg,

    #[clap(flatten)]
    pub ca: CompressArgs,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    from_csv(CliArgs::from_arg_matches(submatches)?)
}

pub fn from_csv(args: CliArgs) -> Result<()> {
    let dir = Builder::new().prefix("FromCsvPairs").tempdir()?;

    let mut group_by = SortPairs::new(args.batch_size.batch_size, dir)?;
    let mut nodes = HashMap::new();

    // read the csv and put it inside the sort pairs
    let stdin = std::io::stdin();
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("lines")
        .expected_updates(args.arcs_args.max_lines.or(args.num_arcs));
    pl.start("Reading arcs CSV");

    let mut iter = stdin.lock().lines();
    // skip the first few lines
    for _ in 0..args.arcs_args.lines_to_skip {
        iter.next().unwrap().unwrap();
    }
    let mut line_id = 0;
    for line in iter {
        // break if we reached the end
        if let Some(max_lines) = args.arcs_args.max_lines {
            if line_id > max_lines {
                break;
            }
        }
        let line = line.unwrap();
        // skip comment
        if line.trim().starts_with(args.arcs_args.line_comment_simbol) {
            continue;
        }

        // split the csv line into the args
        let vals = line.split(args.arcs_args.separator).collect::<Vec<_>>();
        let src = vals[0];
        let dst = vals[1];

        // parse if exact, or build a node list
        let src_id = if args.arcs_args.exact {
            src.parse::<usize>().unwrap()
        } else {
            let node_id = nodes.len();
            *nodes.entry(src.to_string()).or_insert(node_id)
        };
        let dst_id = if args.arcs_args.exact {
            dst.parse::<usize>().unwrap()
        } else {
            let node_id = nodes.len();
            *nodes.entry(dst.to_string()).or_insert(node_id)
        };

        group_by.push(src_id, dst_id).unwrap();
        pl.light_update();
        line_id += 1;
    }
    pl.done();
    log::info!("Arcs read: {}", line_id);

    // convert the iter to a graph
    let g = Left(ArcListGraph::new(
        args.num_nodes,
        group_by
            .iter()
            .unwrap()
            .map(|(src, dst, _)| (src, dst))
            .dedup(),
    ));

    create_parent_dir(&args.dst)?;

    // compress it
    let target_endianness = args.ca.endianness.clone();
    let dir = Builder::new().prefix("CompressSimplified").tempdir()?;
    let thread_pool = crate::cli::get_thread_pool(args.num_threads.num_threads);
    BVComp::parallel_endianness(
        &args.dst,
        &g,
        args.num_nodes,
        args.ca.into(),
        thread_pool,
        dir,
        &target_endianness.unwrap_or_else(|| BE::NAME.into()),
    )
    .unwrap();

    // save the nodes
    if !args.arcs_args.exact {
        let mut file = std::fs::File::create(args.dst.with_extension("nodes")).unwrap();
        let mut buf = std::io::BufWriter::new(&mut file);
        let mut nodes = nodes.into_iter().collect::<Vec<_>>();
        // sort based on the idx
        nodes.par_sort_by(|(_, a), (_, b)| a.cmp(b));
        for (node, _) in nodes {
            buf.write_all(node.as_bytes()).unwrap();
            buf.write_all(b"\n").unwrap();
        }
    }
    Ok(())
}
