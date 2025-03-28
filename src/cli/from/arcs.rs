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
use clap::Parser;
use dsi_bitstream::prelude::{Endianness, BE};
use dsi_progress_logger::prelude::*;
use itertools::Itertools;
use rayon::prelude::ParallelSliceMut;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use tempfile::Builder;

#[derive(Parser, Debug)]
#[command(
    about = "Read from standard input a list of arcs and create a BvGraph. Each arc is specified by a pair of labels separated by a TAB (but the format is customizable), and numerical identifiers will be assigned to the labels in appearance order. The final list of node labels will be saved in a file with the same basename of the graph and extension .nodes. The option --exact can be used to use the labels directly as node identifiers. Note that in that case nodes are numbered starting from zero."
)]
pub struct CliArgs {
    /// The basename of the graph.
    pub dst: PathBuf,

    #[arg(long)]
    /// The number of nodes in the graph; if specified this will be used instead of the number inferred.
    /// This is useful if you want to add disconnected nodes at the end of the graph.
    pub num_nodes: Option<usize>,

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

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    log::info!("Reading arcs from stdin...");
    let stdin = std::io::stdin().lock();
    from_csv(global_args, args, stdin)
}

pub fn from_csv(global_args: GlobalArgs, args: CliArgs, file: impl BufRead) -> Result<()> {
    let dir = Builder::new().prefix("from_arcs_sort_").tempdir()?;

    let mut group_by = SortPairs::new(args.batch_size.batch_size, &dir)?;
    let mut nodes = HashMap::new();

    // read the csv and put it inside the sort pairs
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("lines")
        .expected_updates(args.arcs_args.max_arcs.or(args.num_arcs));

    if let Some(duration) = global_args.log_interval {
        pl.log_interval(duration);
    }
    pl.start("Reading arcs CSV");

    let mut iter = file.lines();
    // skip the first few lines
    for _ in 0..args.arcs_args.lines_to_skip {
        let _ = iter.next();
    }
    let biggest_idx = args
        .arcs_args
        .source_column
        .max(args.arcs_args.target_column);
    let mut num_nodes = 0;
    let mut num_arcs = 0;
    for (line_num, line) in iter.enumerate() {
        // break if we reached the end
        if let Some(max_arcs) = args.arcs_args.max_arcs {
            if num_arcs > max_arcs {
                break;
            }
        }
        let line = line.unwrap();
        // skip comment
        if line.trim().starts_with(args.arcs_args.line_comment_symbol) {
            continue;
        }

        // split the csv line into the args
        let vals = line.split(args.arcs_args.separator).collect::<Vec<_>>();

        if vals.get(biggest_idx).is_none() {
            log::warn!(
                "Line {}: {:?} from stdin does not have enough columns: got {} columns but expected at least {} columns separated by {:?} (you can change the separator using the --separator option)", 
                line_num, line, vals.len(), biggest_idx + 1, args.arcs_args.separator,
            );
            continue;
        }

        let src = vals[args.arcs_args.source_column];
        let dst = vals[args.arcs_args.target_column];

        // parse if exact, or build a node list
        let src_id = if args.arcs_args.exact {
            match src.parse::<usize>() {
                Ok(src_id) => src_id,
                Err(err) => {
                    log::error!(
                        "Error parsing as integer source column value {:?} at line {}: {:?}",
                        src,
                        line_num,
                        err
                    );
                    return Ok(());
                }
            }
        } else {
            let node_id = nodes.len();
            *nodes.entry(src.to_string()).or_insert(node_id)
        };
        let dst_id = if args.arcs_args.exact {
            match dst.parse::<usize>() {
                Ok(dst_id) => dst_id,
                Err(err) => {
                    log::error!(
                        "Error parsing as integer target column value {:?} at line {}: {:?}",
                        dst,
                        line_num,
                        err
                    );
                    return Ok(());
                }
            }
        } else {
            let node_id = nodes.len();
            *nodes.entry(dst.to_string()).or_insert(node_id)
        };

        num_nodes = num_nodes.max(src_id.max(dst_id) + 1);
        group_by.push(src_id, dst_id).unwrap();
        pl.light_update();
        num_arcs += 1;
    }
    pl.done();

    if !args.arcs_args.exact {
        debug_assert_eq!(num_nodes, nodes.len(), "Consistency check of the algorithm. The number of nodes should be equal to the number of unique nodes found in the arcs.");
    }

    if let Some(user_num_nodes) = args.num_nodes {
        if user_num_nodes < num_nodes {
            log::warn!(
                "The number of nodes specified by --num-nodes={} is smaller than the number of nodes found in the arcs: {}",
                user_num_nodes,
                num_nodes
            );
        }
        num_nodes = user_num_nodes;
    }

    log::info!("Arcs read: {} Nodes: {}", num_arcs, num_nodes);
    if num_arcs == 0 {
        log::error!("No arcs read from stdin! Check that the --separator={:?} value is correct and that the --source-column={:?} and --target-column={:?} values are correct.", args.arcs_args.separator, args.arcs_args.source_column, args.arcs_args.target_column);
        return Ok(());
    }

    // convert the iter to a graph
    let g = Left(ArcListGraph::new(
        num_nodes,
        group_by
            .iter()
            .unwrap()
            .map(|(src, dst, _)| (src, dst))
            .dedup(),
    ));

    create_parent_dir(&args.dst)?;

    // compress it
    let target_endianness = args.ca.endianness.clone();
    let dir = Builder::new().prefix("from_arcs_compress_").tempdir()?;
    let thread_pool = crate::cli::get_thread_pool(args.num_threads.num_threads);
    BvComp::parallel_endianness(
        &args.dst,
        &g,
        num_nodes,
        args.ca.into(),
        &thread_pool,
        dir,
        &target_endianness.unwrap_or_else(|| BE::NAME.into()),
    )
    .unwrap();

    // save the nodes
    if !args.arcs_args.exact {
        let nodes_file = args.dst.with_extension("nodes");
        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("lines")
            .expected_updates(args.arcs_args.max_arcs.or(args.num_arcs));
        if let Some(duration) = global_args.log_interval {
            pl.log_interval(duration);
        }

        let mut file = std::fs::File::create(&nodes_file).unwrap();
        let mut buf = std::io::BufWriter::new(&mut file);
        let mut nodes = nodes.into_iter().collect::<Vec<_>>();
        // sort based on the idx
        nodes.par_sort_by(|(_, a), (_, b)| a.cmp(b));
        pl.start(format!("Storing the nodes to {}", nodes_file.display()));
        for (node, _) in nodes {
            buf.write_all(node.as_bytes()).unwrap();
            buf.write_all(b"\n").unwrap();
            pl.light_update();
        }
        pl.done();
    }
    Ok(())
}
