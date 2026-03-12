/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::create_parent_dir;
use crate::*;
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::prelude::{BE, Endianness};
use dsi_progress_logger::prelude::*;
use rayon::prelude::ParallelSliceMut;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use tempfile::Builder;
use webgraph::graphs::arc_list_graph::ArcListGraph;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(
    about = "Reads a list of arcs from standard input and creates a graph in the BV format.",
    long_about = "Reads a list of arcs from standard input and creates a graph in the BV format. Each arc is \
        a pair of numeric node identifiers separated by a TAB (but the format is \
        customizable). If --labels is used, source and target values are treated \
        as string labels: numerical identifiers will be assigned in appearance \
        order, and the final list of labels will be saved in a file with the same \
        basename as the graph and extension .nodes. Without --labels, nodes are \
        numbered starting from zero."
)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub dst: PathBuf,

    #[arg(long)]
    /// The number of nodes in the graph. If specified, overrides the value
    /// inferred from the arcs; useful for adding isolated nodes at the end.​
    pub num_nodes: Option<usize>,

    #[arg(long)]
    /// The number of arcs in the graph; if specified, it will be used to estimate the progress.​
    pub num_arcs: Option<usize>,

    #[clap(flatten)]
    pub arcs_args: ArcsArgs,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    log::info!("Reading arcs from stdin...");
    let stdin = std::io::stdin().lock();
    from_csv(args, stdin)
}

pub fn from_csv(args: CliArgs, file: impl BufRead) -> Result<()> {
    let dir = Builder::new().prefix("from_arcs_sort_").tempdir()?;

    let mut group_by = SortPairs::new_dedup(args.memory_usage.memory_usage, &dir)?;
    let mut nodes = HashMap::new();

    // read the csv and put it inside the sort pairs
    let mut pl = progress_logger![
        display_memory = true,
        item_name = "lines",
        expected_updates = args.arcs_args.max_arcs.or(args.num_arcs),
    ];

    if let Some(duration) = args.log_interval.log_interval {
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
            if num_arcs >= max_arcs {
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
                line_num,
                line,
                vals.len(),
                biggest_idx + 1,
                args.arcs_args.separator,
            );
            continue;
        }

        let src = vals[args.arcs_args.source_column];
        let dst = vals[args.arcs_args.target_column];

        // parse if exact, or build a node list
        let src_id = if args.arcs_args.labels {
            let node_id = nodes.len();
            *nodes.entry(src.to_string()).or_insert(node_id)
        } else {
            src.parse::<usize>().with_context(|| {
                format!(
                    "Error parsing as integer source column value {:?} at line {}",
                    src, line_num,
                )
            })?
        };
        let dst_id = if args.arcs_args.labels {
            let node_id = nodes.len();
            *nodes.entry(dst.to_string()).or_insert(node_id)
        } else {
            dst.parse::<usize>().with_context(|| {
                format!(
                    "Error parsing as integer target column value {:?} at line {}",
                    dst, line_num,
                )
            })?
        };

        num_nodes = num_nodes.max(src_id.max(dst_id) + 1);
        group_by.push(src_id, dst_id).unwrap();
        pl.light_update();
        num_arcs += 1;
    }
    pl.done();

    if args.arcs_args.labels {
        debug_assert_eq!(
            num_nodes,
            nodes.len(),
            "Consistency check of the algorithm. The number of nodes should be equal to the number of unique nodes found in the arcs."
        );
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
        log::error!(
            "No arcs read from stdin! Check that the --separator={:?} value is correct and that the --source-column={:?} and --target-column={:?} values are correct.",
            args.arcs_args.separator,
            args.arcs_args.source_column,
            args.arcs_args.target_column
        );
        return Ok(());
    }

    // convert the iter to a graph
    let g = ArcListGraph::new(num_nodes, group_by.iter().unwrap().map(|(pair, _)| pair));

    create_parent_dir(&args.dst)?;

    // compress it
    let target_endianness = args.ca.endianness.clone();
    let dir = Builder::new().prefix("from_arcs_compress_").tempdir()?;
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    thread_pool.install(|| {
        builder
            .par_comp_lenders_endianness(&g, &target_endianness.unwrap_or_else(|| BE::NAME.into()))
    })?;

    // save the nodes
    if args.arcs_args.labels {
        let nodes_file = args.dst.with_extension("nodes");
        let mut pl = progress_logger![
            display_memory = true,
            item_name = "lines",
            expected_updates = args.arcs_args.max_arcs.or(args.num_arcs),
        ];
        if let Some(duration) = args.log_interval.log_interval {
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
