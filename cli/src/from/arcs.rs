/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::create_parent_dir;
use crate::*;
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::{BE, Endianness};
use dsi_progress_logger::prelude::*;
use rayon::prelude::ParallelSliceMut;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use tempfile::Builder;
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
        numbered starting from zero.",
    next_line_help = true
)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub dst: PathBuf,

    #[arg(long)]
    /// The number of nodes in the graph.​
    pub num_nodes: usize,

    #[arg(long)]
    /// The number of arcs in the graph; if specified, it will be used to
    /// estimate the progress.​
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
    let num_nodes = args.num_nodes;

    let labels = args.arcs_args.labels;
    let separator = args.arcs_args.separator;
    let source_column = args.arcs_args.source_column;
    let target_column = args.arcs_args.target_column;
    let comment = args.arcs_args.line_comment_symbol;
    let max_arcs = args.arcs_args.max_arcs;
    let biggest_idx = source_column.max(target_column);

    let mut nodes = HashMap::new();
    let mut num_arcs = 0usize;
    let mut parse_error: Option<anyhow::Error> = None;

    let mut lines = file.lines();
    for _ in 0..args.arcs_args.lines_to_skip {
        let _ = lines.next();
    }
    let mut line_count = 0usize;

    let pairs = std::iter::from_fn(|| {
        loop {
            if parse_error.is_some() {
                return None;
            }
            if max_arcs.is_some_and(|m| num_arcs >= m) {
                return None;
            }
            let line = match lines.next()? {
                Ok(l) => l,
                Err(e) => {
                    parse_error = Some(e.into());
                    return None;
                }
            };
            line_count += 1;

            if line.trim().starts_with(comment) {
                continue;
            }

            let vals = line.split(separator).collect::<Vec<_>>();
            if vals.get(biggest_idx).is_none() {
                log::warn!(
                    "Line {}: {:?} does not have enough columns: got {} columns \
                     but expected at least {} columns separated by {:?} \
                     (you can change the separator using the --separator option)",
                    line_count,
                    line,
                    vals.len(),
                    biggest_idx + 1,
                    separator,
                );
                continue;
            }

            let src = vals[source_column];
            let dst = vals[target_column];

            let src_id = if labels {
                let node_id = nodes.len();
                *nodes.entry(src.to_string()).or_insert(node_id)
            } else {
                match src.parse::<usize>() {
                    Ok(v) => v,
                    Err(_) => {
                        parse_error = Some(anyhow::anyhow!(
                            "Error parsing as integer source column value {:?} at line {}",
                            src,
                            line_count
                        ));
                        return None;
                    }
                }
            };

            let dst_id = if labels {
                let node_id = nodes.len();
                *nodes.entry(dst.to_string()).or_insert(node_id)
            } else {
                match dst.parse::<usize>() {
                    Ok(v) => v,
                    Err(_) => {
                        parse_error = Some(anyhow::anyhow!(
                            "Error parsing as integer target column value {:?} at line {}",
                            dst,
                            line_count
                        ));
                        return None;
                    }
                }
            };

            num_arcs += 1;
            return Some((src_id, dst_id));
        }
    });

    // Sort and partition arcs for parallel compression
    let mut conf = ParSortedGraph::config()
        .dedup()
        .memory_usage(args.memory_usage.memory_usage);
    if let Some(n) = args.num_arcs {
        conf = conf.expected_num_pairs(n);
    }
    let sorted = conf.sort_pairs(num_nodes, pairs)?;

    if let Some(e) = parse_error {
        return Err(e);
    }

    log::info!("Arcs read: {} Nodes: {}", num_arcs, num_nodes);
    if num_arcs == 0 {
        log::error!(
            "No arcs read from stdin! Check that the --separator={:?} value is correct \
             and that the --source-column={:?} and --target-column={:?} values are correct.",
            separator,
            source_column,
            target_column
        );
        return Ok(());
    }

    create_parent_dir(&args.dst)?;

    // Compress
    let target_endianness = args
        .ca
        .endianness
        .clone()
        .unwrap_or_else(|| BE::NAME.into());
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

    thread_pool.install(|| par_comp!(builder, sorted, target_endianness))?;

    // Save the label-to-node-id mapping
    if labels {
        let nodes_file = args.dst.with_extension("nodes");
        let mut pl = progress_logger![display_memory = true, item_name = "lines",];
        if let Some(duration) = args.log_interval.log_interval {
            pl.log_interval(duration);
        }

        let mut file = std::fs::File::create(&nodes_file).unwrap();
        let mut buf = std::io::BufWriter::new(&mut file);
        let mut nodes = nodes.into_iter().collect::<Vec<_>>();
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
