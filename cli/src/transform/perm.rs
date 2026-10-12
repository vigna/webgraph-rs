/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::*;
use anyhow::Result;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "perm", about = "Permutes a graph in the BV format according to a given permutation.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub src: PathBuf,
    /// The basename of the permuted graph.​
    pub dst: PathBuf,

    /// The path to the permutation to apply to the graph.​
    pub permutation: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.​
    pub fmt: IntSliceFormat,

    #[arg(short, long)]
    /// Uses the sequential algorithm (does not need offsets).​
    pub sequential: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[arg(long, conflicts_with = "sequential")]
    /// Uses the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.​
    pub dcf: bool,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

/// Delegates to [`to bvgraph`](crate::to::bvgraph), which can apply a
/// permutation while compressing a graph.
pub fn main(args: CliArgs) -> Result<()> {
    crate::to::bvgraph::main(crate::to::bvgraph::CliArgs {
        src: args.src,
        dst: args.dst,
        num_threads: args.num_threads,
        permutation: Some(args.permutation),
        fmt: args.fmt,
        sequential: args.sequential,
        dcf: args.dcf,
        memory_usage: args.memory_usage,
        ca: args.ca,
        log_interval: args.log_interval,
    })
}
