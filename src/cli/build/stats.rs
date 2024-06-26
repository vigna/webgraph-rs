/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Result, Context};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use std::path::PathBuf;
use serde::{ser::SerializeStruct, Deserialize, Serialize};

use crate::prelude::*;
use super::counts::Counts;
use dsi_bitstream::utils::{stats::Code, CodesStats};
use std::collections::HashMap;

pub const COMMAND_NAME: &str = "stats";

#[derive(Args, Debug)]
#[command(about = "Read the .count file and compute human readable statistics as .stats", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

#[derive(Debug)]
pub struct GraphSizeInfo {
    pub total_size: u64,
    pub bits_per_link: f64,
    pub bits_per_node: f64,

    pub outdegrees: u64,
    pub reference_offsets: u64,
    pub block_counts: u64,
    pub blocks: u64,
    pub interval_counts: u64,
    pub interval_starts: u64,
    pub interval_lens: u64,
    pub first_residuals: u64,
    pub residuals: u64,
}

fn humanize(mut value: f64) -> String {
    let mut uom = ' ';
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'K';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'M';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'G';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'T';
    }
    format!("{:.3}{}", value, uom)
}


#[derive(Debug)]
pub struct OptimalCodes {
    pub outdegrees: Code,
    pub reference_offsets: Code,
    pub block_counts: Code,
    pub blocks: Code,
    pub interval_counts: Code,
    pub interval_starts: Code,
    pub interval_lens: Code,
    pub first_residuals: Code,
    pub residuals: Code,
}

fn compute(code: &crate::prelude::Code, distrib: &HashMap<u64, u64>) -> (u64, u64, u64, Code) {
    let total = distrib.values().sum::<u64>();
    let mut graph = 0;
    let mut entropy = 0.0;
    let mut code_stats = <CodesStats>::default();

    for (k, v) in distrib.iter() {
        let p = total as f64 / *v as f64;
        entropy += *v as f64 * p.log2();
        graph += code.len(*k) * *v;
        code_stats.update_many(*k, *v);
    }

    let opt = code_stats.best_code();

    (graph, entropy as u64, opt.1, opt.0)
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => build_stats::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => build_stats::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_stats<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    // load the properties
    let (num_nodes, num_arcs, comp_flags) = parse_properties::<E>(&args.basename.with_extension("properties"))?;

    // load the counts file
    let counts_file = args.basename.with_extension("counts");
    let counts = bincode::deserialize::<Counts>(&std::fs::read(&counts_file)
        .with_context(|| format!("Could not read the counts file {}", counts_file.display()))?
    )?;

    // compute the size of the graph
    let outdegrees = compute(&comp_flags.outdegrees, &counts.outdegrees);
    let reference_offsets = compute(&comp_flags.references, &counts.reference_offsets);
    let block_counts = compute(&comp_flags.blocks, &counts.block_counts);
    let blocks = compute(&comp_flags.blocks, &counts.blocks);
    let interval_counts = compute(&comp_flags.intervals, &counts.interval_counts);
    let interval_starts = compute(&comp_flags.intervals, &counts.interval_starts);
    let interval_lens = compute(&comp_flags.intervals, &counts.interval_lens);
    let first_residuals = compute(&comp_flags.residuals, &counts.first_residuals);
    let residuals = compute(&comp_flags.residuals, &counts.residuals);

    let graph_total_size = outdegrees.0 + reference_offsets.0 + block_counts.0 + blocks.0 + interval_counts.0 + interval_starts.0 + interval_lens.0 + first_residuals.0 + residuals.0;
    let graph = GraphSizeInfo {
        total_size: graph_total_size,
        bits_per_link: graph_total_size as f64 / num_arcs as f64,
        bits_per_node: graph_total_size as f64 / num_nodes as f64,
        outdegrees: outdegrees.0,
        reference_offsets: reference_offsets.0,
        block_counts: block_counts.0,
        blocks: blocks.0,
        interval_counts: interval_counts.0,
        interval_starts: interval_starts.0,
        interval_lens: interval_lens.0,
        first_residuals: first_residuals.0,
        residuals: residuals.0,
    };
    
    let entropy_total_size = outdegrees.1 + reference_offsets.1 + block_counts.1 + blocks.1 + interval_counts.1 + interval_starts.1 + interval_lens.1 + first_residuals.1 + residuals.1;

    let entropy = GraphSizeInfo {
        total_size: entropy_total_size,
        bits_per_link: entropy_total_size as f64 / num_arcs as f64,
        bits_per_node: entropy_total_size as f64 / num_nodes as f64,
        outdegrees: outdegrees.1,
        reference_offsets: reference_offsets.1,
        block_counts: block_counts.1,
        blocks: blocks.1,
        interval_counts: interval_counts.1,
        interval_starts: interval_starts.1,
        interval_lens: interval_lens.1,
        first_residuals: first_residuals.1,
        residuals: residuals.1,
    };

    // compute the optimal codes
    let opt_total_size = outdegrees.2 + reference_offsets.2 + block_counts.2 + blocks.2 + interval_counts.2 + interval_starts.2 + interval_lens.2 + first_residuals.2 + residuals.2;

    let opt_graph = GraphSizeInfo {
        total_size: opt_total_size,
        bits_per_link: opt_total_size as f64 / num_arcs as f64,
        bits_per_node: opt_total_size as f64 / num_nodes as f64,
        outdegrees: outdegrees.2,
        reference_offsets: reference_offsets.2,
        block_counts: block_counts.2,
        blocks: blocks.2,
        interval_counts: interval_counts.2,
        interval_starts: interval_starts.2,
        interval_lens: interval_lens.2,
        first_residuals: first_residuals.2,
        residuals: residuals.2,
    };

    let opt_codes = OptimalCodes {
        outdegrees: outdegrees.3,
        reference_offsets: reference_offsets.3,
        block_counts: block_counts.3,
        blocks: blocks.3,
        interval_counts: interval_counts.3,
        interval_starts: interval_starts.3,
        interval_lens: interval_lens.3,
        first_residuals: first_residuals.3,
        residuals: residuals.3,
    };

    println!("{:>17},{:>16},{:>16},{:>16}", "", "Graph", "Entropy", "Optimal");
    println!("{:>17},{:>16}b,{:>16}b,{:>16}b", "Total size", graph.total_size, entropy.total_size, opt_graph.total_size);
    println!("{:>17},{:>16}b,{:>16}b,{:>16}b", "Total size", humanize(graph.total_size as f64), humanize(entropy.total_size as f64), humanize(opt_graph.total_size as f64));
    println!("{:>17},{:>16.3},{:>16.3},{:>16.3}", "Bits/Link", graph.bits_per_link, entropy.bits_per_link, opt_graph.bits_per_link);
    println!("{:>17},{:>16.3},{:>16.3},{:>16.3}", "Bits/Node", graph.bits_per_node, entropy.bits_per_node, opt_graph.bits_per_node);
    println!("");
    
    println!("Entropy Savings: {:>16.3}b", graph.total_size - entropy.total_size);
    println!("Entropy Savings: {:>16}b", humanize((graph.total_size - entropy.total_size) as f64));
    println!("Entropy Savings: {:>16.3}%", (1.0 - entropy.total_size as f64 / graph.total_size as f64) * 100.0);
    println!("");
    println!("Optimal Savings: {:>16.3}b", graph.total_size - opt_graph.total_size);
    println!("Optimal Savings: {:>16}b", humanize((graph.total_size - opt_graph.total_size) as f64));
    println!("Optimal Savings: {:>16.3}%", (1.0 - opt_graph.total_size as f64 / graph.total_size as f64) * 100.0);
    println!("");

    println!("{:>17},{:>16},{:>16},{:>16}", "Code", "Size %", "Size Human", "Size");
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Outdegrees", 100.0 * graph.outdegrees as f64 / graph.total_size as f64, humanize(graph.outdegrees as f64), graph.outdegrees);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Reference offsets", 100.0 * graph.reference_offsets as f64 / graph.total_size as f64, humanize(graph.reference_offsets as f64), graph.reference_offsets);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Block counts", 100.0 * graph.block_counts as f64 / graph.total_size as f64, humanize(graph.block_counts as f64), graph.block_counts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Blocks", 100.0 * graph.blocks as f64 / graph.total_size as f64, humanize(graph.blocks as f64), graph.blocks);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Interval counts", 100.0 * graph.interval_counts as f64 / graph.total_size as f64, humanize(graph.interval_counts as f64), graph.interval_counts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Interval starts", 100.0 * graph.interval_starts as f64 / graph.total_size as f64, humanize(graph.interval_starts as f64), graph.interval_starts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Interval lens", 100.0 * graph.interval_lens as f64 / graph.total_size as f64, humanize(graph.interval_lens as f64), graph.interval_lens);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "First residuals", 100.0 * graph.first_residuals as f64 / graph.total_size as f64, humanize(graph.first_residuals as f64), graph.first_residuals);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Residuals", 100.0 * graph.residuals as f64 / graph.total_size as f64, humanize(graph.residuals as f64), graph.residuals);
    println!("");

    println!("{:>17},{:>16},{:>16},{:>16}", "Entropy", "Size %", "Size Human", "Size");
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Outdegrees", 100.0 * entropy.outdegrees as f64 / entropy.total_size as f64, humanize(entropy.outdegrees as f64), entropy.outdegrees);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Reference offsets", 100.0 * entropy.reference_offsets as f64 / entropy.total_size as f64, humanize(entropy.reference_offsets as f64), entropy.reference_offsets);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Block counts", 100.0 * entropy.block_counts as f64 / entropy.total_size as f64, humanize(entropy.block_counts as f64), entropy.block_counts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Blocks", 100.0 * entropy.blocks as f64 / entropy.total_size as f64, humanize(entropy.blocks as f64), entropy.blocks);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Interval counts", 100.0 * entropy.interval_counts as f64 / entropy.total_size as f64, humanize(entropy.interval_counts as f64), entropy.interval_counts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Interval starts", 100.0 * entropy.interval_starts as f64 / entropy.total_size as f64, humanize(entropy.interval_starts as f64), entropy.interval_starts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Interval lens", 100.0 * entropy.interval_lens as f64 / entropy.total_size as f64, humanize(entropy.interval_lens as f64), entropy.interval_lens);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "First residuals", 100.0 * entropy.first_residuals as f64 / entropy.total_size as f64, humanize(entropy.first_residuals as f64), entropy.first_residuals);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b", "Residuals", 100.0 * entropy.residuals as f64 / entropy.total_size as f64, humanize(entropy.residuals as f64), entropy.residuals);
    println!("");

    println!("{:>17},{:>16},{:>16},{:>16},{:>16}", "Optimal", "Size %", "Size Human", "Size", "Code");
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Outdegrees", 100.0 * opt_graph.outdegrees as f64 / opt_graph.total_size as f64, humanize(opt_graph.outdegrees as f64), opt_graph.outdegrees, opt_codes.outdegrees);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Reference offsets", 100.0 * opt_graph.reference_offsets as f64 / opt_graph.total_size as f64, humanize(opt_graph.reference_offsets as f64), opt_graph.reference_offsets, opt_codes.reference_offsets);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Block counts", 100.0 * opt_graph.block_counts as f64 / opt_graph.total_size as f64, humanize(opt_graph.block_counts as f64), opt_graph.block_counts, opt_codes.block_counts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Blocks", 100.0 * opt_graph.blocks as f64 / opt_graph.total_size as f64, humanize(opt_graph.blocks as f64), opt_graph.blocks, opt_codes.blocks);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Interval counts", 100.0 * opt_graph.interval_counts as f64 / opt_graph.total_size as f64, humanize(opt_graph.interval_counts as f64), opt_graph.interval_counts, opt_codes.interval_counts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Interval starts", 100.0 * opt_graph.interval_starts as f64 / opt_graph.total_size as f64, humanize(opt_graph.interval_starts as f64), opt_graph.interval_starts, opt_codes.interval_starts);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Interval lens", 100.0 * opt_graph.interval_lens as f64 / opt_graph.total_size as f64, humanize(opt_graph.interval_lens as f64), opt_graph.interval_lens, opt_codes.interval_lens);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "First residuals", 100.0 * opt_graph.first_residuals as f64 / opt_graph.total_size as f64, humanize(opt_graph.first_residuals as f64), opt_graph.first_residuals, opt_codes.first_residuals);
    println!("{:>17},{:>16.3}%,{:>16}b,{:>16}b,{:>16}", "Residuals", 100.0 * opt_graph.residuals as f64 / opt_graph.total_size as f64, humanize(opt_graph.residuals as f64), opt_graph.residuals, opt_codes.residuals);
    println!("");


    println!("{:>17},{:>16},{:>16},{:>16},{:>16}", "", "Entropy Savings", "Entropy Savings %", "Optimal Savings", "Optimal Savings %");
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Outdegrees", humanize((graph.outdegrees - entropy.outdegrees) as f64), 100.0 * (graph.outdegrees - entropy.outdegrees) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.outdegrees - opt_graph.outdegrees) as f64), 100.0 * (graph.outdegrees - opt_graph.outdegrees) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Reference offsets", humanize((graph.reference_offsets - entropy.reference_offsets) as f64), 100.0 * (graph.reference_offsets - entropy.reference_offsets) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.reference_offsets - opt_graph.reference_offsets) as f64), 100.0 * (graph.reference_offsets - opt_graph.reference_offsets) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Block counts", humanize((graph.block_counts - entropy.block_counts) as f64), 100.0 * (graph.block_counts - entropy.block_counts) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.block_counts - opt_graph.block_counts) as f64), 100.0 * (graph.block_counts - opt_graph.block_counts) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Blocks", humanize((graph.blocks - entropy.blocks) as f64), 100.0 * (graph.blocks - entropy.blocks) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.blocks - opt_graph.blocks) as f64), 100.0 * (graph.blocks - opt_graph.blocks) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Interval counts", humanize((graph.interval_counts - entropy.interval_counts) as f64), 100.0 * (graph.interval_counts - entropy.interval_counts) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.interval_counts - opt_graph.interval_counts) as f64), 100.0 * (graph.interval_counts - opt_graph.interval_counts) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Interval starts", humanize((graph.interval_starts - entropy.interval_starts) as f64), 100.0 * (graph.interval_starts - entropy.interval_starts) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.interval_starts - opt_graph.interval_starts) as f64), 100.0 * (graph.interval_starts - opt_graph.interval_starts) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Interval lens", humanize((graph.interval_lens - entropy.interval_lens) as f64), 100.0 * (graph.interval_lens - entropy.interval_lens) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.interval_lens - opt_graph.interval_lens) as f64), 100.0 * (graph.interval_lens - opt_graph.interval_lens) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "First residuals", humanize((graph.first_residuals - entropy.first_residuals) as f64), 100.0 * (graph.first_residuals - entropy.first_residuals) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.first_residuals - opt_graph.first_residuals) as f64), 100.0 * (graph.first_residuals - opt_graph.first_residuals) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("{:>17},{:>16}b,{:>16.3}%,{:>16}b,{:>16.3}%", "Residuals", humanize((graph.residuals - entropy.residuals) as f64), 100.0 * (graph.residuals - entropy.residuals) as f64 / (graph.total_size - entropy.total_size) as f64, humanize((graph.residuals - opt_graph.residuals) as f64), 100.0 * (graph.residuals - opt_graph.residuals) as f64 / (graph.total_size - opt_graph.total_size) as f64);
    println!("");

    Ok(())
}
