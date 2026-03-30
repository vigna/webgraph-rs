/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::LogIntervalArg;
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::*;
use std::io::Write;
use std::path::PathBuf;
use value_traits::slices::SliceByValue;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "stats",
    about = "Computes statistical data of a graph.",
    long_about = "Computes statistical data of a graph: outdegree and indegree \
        distributions, average gap, average locality, dangling/terminal nodes, \
        successor delta statistics, and optionally SCC-related statistics. \
        The graph is scanned sequentially, so only the indegree array (one \
        counter per node) is allocated.",
    next_line_help = true
)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    /// The basename for result files (default: same as graph basename).​
    pub results_basename: Option<PathBuf>,

    #[arg(short = 's', long)]
    /// Save per-node indegrees and outdegrees.​
    pub save_degrees: bool,

    #[arg(long, value_enum, default_value_t = crate::IntSliceFormat::Ascii)]
    /// Format for per-node indegrees and outdegrees.​
    pub degrees_fmt: crate::IntSliceFormat,

    #[arg(long)]
    /// Path to sizes of strongly connected components (as produced by webgraph-sccs --sizes).​
    pub scc_sizes: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = crate::IntSliceFormat::Ascii)]
    /// Format of the sizes of strongly connected components.​
    pub scc_sizes_fmt: crate::IntSliceFormat,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => stats::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => stats::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn stats<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let graph = BvGraphSeq::with_basename(&args.basename)
        .endianness::<E>()
        .load()?;

    let num_nodes = graph.num_nodes();
    let results_basename = args.results_basename.as_ref().unwrap_or(&args.basename);

    let mut outdegree_count: Vec<u64> = Vec::new();
    let mut indegree = vec![0usize; num_nodes];
    let mut outdegrees: Vec<usize> = if args.save_degrees {
        Vec::with_capacity(num_nodes)
    } else {
        Vec::new()
    };

    let mut max_outdegree: usize = 0;
    let mut max_outdegree_node: usize = 0;
    let mut min_outdegree: usize = usize::MAX;
    let mut min_outdegree_node: usize = 0;
    let mut dangling: u64 = 0;
    let mut terminal: u64 = 0;
    let mut loops: u64 = 0;
    let mut num_arcs: u64 = 0;
    let mut num_gaps: u64 = 0;
    let mut tot_loc: u128 = 0;
    let mut tot_gap: u128 = 0;

    // Statistics for the gap width of successor lists (exponentially binned).
    let mut successor_delta_stats = [0u64; 64];

    let mut pl = progress_logger![
        display_memory = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
    ];
    if let Some(duration) = args.log_interval.log_interval {
        pl.log_interval(duration);
    }
    pl.start("Scanning...");

    for_!((node, successors) in graph.iter() {
        let succ: Vec<usize> = successors.collect();
        let d = succ.len();

        if args.save_degrees {
            outdegrees.push(d);
        }

        if d > 1 {
            tot_gap += (succ[d - 1] - succ[0]) as u128;
            tot_gap += (succ[0] as i64 - node as i64).to_nat() as u128;
            num_gaps += d as u64;
        }

        for &s in &succ {
            tot_loc += s.abs_diff(node) as u128;

            if s != node {
                let delta = node.abs_diff(s);
                successor_delta_stats[delta.ilog2() as usize] += 1;
            } else {
                loops += 1;
            }

            indegree[s] += 1;
        }

        if d == 0 {
            dangling += 1;
            terminal += 1;
        }

        if d == 1 && succ[0] == node {
            terminal += 1;
        }

        if d < min_outdegree {
            min_outdegree = d;
            min_outdegree_node = node;
        }

        if d > max_outdegree {
            max_outdegree = d;
            max_outdegree_node = node;
        }

        num_arcs += d as u64;

        if d >= outdegree_count.len() {
            outdegree_count.resize(d + 1, 0);
        }
        outdegree_count[d] += 1;

        pl.light_update();
    });

    pl.done();

    if args.save_degrees {
        args.degrees_fmt.store(
            results_basename.with_extension("outdegrees"),
            &outdegrees,
            Some(max_outdegree),
        )?;
        args.degrees_fmt.store(
            results_basename.with_extension("indegrees"),
            &indegree,
            None,
        )?;
    }

    // Write .stats properties file
    let stats_path = results_basename.with_extension("stats");
    let mut p = std::io::BufWriter::new(std::fs::File::create(&stats_path)?);

    writeln!(p, "nodes={}", num_nodes)?;
    writeln!(p, "arcs={}", num_arcs)?;
    writeln!(p, "loops={}", loops)?;
    writeln!(
        p,
        "successoravggap={:.3}",
        tot_gap as f64 / num_gaps.max(1) as f64
    )?;
    writeln!(
        p,
        "avglocality={:.3}",
        tot_loc as f64 / num_arcs.max(1) as f64
    )?;
    writeln!(p, "minoutdegree={}", min_outdegree)?;
    writeln!(p, "maxoutdegree={}", max_outdegree)?;
    writeln!(p, "minoutdegreenode={}", min_outdegree_node)?;
    writeln!(p, "maxoutdegreenode={}", max_outdegree_node)?;
    writeln!(p, "dangling={}", dangling)?;
    writeln!(p, "terminal={}", terminal)?;
    writeln!(
        p,
        "percdangling={}",
        100.0 * dangling as f64 / num_nodes as f64
    )?;
    writeln!(p, "avgoutdegree={}", num_arcs as f64 / num_nodes as f64)?;

    // Successor log-delta statistics
    let l = successor_delta_stats
        .iter()
        .rposition(|&x| x != 0)
        .unwrap_or(0);

    let mut tot_log_delta = 0u128;
    let mut num_delta = 0u64;
    let delta_str: String = (0..=l)
        .map(|i| {
            let count = successor_delta_stats[i];
            num_delta += count;
            let g: u64 = 1 << i; // 2^i
            tot_log_delta += (((g * 3 + 1).ilog2() - 1) as u64 * count) as u128;
            count.to_string()
        })
        .collect::<Vec<_>>()
        .join(",");

    writeln!(p, "successorlogdeltastats={}", delta_str)?;
    writeln!(
        p,
        "successoravglogdelta={}",
        if num_delta == 0 {
            "0".to_string()
        } else {
            format!("{:.3}", tot_log_delta as f64 / (num_delta * 2) as f64)
        }
    )?;

    // Write outdegree distribution
    {
        let path = results_basename.with_extension("outdegree");
        let mut w = std::io::BufWriter::new(std::fs::File::create(&path)?);
        for &count in &outdegree_count {
            writeln!(w, "{}", count)?;
        }
    }

    // Compute and write indegree statistics
    let mut indegree_count: Vec<u64> = Vec::new();
    let mut max_indegree: usize = 0;
    let mut max_indegree_node: usize = 0;
    let mut min_indegree = usize::MAX;
    let mut min_indegree_node: usize = 0;

    // Iterate in reverse to match the Java behavior for tie-breaking on
    // min/max indegree nodes (last node wins for min, first for max when
    // iterating backwards).
    for i in (0..num_nodes).rev() {
        let d = indegree[i];
        if d >= indegree_count.len() {
            indegree_count.resize(d + 1, 0);
        }
        if d < min_indegree {
            min_indegree = d;
            min_indegree_node = i;
        }
        if d > max_indegree {
            max_indegree = d;
            max_indegree_node = i;
        }
        indegree_count[d] += 1;
    }

    {
        let path = results_basename.with_extension("indegree");
        let mut w = std::io::BufWriter::new(std::fs::File::create(&path)?);
        for &count in &indegree_count {
            writeln!(w, "{}", count)?;
        }
    }

    writeln!(p, "minindegree={}", min_indegree)?;
    writeln!(p, "maxindegree={}", max_indegree)?;
    writeln!(p, "minindegreenode={}", min_indegree_node)?;
    writeln!(p, "maxindegreenode={}", max_indegree_node)?;
    writeln!(p, "avgindegree={}", num_arcs as f64 / num_nodes as f64)?;

    // SCC statistics (optional)
    if let Some(scc_sizes_path) = args.scc_sizes {
        let int_slice = args.scc_sizes_fmt.load(&scc_sizes_path)?;
        dispatch_int_slice!(int_slice, |sizes| {
            let mut scc_sizes: Vec<usize> =
                (0..sizes.len()).map(|i| sizes.index_value(i)).collect();
            scc_sizes.sort_unstable();

            let m = scc_sizes.len();
            let max_size = scc_sizes[m - 1];
            let min_size = scc_sizes[0];

            writeln!(p, "sccs={}", m)?;
            writeln!(p, "maxsccsize={}", max_size)?;
            writeln!(
                p,
                "percmaxscc={}",
                100.0 * max_size as f64 / num_nodes as f64
            )?;
            writeln!(p, "minsccsize={}", min_size)?;
            writeln!(
                p,
                "percminscc={}",
                100.0 * min_size as f64 / num_nodes as f64
            )?;

            // Write SCC size distribution (descending size, with counts)
            let scc_distr_path = results_basename.with_extension("sccdistr");
            let mut pw = std::io::BufWriter::new(std::fs::File::create(&scc_distr_path)?);

            let mut current = max_size;
            let mut count: usize = 0;
            for i in (0..scc_sizes.len()).rev() {
                if scc_sizes[i] != current {
                    writeln!(pw, "{}\t{}", current, count)?;
                    current = scc_sizes[i];
                    count = 0;
                }
                count += 1;
            }
            writeln!(pw, "{}\t{}", current, count)?;
        });
    }

    Ok(())
}
