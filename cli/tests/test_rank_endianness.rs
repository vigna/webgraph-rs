/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_bitstream::prelude::*;
use std::time::Duration;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;
use webgraph_cli::rank::{birank, pagerank};
use webgraph_cli::{FloatSliceFormat, GranularityArgs, LogIntervalArg, NumThreadsArg, cli_main};

fn log_interval() -> LogIntervalArg {
    LogIntervalArg {
        log_interval: Duration::from_secs(10),
    }
}

fn granularity() -> GranularityArgs {
    GranularityArgs {
        node_granularity: None,
        arc_granularity: None,
    }
}

#[test]
fn test_pagerank_little_endian_graph() -> Result<()> {
    // Regression: the rank commands dispatched on the detected endianness
    // but loaded the graphs with the default (big-endian) loader, so
    // little-endian inputs failed to load.
    let transpose = VecGraph::from_arcs([(1, 0), (2, 0), (2, 1), (0, 2)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("transpose-le").display().to_string();
    BvComp::with_basename(&basename).comp_graph::<LE>(&transpose)?;
    cli_main(vec!["webgraph", "build", "ef", &basename])?;

    let output = tmp.path().join("rank.txt");
    pagerank::main(pagerank::CliArgs {
        transpose: basename.into(),
        output: output.clone(),
        alpha: 0.85,
        max_iter: Some(10),
        threshold: 1e-6,
        preference: None,
        preference_fmt: FloatSliceFormat::Ascii,
        mode: pagerank::CliMode::StronglyPreferential,
        fmt: FloatSliceFormat::Ascii,
        precision: None,
        num_threads: NumThreadsArg { num_threads: 2 },
        granularity: granularity(),
        log_interval: log_interval(),
    })?;

    let ranks: Vec<f64> = std::fs::read_to_string(&output)?
        .lines()
        .map(|l| l.parse::<f64>())
        .collect::<Result<_, _>>()?;
    assert_eq!(ranks.len(), 3);
    assert!(
        ranks.iter().all(|r| r.is_finite() && *r >= 0.0),
        "ranks not finite/nonnegative: {ranks:?}"
    );
    Ok(())
}

#[test]
fn test_birank_little_endian_graph() -> Result<()> {
    // U = {0, 1}, P = {2, 3}; arcs U -> P plus their transpose, both LE.
    let graph = VecGraph::from_arcs([(0, 2), (0, 3), (1, 2)]);
    let transpose = VecGraph::from_arcs([(2, 0), (3, 0), (2, 1)]);
    let tmp = tempfile::tempdir()?;
    let g_base = tmp.path().join("g-le").display().to_string();
    let t_base = tmp.path().join("t-le").display().to_string();
    BvComp::with_basename(&g_base).comp_graph::<LE>(&graph)?;
    BvComp::with_basename(&t_base).comp_graph::<LE>(&transpose)?;
    cli_main(vec!["webgraph", "build", "ef", &g_base])?;
    cli_main(vec!["webgraph", "build", "ef", &t_base])?;

    let output = tmp.path().join("birank.txt");
    birank::main(birank::CliArgs {
        graph: g_base.into(),
        transpose: t_base.into(),
        num_sources: 2,
        output: output.clone(),
        alpha: 0.85,
        beta: 0.85,
        max_iter: Some(10),
        l1_threshold: None,
        linf_threshold: None,
        preference: None,
        preference_fmt: FloatSliceFormat::Ascii,
        fmt: FloatSliceFormat::Ascii,
        precision: None,
        num_threads: NumThreadsArg { num_threads: 2 },
        granularity: granularity(),
        log_interval: log_interval(),
    })?;

    let ranks: Vec<f64> = std::fs::read_to_string(&output)?
        .lines()
        .map(|l| l.parse::<f64>())
        .collect::<Result<_, _>>()?;
    assert_eq!(ranks.len(), 4);
    Ok(())
}
