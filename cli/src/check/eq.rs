/*
 * SPDX-FileCopyrightText: 2024 Davide Cologni
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use anyhow::Result;
use clap::Args;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::Lender;
use std::path::PathBuf;
use webgraph::traits::SequentialLabeling;

#[derive(Args, Debug)]
#[command(name = "eq", about = "Checks that two graphs have the same contents, listed in the same order. Useful to check equality when two graph are compressed with different parameters or with different algorithms (think about reference selection).", long_about = None)]
pub struct CliArgs {
    /// The basename of the first graph.
    pub first_basename: PathBuf,
    /// The basename of the second graph.
    pub second_basename: PathBuf,
}

pub fn main(_global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    compare_graphs(args)
}

pub fn compare_graphs(args: CliArgs) -> Result<()> {
    let first_graph =
        webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.first_basename)
            .endianness::<BE>()
            .load()?;
    let second_graph =
        webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.second_basename)
            .endianness::<BE>()
            .load()?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("compare graphs")
        .expected_updates(Some(first_graph.num_nodes()));

    let mut first_iter = first_graph.iter().enumerate();
    let mut second_iter = second_graph.iter();

    pl.start("Start comparing the graphs...");
    while let Some((i, (true_node_id, true_succ))) = first_iter.next() {
        let (node_id, succ) = second_iter.next().unwrap();

        assert_eq!(true_node_id, i);
        assert_eq!(true_node_id, node_id);
        assert_eq!(
            true_succ.into_iter().collect::<Vec<_>>(),
            succ.into_iter().collect::<Vec<_>>(),
            "Different successor lists for node {}",
            node_id
        );
        pl.light_update();
    }

    pl.done();
    Ok(())
}
