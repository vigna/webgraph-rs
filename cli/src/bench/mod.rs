/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod bf_visit;
pub mod bvgraph;

#[derive(Subcommand, Debug)]
#[command(name = "bench")]
/// Benchmarking utilities.​
pub enum SubCommands {
    Bvgraph(bvgraph::CliArgs),
    BFVisit(bf_visit::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Bvgraph(args) => bvgraph::main(args),
        SubCommands::BFVisit(args) => bf_visit::main(args),
    }
}
