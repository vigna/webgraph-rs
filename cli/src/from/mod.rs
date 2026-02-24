/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

use super::GlobalArgs;

pub mod arcs;

/// Ingests data into graphs.
#[derive(Subcommand, Debug)]
#[command(name = "from")]
pub enum SubCommands {
    Arcs(arcs::CliArgs),
}

pub fn main(global_args: GlobalArgs, subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Arcs(args) => arcs::main(global_args, args),
    }
}
