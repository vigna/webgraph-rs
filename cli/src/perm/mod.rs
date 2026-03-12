/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod bfs;
pub mod comp;
pub mod rand;

/// Permutation-related subcommands.
#[derive(Subcommand, Debug)]
#[command(name = "perm")]
pub enum SubCommands {
    Bfs(bfs::CliArgs),
    Comp(comp::CliArgs),
    Rand(rand::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Bfs(args) => bfs::main(args),
        SubCommands::Comp(args) => comp::main(args),
        SubCommands::Rand(args) => rand::main(args),
    }
}
