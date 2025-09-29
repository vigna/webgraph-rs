/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

use super::GlobalArgs;

pub mod bfs;
pub mod comp;
pub mod rand;

#[derive(Subcommand, Debug)]
#[command(name = "perm")]
/// Permutations related things.
pub enum SubCommands {
    Bfs(bfs::CliArgs),
    Comp(comp::CliArgs),
    Rand(rand::CliArgs),
}

pub fn main(global_args: GlobalArgs, subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Bfs(args) => bfs::main(global_args, args),
        SubCommands::Comp(args) => comp::main(global_args, args),
        SubCommands::Rand(args) => rand::main(global_args, args),
    }
}
