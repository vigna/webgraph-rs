/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod map;
pub mod perm;
pub mod simplify;
pub mod transpose;

/// Applies a transformation to a graph.​
#[derive(Subcommand, Debug)]
#[command(name = "transform")]
pub enum SubCommands {
    Map(map::CliArgs),
    Perm(perm::CliArgs),
    Simplify(simplify::CliArgs),
    Transpose(transpose::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Map(args) => map::main(args),
        SubCommands::Perm(args) => perm::main(args),
        SubCommands::Simplify(args) => simplify::main(args),
        SubCommands::Transpose(args) => transpose::main(args),
    }
}
