/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

use super::GlobalArgs;

pub mod simplify;
pub mod transpose;

/// Applies a transformation to a graph.
#[derive(Subcommand, Debug)]
#[command(name = "transform")]
pub enum SubCommands {
    Simplify(simplify::CliArgs),
    Transpose(transpose::CliArgs),
}

pub fn main(global_args: GlobalArgs, subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Simplify(args) => simplify::main(global_args, args),
        SubCommands::Transpose(args) => transpose::main(global_args, args),
    }
}
