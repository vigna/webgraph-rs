/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::GlobalArgs;
use anyhow::Result;
use clap::Subcommand;

pub mod codes;

pub const COMMAND_NAME: &str = "";

#[derive(Subcommand, Debug)]
#[command(name = "analyze")]
/// Compute statistics on a graphs.
pub enum SubCommands {
    Codes(codes::CliArgs),
}

pub fn main(global_args: GlobalArgs, subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Codes(args) => codes::main(global_args, args),
    }
}
