/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

use super::GlobalArgs;

pub mod llp;
pub mod llp_combine;
pub mod pad;

#[derive(Subcommand, Debug)]
#[command(name = "run")]
/// Run algorithms on graphs.
pub enum SubCommands {
    Llp(llp::CliArgs),
    LlpCombine(llp_combine::CliArgs),
    Pad(pad::CliArgs),
}

pub fn main(global_args: GlobalArgs, subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Llp(args) => llp::main(global_args, args),
        SubCommands::LlpCombine(args) => llp_combine::main(global_args, args),
        SubCommands::Pad(args) => pad::main(global_args, args),
    }
}
