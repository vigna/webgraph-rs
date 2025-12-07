/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

use super::GlobalArgs;

pub mod ef;
pub mod eq;

#[derive(Subcommand, Debug)]
#[command(name = "check")]
/// Check coherence of files.
pub enum SubCommands {
    Ef(ef::CliArgs),
    Eq(eq::CliArgs),
}

pub fn main(global_args: GlobalArgs, subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Ef(args) => ef::main(global_args, args),
        SubCommands::Eq(args) => eq::main(global_args, args),
    }
}
