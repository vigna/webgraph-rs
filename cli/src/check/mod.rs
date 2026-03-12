/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod ef;
pub mod eq;

/// Checks coherence of files.​
#[derive(Subcommand, Debug)]
#[command(name = "check")]
pub enum SubCommands {
    Ef(ef::CliArgs),
    Eq(eq::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Ef(args) => ef::main(args),
        SubCommands::Eq(args) => eq::main(args),
    }
}
