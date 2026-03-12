/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod codes;

/// Computes statistics on graphs.​
#[derive(Subcommand, Debug)]
#[command(name = "analyze")]
pub enum SubCommands {
    Codes(codes::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Codes(args) => codes::main(args),
    }
}
