/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod codes;
pub mod stats;

/// Computes statistics on graphs.​
#[derive(Subcommand, Debug)]
#[command(name = "analyze")]
pub enum SubCommands {
    Codes(codes::CliArgs),
    Stats(stats::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Codes(args) => codes::main(args),
        SubCommands::Stats(args) => stats::main(args),
    }
}
