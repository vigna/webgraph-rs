/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod llp;
pub mod llp_combine;
pub mod pad;
pub mod sllp;

/// Runs algorithms on graphs.​
#[derive(Subcommand, Debug)]
#[command(name = "run")]
pub enum SubCommands {
    Llp(llp::CliArgs),
    LlpCombine(llp_combine::CliArgs),
    Pad(pad::CliArgs),
    Sllp(sllp::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Llp(args) => llp::main(args),
        SubCommands::LlpCombine(args) => llp_combine::main(args),
        SubCommands::Pad(args) => pad::main(args),
        SubCommands::Sllp(args) => sllp::main(args),
    }
}
