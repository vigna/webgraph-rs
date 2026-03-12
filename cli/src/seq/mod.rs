/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod float;
pub mod int;

#[derive(Subcommand, Debug)]
#[command(name = "seq")]
/// Converts sequences (e.g., permutations, centralities) between formats.​
pub enum SubCommands {
    Float(float::CliArgs),
    Int(int::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Float(args) => float::main(args),
        SubCommands::Int(args) => int::main(args),
    }
}
