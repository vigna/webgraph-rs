/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod arcs;
pub mod ascii;
pub mod bvgraph;
pub mod csr;
pub mod endianness;

#[derive(Subcommand, Debug)]
#[command(name = "to")]
/// Converts graphs from a representation to another.​
pub enum SubCommands {
    Ascii(ascii::CliArgs),
    Bvgraph(bvgraph::CliArgs),
    Csr(csr::CliArgs),
    Arcs(arcs::CliArgs),
    Endianness(endianness::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Ascii(args) => ascii::main(args),
        SubCommands::Bvgraph(args) => bvgraph::main(args),
        SubCommands::Csr(args) => csr::main(args),
        SubCommands::Arcs(args) => arcs::main(args),
        SubCommands::Endianness(args) => endianness::main(args),
    }
}
