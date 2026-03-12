/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Subcommand;

pub mod arcs;
pub mod ascii;
pub mod bvgraph;
pub mod endianness;
pub mod floatfmt;
pub mod intfmt;

#[derive(Subcommand, Debug)]
#[command(name = "to")]
/// Converts graphs and slices from a representation to another.​
pub enum SubCommands {
    Ascii(ascii::CliArgs),
    Bvgraph(bvgraph::CliArgs),
    Arcs(arcs::CliArgs),
    Endianness(endianness::CliArgs),
    Floatfmt(floatfmt::CliArgs),
    Intfmt(intfmt::CliArgs),
}

pub fn main(subcommand: SubCommands) -> Result<()> {
    match subcommand {
        SubCommands::Ascii(args) => ascii::main(args),
        SubCommands::Bvgraph(args) => bvgraph::main(args),
        SubCommands::Arcs(args) => arcs::main(args),
        SubCommands::Endianness(args) => endianness::main(args),
        SubCommands::Floatfmt(args) => floatfmt::main(args),
        SubCommands::Intfmt(args) => intfmt::main(args),
    }
}
