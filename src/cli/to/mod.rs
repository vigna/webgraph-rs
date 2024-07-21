/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{ArgMatches, Command};

pub mod ascii;
pub mod bvgraph;
pub mod csv;
pub mod endianness;

pub const COMMAND_NAME: &str = "to";

pub fn cli(command: Command) -> Command {
    let sub_command = Command::new(COMMAND_NAME)
        .about("Converts graphs from a representation to another.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);
    let sub_command = ascii::cli(sub_command);
    let sub_command = bvgraph::cli(sub_command);
    let sub_command = csv::cli(sub_command);
    let sub_command = endianness::cli(sub_command);
    command.subcommand(sub_command.display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    match submatches.subcommand() {
        Some((ascii::COMMAND_NAME, sub_m)) => ascii::main(sub_m),
        Some((bvgraph::COMMAND_NAME, sub_m)) => bvgraph::main(sub_m),
        Some((csv::COMMAND_NAME, sub_m)) => csv::main(sub_m),
        Some((endianness::COMMAND_NAME, sub_m)) => endianness::main(sub_m),
        Some((command_name, _)) => {
            eprintln!("Unknown command: {:?}", command_name);
            std::process::exit(1);
        }
        None => {
            eprintln!("No command given for to");
            std::process::exit(1);
        }
    }
}
