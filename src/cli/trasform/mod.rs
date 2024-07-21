/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{ArgMatches, Command};

pub mod simplify;
pub mod transpose;

pub const COMMAND_NAME: &str = "transform";

pub fn cli(command: Command) -> Command {
    let sub_command = Command::new(COMMAND_NAME)
        .about("Apply a trasformation to a graph.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);
    let sub_command = simplify::cli(sub_command);
    let sub_command = transpose::cli(sub_command);
    command.subcommand(sub_command)
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    match submatches.subcommand() {
        Some((simplify::COMMAND_NAME, sub_m)) => simplify::main(sub_m),
        Some((transpose::COMMAND_NAME, sub_m)) => transpose::main(sub_m),
        Some((command_name, _)) => {
            eprintln!("Unknown command: {:?}", command_name);
            std::process::exit(1);
        }
        None => {
            eprintln!("No command given for trasform");
            std::process::exit(1);
        }
    }
}
