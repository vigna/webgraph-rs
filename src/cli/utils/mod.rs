/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{ArgMatches, Command};

pub mod pad;

pub const COMMAND_NAME: &str = "utils";

pub fn cli(command: Command) -> Command {
    let sub_command = Command::new(COMMAND_NAME)
        .about("Miscellaneous commands.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);
    let sub_command = pad::cli(sub_command);
    command.subcommand(sub_command)
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    match submatches.subcommand() {
        Some((pad::COMMAND_NAME, sub_m)) => pad::main(sub_m),
        Some((command_name, _)) => {
            eprintln!("Unknown command: {:?}", command_name);
            std::process::exit(1);
        }
        None => {
            eprintln!("No command given for utils");
            std::process::exit(1);
        }
    }
}
