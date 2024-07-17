/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{ArgMatches, Command};

pub mod bfs;
pub mod merge;
pub mod rand;

pub const COMMAND_NAME: &str = "perm";

pub fn cli(command: Command) -> Command {
    let sub_command = Command::new(COMMAND_NAME)
        .about("Permutations related things.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);
    let sub_command = bfs::cli(sub_command);
    let sub_command = merge::cli(sub_command);
    let sub_command = rand::cli(sub_command);
    command.subcommand(sub_command)
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    match submatches.subcommand() {
        Some((bfs::COMMAND_NAME, sub_m)) => bfs::main(sub_m),
        Some((merge::COMMAND_NAME, sub_m)) => merge::main(sub_m),
        Some((rand::COMMAND_NAME, sub_m)) => rand::main(sub_m),
        Some((command_name, _)) => {
            eprintln!("Unknown command: {:?}", command_name);
            std::process::exit(1);
        }
        None => {
            eprintln!("No command given for perm");
            std::process::exit(1);
        }
    }
}
