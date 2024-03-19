/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{ArgMatches, Command};

pub mod deg_cef;
pub mod ef;
pub mod offsets;

pub const COMMAND_NAME: &str = "build";

pub fn cli(command: Command) -> Command {
    let sub_command = Command::new(COMMAND_NAME)
        .about("Build accessory bv graph datastructures (e.g., offsets, ef, etc.).")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);
    let sub_command = deg_cef::cli(sub_command);
    let sub_command = ef::cli(sub_command);
    let sub_command = offsets::cli(sub_command);
    command.subcommand(sub_command)
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    match submatches.subcommand() {
        Some((deg_cef::COMMAND_NAME, sub_m)) => deg_cef::main(sub_m),
        Some((ef::COMMAND_NAME, sub_m)) => ef::main(sub_m),
        Some((offsets::COMMAND_NAME, sub_m)) => offsets::main(sub_m),
        Some((command_name, _)) => {
            eprintln!("Unknown command: {:?}", command_name);
            std::process::exit(1);
        }
        None => {
            eprintln!("No command given for build");
            std::process::exit(1);
        }
    }
}
