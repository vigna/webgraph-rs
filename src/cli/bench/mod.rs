/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{ArgMatches, Command};

mod bf_visit;
mod bvgraph;

pub const COMMAND_NAME: &str = "bench";

pub fn cli(command: Command) -> Command {
    let sub_command = Command::new(COMMAND_NAME)
        .about("A Few benchmark utilities.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);
    let sub_command = bvgraph::cli(sub_command);
    let sub_command = bf_visit::cli(sub_command);
    command.subcommand(sub_command)
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    match submatches.subcommand() {
        Some((bvgraph::COMMAND_NAME, sub_m)) => bvgraph::main(sub_m),
        Some((bf_visit::COMMAND_NAME, sub_m)) => bf_visit::main(sub_m),
        _ => unreachable!(),
    }
}
