/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{value_parser, ArgMatches, Command};
use clap_complete::shells::Shell;

pub mod dcf;
pub mod ef;
pub mod offsets;

pub const COMMAND_NAME: &str = "build";

pub fn cli(command: Command) -> Command {
    let sub_command = Command::new(COMMAND_NAME)
        .about("Builds accessory bv graph data structures (e.g., offsets, ef, etc.).")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("completions")
                .about("Generates shell completions. Use with `source <(webgraph build completions $SHELL)`.")
                .display_order(0)
                .arg(
                    clap::Arg::new("shell")
                        .required(true)
                        .value_parser(value_parser!(Shell)),
                ),
        );
    let sub_command = dcf::cli(sub_command);
    let sub_command = ef::cli(sub_command);
    let sub_command = offsets::cli(sub_command);
    command.subcommand(sub_command.display_order(0))
}

pub fn main(submatches: &ArgMatches, top_command: &mut Command) -> Result<()> {
    match submatches.subcommand() {
        Some(("completions", sub_m)) => {
            let shell = sub_m.get_one::<Shell>("shell").unwrap();
            clap_complete::generate(*shell, top_command, "webgraph", &mut std::io::stdout());
            Ok(())
        }
        Some((dcf::COMMAND_NAME, sub_m)) => dcf::main(sub_m),
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
