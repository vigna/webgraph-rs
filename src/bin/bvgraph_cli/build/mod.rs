/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

 use anyhow::Result;
 use clap::{ArgMatches, Command};
 
 mod ef;
 mod offsets;
 
 pub const COMMAND_NAME: &str = "build";
 
 pub fn cli(command: Command) -> Command {
     let sub_command = Command::new(COMMAND_NAME)
         .about("Build accessory bv graph datastructures (e.g., offsets, ef, etc.).")
         .subcommand_required(true)
         .arg_required_else_help(true)
         .allow_external_subcommands(true);
     let sub_command = ef::cli(sub_command);
     let sub_command = offsets::cli(sub_command);
     command.subcommand(sub_command)
 }
 
 pub fn main(submatches: &ArgMatches) -> Result<()> {
     match submatches.subcommand() {
         Some((ef::COMMAND_NAME, sub_m)) => ef::main(sub_m),
         Some((offsets::COMMAND_NAME, sub_m)) => offsets::main(sub_m),
         _ => unreachable!(),
     }
 }