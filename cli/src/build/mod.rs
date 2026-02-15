/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{Command, Parser, Subcommand};
use clap_complete::shells::Shell;

use super::GlobalArgs;

pub mod dcf;
pub mod ef;
pub mod offsets;

/// Builds accessory graph data structures (e.g., offsets, ef, dcf).
#[derive(Subcommand, Debug)]
#[command(name = "build")]
pub enum SubCommands {
    Ef(ef::CliArgs),
    Dcf(dcf::CliArgs),
    Offsets(offsets::CliArgs),
    Complete(CompleteArgs),
}

/// Generates shell completions. Use with `source <(webgraph build completions $SHELL)`.
#[derive(Parser, Debug)]
pub struct CompleteArgs {
    shell: Shell,
}

pub fn main(
    global_args: GlobalArgs,
    subcommand: SubCommands,
    mut top_command: Command,
) -> Result<()> {
    match subcommand {
        SubCommands::Ef(args) => ef::main(global_args, args),
        SubCommands::Dcf(args) => dcf::main(global_args, args),
        SubCommands::Offsets(args) => offsets::main(global_args, args),
        SubCommands::Complete(args) => {
            clap_complete::generate(
                args.shell,
                &mut top_command,
                "webgraph",
                &mut std::io::stdout(),
            );
            Ok(())
        }
    }
}
