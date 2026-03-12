/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{Command, Parser, Subcommand};
use clap_complete::shells::Shell;

pub mod dcf;
pub mod ef;
pub mod offsets;

/// Builds accessory graph data structures (e.g., offsets, ef, dcf).​
#[derive(Subcommand, Debug)]
#[command(name = "build")]
pub enum SubCommands {
    Ef(ef::CliArgs),
    Dcf(dcf::CliArgs),
    Offsets(offsets::CliArgs),
    Complete(CompleteArgs),
}

/// Generates shell completions. Use with `source <(webgraph build complete
/// $SHELL)` to install completions locally, or redirect standard output to a
/// proper location for a permanent installation.​
#[derive(Parser, Debug)]
#[command(next_line_help = true)]
pub struct CompleteArgs {
    shell: Shell,
}

pub fn main(subcommand: SubCommands, mut top_command: Command) -> Result<()> {
    match subcommand {
        SubCommands::Ef(args) => ef::main(args),
        SubCommands::Dcf(args) => dcf::main(args),
        SubCommands::Offsets(args) => offsets::main(args),
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
