/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{build_info, pretty_print_elapsed};
use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod ess;
pub mod hyperball;

#[derive(Subcommand, Debug)]
#[command(name = "dist")]
pub enum SubCommands {
    #[clap(name = "hyperball", visible_alias = "hb")]
    HyperBall(hyperball::CliArgs),
    #[clap(visible_alias = "ess")]
    ExactSumSweep(ess::CliArgs),
}

#[derive(Parser, Debug)]
#[command(name = "webgraph-dist", version=build_info::version_string(), max_term_width = 100, after_help = include_str!("../common_env.txt"))]
/// WebGraph tools computing graph properties based on distances.​
pub struct Cli {
    #[command(subcommand)]
    command: SubCommands,
}

pub fn cli_main<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let start = std::time::Instant::now();
    let cli = Cli::parse_from(args);
    match cli.command {
        SubCommands::HyperBall(args) => {
            hyperball::main(args)?;
        }
        SubCommands::ExactSumSweep(args) => {
            ess::main(args)?;
        }
    }

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}
