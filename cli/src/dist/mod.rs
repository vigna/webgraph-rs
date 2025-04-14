/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{build_info, pretty_print_elapsed};
use anyhow::Result;
use clap::{Parser, Subcommand};

use super::GlobalArgs;

pub mod ess;
pub mod hyperball;

#[derive(Subcommand, Debug)]
#[command(name = "dist")]
pub enum SubCommands {
    #[clap(visible_alias = "hb")]
    HyperBall(hyperball::CliArgs),
    #[clap(visible_alias = "ess")]
    ExactSumSweep(ess::CliArgs),
}

#[derive(Parser, Debug)]
#[command(name = "webgraph-dist", version=build_info::version_string())]
/// Webgraph tools computing graph properties based on distances.
///
#[doc = include_str!("../common_env.txt")]
pub struct Cli {
    #[clap(flatten)]
    args: GlobalArgs,
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
            hyperball::main(cli.args, args)?;
        }
        SubCommands::ExactSumSweep(args) => {
            ess::main(cli.args, args)?;
        }
    }

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}
