/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{build_info, pretty_print_elapsed};
use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod pagerank;

#[derive(Subcommand, Debug)]
#[command(name = "rank")]
pub enum SubCommands {
    #[clap(name = "pagerank", visible_alias = "pr")]
    PageRank(pagerank::CliArgs),
}

#[derive(Parser, Debug)]
#[command(name = "webgraph-rank", version=build_info::version_string())]
/// WebGraph tools computing centrality measures.​
#[doc = include_str!("../common_ps.txt")]
#[doc = include_str!("../common_env.txt")]
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
        SubCommands::PageRank(args) => {
            pagerank::main(args)?;
        }
    }

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}
